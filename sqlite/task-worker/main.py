import json
import multiprocessing
import os
import random
import shutil
import sqlite3
import time
import uuid
from datetime import datetime, timedelta

IMAGE_SAVE_DIR = "./"
DB_PATH = "outbox.db"

BASE_BACKOFF_SECONDS = 5
MAX_BACKOFF_SECONDS = 60 * 60 * 24


def make_conn(path=DB_PATH):
    conn = sqlite3.connect(path, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except Exception:
        pass
    return conn


def compute_next_retry(
    attempts, base=BASE_BACKOFF_SECONDS, max_backoff=MAX_BACKOFF_SECONDS
):
    expo = base * (2 ** (max(0, attempts - 1)))
    jitter = random.uniform(0, base)
    backoff = min(expo + jitter, max_backoff)
    return (datetime.now() + timedelta(seconds=backoff)).strftime("%Y-%m-%d %H:%M:%S")


def fetch_and_lock(conn, worker_tag):
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")  # DB 전체 락
        cursor.execute(
            "SELECT id FROM outbox WHERE status='pending' ORDER BY created_at LIMIT 1"
        )
        row = cursor.fetchone()
        if not row:
            conn.rollback()
            return None

        id_ = row["id"]
        cursor.execute(
            """
            UPDATE outbox
            SET status='in_progress', locked_by=?, locked_at=CURRENT_TIMESTAMP, processed_at=NULL
            WHERE id=? AND status='pending'
            """,
            (worker_tag, id_),
        )
        if cursor.rowcount == 0:  # 다른 프로세스가 이미 가져간 경우
            conn.rollback()
            return None
        cursor.execute(
            "SELECT id, gid, path, created_at FROM outbox WHERE id=?", (id_,)
        )
        row = cursor.fetchone()
        conn.commit()
        if not row:
            return None
        return (row["id"], row["gid"], row["path"], row["created_at"])

    except sqlite3.OperationalError as e:
        print(f"[LOCK ERROR] {e}")
        try:
            conn.rollback()
        except:
            pass
        return None
    except Exception as e:
        print("[fetch_and_lock ERROR]", e)
        try:
            conn.rollback()
        except:
            pass
        return None


def send_func(path):
    print(f"[SEND] {path}")
    delay = random.uniform(0.2, 1.0)
    time.sleep(delay)
    if random.random() < 0.2:
        raise Exception("전송 실패 (네트워크 오류 시뮬레이션)")
    return {"hello": "world"}


def file_move(gid, src_path, created_at):
    original_filename = os.path.basename(src_path)
    _, extension = os.path.splitext(original_filename)
    unique_filename = f"{gid}{extension}"

    # 파일 이동
    try:
        today_str = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").strftime(
            "%Y%m%d"
        )
    except Exception:
        today_str = datetime.now().strftime("%Y%m%d")

    new_path = os.path.join(IMAGE_SAVE_DIR, today_str)
    dst_path = os.path.join(new_path, unique_filename)
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)

    if os.path.exists(dst_path):
        print(f"[OK] 이미 목적지에 존재 (멱등 처리): {dst_path}")
        return dst_path

    if not os.path.exists(src_path):
        msg = f"원본 파일이 존재하지 않음: {src_path}"
        print(f"[ERROR - file_move] {msg}")
        raise FileNotFoundError(msg)

    try:
        shutil.move(src_path, dst_path)
        print(f"[OK] 파일 이동 완료: {src_path} -> {dst_path}")
        return dst_path
    except Exception as e:
        print(f"[ERROR - file_move] 이동 실패: {e}")
        raise


def do_task(conn, id_, gid, path, created_at):
    cur = conn.cursor()
    try:
        dst = file_move(gid, path, created_at)
        response = send_func(dst)

        # 결과 반영 (짧은 트랜잭션)
        try:
            cur.execute("BEGIN IMMEDIATE")
            cur.execute(
                "UPDATE orders SET payload=? WHERE gid=?", (json.dumps(response), gid)
            )
            cur.execute(
                "UPDATE outbox SET status='done', processed_at=CURRENT_TIMESTAMP WHERE id=?",
                (id_,),
            )
            conn.commit()
            print(f"[DONE] id={id_}")
        except Exception:
            conn.rollback()
            raise

    except Exception as e:
        err = str(e)[:1000]
        print(f"[ERROR] id={id_} => {err}")

        # 실패 처리: attempts 증가, next_retry_at 설정 또는 dead 처리
        try:
            cur.execute("BEGIN IMMEDIATE")
            # 안전하게 현재 attempts, max_attempts 읽기
            cur.execute("SELECT attempts, max_attempts FROM outbox WHERE id=?", (id_,))
            row = cur.fetchone()
            cur_attempts = (row["attempts"] or 0) if row else 0
            cur_max = (row["max_attempts"] or 5) if row else 5

            new_attempts = cur_attempts + 1
            if new_attempts >= cur_max:
                # 최대 재시도 도달 -> dead 상태로 마감 (운영 정책에 따라 변경 가능)
                cur.execute(
                    """
                    UPDATE outbox
                    SET status='dead', processed_at=CURRENT_TIMESTAMP,
                        attempts=?, last_error=?
                    WHERE id=?
                    """,
                    (new_attempts, err, id_),
                )
                print(f"[DEAD] id={id_} attempts={new_attempts}")
            else:
                next_retry = compute_next_retry(new_attempts)
                cur.execute(
                    """
                    UPDATE outbox
                    SET status='failed', processed_at=CURRENT_TIMESTAMP,
                        attempts=?, last_error=?, next_retry_at=?
                    WHERE id=?
                    """,
                    (new_attempts, err, next_retry, id_),
                )
                print(
                    f"[SCHEDULED RETRY] id={id_} attempts={new_attempts} next_retry_at={next_retry}"
                )

            conn.commit()
        except Exception as ex2:
            print("[ERROR] failure-state update failed:", ex2)
            try:
                conn.rollback()
            except:
                pass


def process_outbox(conn, worker_tag, interval=2):
    while True:
        job = fetch_and_lock(conn, worker_tag)
        if not job:
            time.sleep(interval)
            continue
        id_, gid, path, created_at = job
        do_task(conn, id_, gid, path, created_at)


def task_worker_main(worker_index: int):
    worker_tag = f"{os.getpid()}-{worker_index}-{uuid.uuid4()}"
    conn = make_conn()
    print(f"[Worker {worker_index}] 시작 tag={worker_tag}")
    process_outbox(conn, worker_tag, interval=2)


if __name__ == "__main__":
    num_workers = 3
    processes = []
    for i in range(num_workers):
        p = multiprocessing.Process(target=task_worker_main, args=(i,))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()
