import json
import multiprocessing
import os
import random
import shutil
import sqlite3
import time
import uuid
from datetime import datetime

WORKER_ID = str(uuid.uuid4())
IMAGE_SAVE_DIR = "./"


def recover_stuck_events(conn, timeout_seconds=30):
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE outbox
        SET status='pending', locked_by=NULL, locked_at=NULL
        WHERE status='in_progress'
          AND locked_at IS NOT NULL
          AND datetime(locked_at, '+' || ? || ' seconds') < CURRENT_TIMESTAMP
        """,
        (timeout_seconds,),
    )
    affected = cursor.rowcount
    if affected:
        print(f"[RECOVER] {affected} stuck events recovered.")
    conn.commit()


def fetch_and_lock(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")  # DB 전체 락
        cursor.execute(
            """
            SELECT id, gid, path
            FROM outbox
            WHERE status='pending'
            ORDER BY created_at
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        if not row:
            conn.rollback()
            return None

        id, gid, path = row
        cursor.execute(
            """
            UPDATE outbox
            SET status='in_progress', locked_by=?, locked_at=CURRENT_TIMESTAMP, processed_at=NULL
            WHERE id=? AND status='pending'
            """,
            (WORKER_ID, id),
        )
        if cursor.rowcount == 0:  # 다른 프로세스가 이미 가져간 경우
            conn.rollback()
            return None

        conn.commit()
        return id, gid, path
    except sqlite3.OperationalError as e:
        print(f"[LOCK ERROR] {e}")
        conn.rollback()
        return None


def move_file(src: str, dst: str):
    os.makedirs(os.path.dirname(dst), exist_ok=True)

    if os.path.exists(dst):
        print(f"[OK] 이미 목적지에 존재 (멱등 처리): {dst}")
        return dst

    if not os.path.exists(src):
        raise FileNotFoundError(f"원본 파일이 존재하지 않음: {src}")

    shutil.move(src, dst)
    print(f"[OK] 파일 이동 완료: {src} -> {dst}")


def send_func(path):
    print(f"[SEND] {path}")
    delay = random.uniform(0.2, 1.0)
    time.sleep(delay)
    if random.random() < 0.2:
        raise Exception("전송 실패 (네트워크 오류 시뮬레이션)")
    return {"hello": "world"}


def process_outbox(conn, interval=2):
    while True:
        # 오래된 이벤트 복구
        recover_stuck_events(conn, timeout_seconds=30)

        job = fetch_and_lock(conn)
        if not job:
            time.sleep(interval)
            continue

        id, gid, path = job

        try:
            original_filename = os.path.basename(path)
            _, extension = os.path.splitext(original_filename)
            unique_filename = f"{gid}{extension}"

            # 파일 이동
            today_str = datetime.now().strftime("%Y%m%d")
            new_path = os.path.join(IMAGE_SAVE_DIR, today_str)
            final_dest_path = os.path.join(new_path, unique_filename)

            move_file(path, final_dest_path)
            response = send_func(final_dest_path)

            # ---- 결과를 DB에 짧게 반영 ----
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE orders SET payload=? WHERE gid=?",
                (json.dumps(response), gid),
            )
            conn.execute(
                "UPDATE outbox SET status='done', processed_at=CURRENT_TIMESTAMP WHERE id=?",
                (id,),
            )
            conn.commit()

        except Exception as e:
            print(f"[ERROR]: {e}")
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE outbox SET status='failed', processed_at=CURRENT_TIMESTAMP WHERE id=?",
                (id,),
            )
            conn.commit()


def worker_main(worker_id: int):
    conn = sqlite3.connect("outbox.db", check_same_thread=False)
    print(f"[Worker {worker_id}] 시작")
    process_outbox(conn, interval=1)


if __name__ == "__main__":
    num_workers = 3  # 워커 프로세스 개수
    processes = []

    for i in range(num_workers):
        p = multiprocessing.Process(target=worker_main, args=(i,))
        p.start()
        processes.append(p)

    # 프로세스가 끝날 때까지 대기
    for p in processes:
        p.join()
