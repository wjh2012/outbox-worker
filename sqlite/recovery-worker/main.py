import argparse
import sqlite3
import time
from typing import List


def make_conn(db_path: str, timeout: float = 30.0) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=timeout)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except Exception as e:
        print("[PRAGMA] WAL 설정 시도 중 오류:", e)
    return conn


def select_stuck_ids(
    conn: sqlite3.Connection, timeout_seconds: int, limit: int
) -> List[int]:
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")  # 짧게
        cur.execute(
            """
            SELECT id
            FROM outbox
            WHERE status in ('in_progress', 'failed') 
              AND locked_by IS NOT NULL
              AND locked_at IS NOT NULL
              AND datetime(locked_at, '+' || ? || ' seconds') < CURRENT_TIMESTAMP
            ORDER BY locked_at
            LIMIT ?
            """,
            (int(timeout_seconds), int(limit)),
        )
        rows = cur.fetchall()
        cur.execute("COMMIT")
        return [r["id"] for r in rows]
    except Exception as e:
        try:
            cur.execute("ROLLBACK")
        except:
            pass
        print("[ERROR] select_stuck_ids:", e)
        return []


def select_failed_ready_ids(conn, limit: int) -> List[int]:
    """
    실패(failed) 중 next_retry_at <= CURRENT_TIMESTAMP 이고 attempts < max_attempts
    NOTE: next_retry_at이 NULL이면 제외
    """
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")
        cur.execute(
            """
            SELECT id
            FROM outbox
            WHERE status='failed'
              AND next_retry_at IS NOT NULL
              AND datetime(next_retry_at) <= CURRENT_TIMESTAMP
              AND (attempts IS NULL OR attempts < (max_attempts IS NULL OR max_attempts) )
            ORDER BY next_retry_at
            LIMIT ?
            """,
            (int(limit),),
        )
        rows = cur.fetchall()
        cur.execute("COMMIT")
        return [r["id"] for r in rows]
    except Exception as e:
        try:
            cur.execute("ROLLBACK")
        except:
            pass
        print("[ERROR] select_failed_ready_ids:", e)
        return []


# 파이썬 필터 방식
def select_failed_ready_ids_safe(conn, limit: int) -> List[int]:
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")
        cur.execute(
            """
            SELECT id, attempts, max_attempts, next_retry_at
            FROM outbox
            WHERE status='failed'
              AND next_retry_at IS NOT NULL
              AND datetime(next_retry_at) <= CURRENT_TIMESTAMP
            ORDER BY next_retry_at
            LIMIT ?
            """,
            (int(limit),),
        )
        rows = cur.fetchall()
        cur.execute("COMMIT")
        ids = []
        for r in rows:
            attempts = r["attempts"] or 0
            max_attempts = r["max_attempts"] or 5
            if attempts < max_attempts:
                ids.append(r["id"])
        return ids
    except Exception as e:
        try:
            cur.execute("ROLLBACK")
        except:
            pass
        print("[ERROR] select_failed_ready_ids_safe:", e)
        return []


def recover_ids_to_pending(conn, ids: List[int]) -> int:
    if not ids:
        return 0
    placeholder = ",".join("?" for _ in ids)
    sql = f"""
        UPDATE outbox
        SET status='pending', locked_by=NULL, locked_at=NULL
        WHERE id IN ({placeholder})
    """
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(sql, ids)
        affected = cur.rowcount
        conn.commit()
        return affected
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        print("[ERROR] recover_ids_to_pending:", e)
        return 0


def recover_loop(
    db_path: str,
    interval: int = 10,
    timeout_seconds: int = 30,
    batch_size: int = 100,
    dry_run: bool = False,
):
    conn = make_conn(db_path)
    print(
        f"[Recovery] start db={db_path} interval={interval} timeout={timeout_seconds} batch={batch_size} dry_run={dry_run}"
    )
    while True:
        try:
            # 1) stuck in_progress 복구
            stuck_ids = select_stuck_ids(
                conn, timeout_seconds=timeout_seconds, limit=batch_size
            )
            if stuck_ids:
                print(f"[Recovery] stuck 대상 {len(stuck_ids)}: {stuck_ids[:10]}")
                if not dry_run:
                    n = recover_ids_to_pending(conn, stuck_ids)
                    print(f"[Recovery] stuck 복구된 행: {n}")

            # 2) failed 중 재시도 시점 도래한 것 복구
            failed_ids = select_failed_ready_ids_safe(conn, limit=batch_size)
            if failed_ids:
                print(
                    f"[Recovery] failed->pending 대상 {len(failed_ids)}: {failed_ids[:10]}"
                )
                if not dry_run:
                    n2 = recover_ids_to_pending(conn, failed_ids)
                    print(f"[Recovery] failed->pending으로 복구된 행: {n2}")

            time.sleep(interval)

        except KeyboardInterrupt:
            print("Interrupted. exit")
            break
        except Exception as e:
            print("[Recovery] 예외:", e)
            time.sleep(max(1, interval))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="outbox.db")
    parser.add_argument("--interval", type=int, default=10)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    recover_loop(
        db_path=args.db,
        interval=args.interval,
        timeout_seconds=args.timeout,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )
