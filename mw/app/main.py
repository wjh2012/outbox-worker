import sqlite3
import time
import uuid


def save_event(conn, gid, path):
    with conn:  # 자동 BEGIN / COMMIT / ROLLBACK
        conn.execute(
            "INSERT INTO orders (gid, path) VALUES (?, ?)",
            (gid, path),
        )
        conn.execute(
            "INSERT INTO outbox (gid, path, status) VALUES (?, ?, ?)",
            (gid, path, "pending"),
        )
    print(gid, path)


def init_db(conn):
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS outbox")
    cursor.execute("DROP TABLE IF EXISTS orders")

    cursor.execute(
        """
        -- 비즈니스 테이블(예: 주문)
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gid TEXT UNIQUE,
            path TEXT NOT NULL,
            payload TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    cursor.execute(
        """
        -- 아웃박스 (같은 DB에 둬야 트랜잭션으로 묶임)
        CREATE TABLE IF NOT EXISTS outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gid TEXT UNIQUE,
            path TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', -- pending, in_progress, sent, failed
            locked_by TEXT,
            locked_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP
        );
        """
    )
    cursor.execute(
        "CREATE INDEX idx_outbox_status_created_at ON outbox(status, created_at);"
    )
    conn.commit()


if __name__ == "__main__":
    conn = sqlite3.connect("outbox.db", check_same_thread=False)

    init_db(conn)

    while True:
        save_event(
            conn,
            uuid.uuid4().hex,
            "abc/abc",
        )
        save_event(
            conn,
            uuid.uuid4().hex,
            "bcd/bcd",
        )
        save_event(
            conn,
            uuid.uuid4().hex,
            "def/def",
        )
        time.sleep(0.1)
