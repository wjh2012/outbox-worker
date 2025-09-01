#!/usr/bin/env python3
"""
run_all.py
개발용: sh 안쓰고 Python으로 세 프로세스를 띄우고 종료 시 정리합니다.
사용법: python run_all.py
"""
import subprocess
import signal
import sys
from pathlib import Path
import time

# 변경 가능: 실행할 명령(리스트)
CMDS = {
    "app": ["uv", "run", "-m", "sqlite.app.main"],
    "task-worker": ["uv", "run", "-m", "sqlite.task-worker.main"],
    "recovery-worker": ["uv", "run", "-m", "sqlite.recovery-worker.main"],
}

LOGDIR = Path("./logs")
PIDDIR = LOGDIR
LOGDIR.mkdir(parents=True, exist_ok=True)

procs = {}


def start_all():
    for name, cmd in CMDS.items():
        stdout_path = LOGDIR / f"{name}.log"
        pid_path = PIDDIR / f"{name}.pid"
        # append mode: 기존 로그 보존
        logf = open(stdout_path, "a", buffering=1)  # line-buffered
        print(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] starting {name}: {' '.join(cmd)} -> {stdout_path}"
        )
        # subprocess: shell=False (보안/크로스플랫폼)
        p = subprocess.Popen(cmd, stdout=logf, stderr=subprocess.STDOUT)
        procs[name] = {"proc": p, "log": logf, "pidfile": pid_path}
        pid_path.write_text(str(p.pid))


def stop_all(timeout=5):
    print("Stopping children...")
    # 먼저 terminate (SIGTERM 대신 terminate)
    for name, info in procs.items():
        p = info["proc"]
        if p.poll() is None:
            try:
                print(f"terminating {name} (pid={p.pid})")
                p.terminate()
            except Exception as e:
                print(f"error terminating {name}: {e}")
    # 기다림
    end = time.time() + timeout
    for name, info in procs.items():
        p = info["proc"]
        remaining = max(0, end - time.time())
        try:
            p.wait(timeout=remaining)
            print(f"{name} exited with code {p.returncode}")
        except subprocess.TimeoutExpired:
            print(f"{name} did not exit in time; killing (pid={p.pid})")
            try:
                p.kill()
            except Exception as e:
                print("kill error:", e)
    # 닫기 및 pid파일 정리
    for name, info in procs.items():
        try:
            info["log"].close()
            if info["pidfile"].exists():
                info["pidfile"].unlink()
        except Exception:
            pass
    print("All children stopped.")


def handle_signal(signum, frame):
    print(f"Received signal {signum}, shutting down...")
    stop_all()
    sys.exit(0)


def main():
    try:
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
    except Exception:
        pass

    start_all()
    try:
        while True:
            for name, info in list(procs.items()):
                p = info["proc"]
                if p.poll() is not None:
                    print(
                        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {name} exited (code={p.returncode}). Shutting down others."
                    )
                    stop_all()
                    return
            time.sleep(1)
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
        stop_all()


if __name__ == "__main__":
    main()
