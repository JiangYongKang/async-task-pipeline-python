"""中断恢复测试用的子进程脚本。

由 test_recovery.py 以独立进程启动：
  * 启动服务并提交若干任务；
  * 打印 READY 后等待 PARENT_KILL 信号；
  * 父进程随后 SIGKILL 本进程，模拟真实进程中断（不给优雅停机机会），
    持久化在 SQLite 中的 QUEUED/RUNNING 状态必须在重启后可解释。

用法: python _recoveree.py <db_path> <mode>
  mode=kill      -> 提交 1 个 RUNNING(slow) + 1 个 QUEUED(blocked worker)，等待被杀
  mode=shutdown  -> 提交后正常退出（优雅停机对照）
"""

from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path

from app.handlers import register_handler
from app.service import TaskService
from app.settings import Settings


def main() -> None:
    db_path = Path(sys.argv[1])
    mode = sys.argv[2]

    def slow(payload: str, cancel: threading.Event) -> str:
        # 运行足够久，保证被杀时仍是 RUNNING；响应 cancel 以便优雅停机对照
        cancel.wait(10)
        return "slow-finished"

    def later(payload: str, cancel: threading.Event) -> str:
        cancel.wait(10)
        return "later-finished"

    register_handler("slow", slow)
    register_handler("later", later)

    svc = TaskService(
        Settings(
            max_concurrency=1,
            max_capacity=8,
            db_path=db_path,
        )
    )
    svc.initialize()

    running_task, _ = svc.submit("slow", "", idempotency_key="running-key", request_id="sub-run")
    queued_task, _ = svc.submit("later", "", idempotency_key="queued-key", request_id="sub-queue")

    # 等 slow 确实进入 RUNNING
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if svc.get(running_task.task_id).state.value == "RUNNING":
            break
        time.sleep(0.01)
    assert svc.get(running_task.task_id).state.value == "RUNNING"
    assert svc.get(queued_task.task_id).state.value == "QUEUED"

    print(f"READY running={running_task.task_id} queued={queued_task.task_id}", flush=True)

    if mode == "kill":
        # 等父进程 SIGKILL，绝不执行优雅停机
        time.sleep(30)
    else:
        svc.shutdown()
        print("GRACEFUL-DONE", flush=True)


if __name__ == "__main__":
    main()
