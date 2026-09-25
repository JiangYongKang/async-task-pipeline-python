"""进程中断恢复。

真实地以独立子进程提交任务，再 SIGKILL 中断，随后在同一数据库上
重启服务，验证：

* 被杀时处于 RUNNING 的任务 -> FAILED / INTERRUPTED（终态确定、可查询）；
* 被杀时处于 QUEUED 的任务 -> 重启后被重新入队并执行到 SUCCEEDED，
  不会被静默丢弃；
* 幂等键在重启后依然有效（重复提交不产生新任务）。
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from app.enums import FailureReason, TaskState

_RECOVEREE = Path(__file__).parent / "_recoveree.py"
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _start_recoveree(db_path: Path, mode: str) -> tuple[subprocess.Popen, str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = (
        str(_PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    )
    proc = subprocess.Popen(
        [sys.executable, str(_RECOVEREE), str(db_path), mode],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        cwd=str(_PROJECT_ROOT),
    )
    assert proc.stdout is not None
    line = proc.stdout.readline()
    if not line.startswith("READY"):
        err = proc.stderr.read()
        pytest.fail(f"recoveree 未就绪: {line!r}\n{err}")
    parts = line.strip().split()
    running_id = parts[1].split("=")[1]
    queued_id = parts[2].split("=")[1]
    return proc, running_id, queued_id


def test_recovery_after_hard_kill(tmp_path) -> None:
    db_path = tmp_path / "recovery.db"
    proc, running_id, queued_id = _start_recoveree(db_path, "kill")
    try:
        # 给 worker 一点运行时间后强杀（无 finally、无 atexit 机会）
        time.sleep(0.3)
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=5)
        assert proc.returncode is not None and proc.returncode < 0
    finally:
        if proc.poll() is None:
            proc.kill()

    # 在同一数据库上重启
    from app.handlers import register_builtin_handlers, register_handler
    from app.service import TaskService
    from app.settings import Settings

    def _later(payload: str, cancel) -> str:
        # 重启进程中的重放版本：立即成功，验证 QUEUED 不丢失
        return "replayed-ok"

    def _slow(payload: str, cancel) -> str:
        cancel.wait(2)
        return "slow"

    register_handler("later", _later)
    register_handler("slow", _slow)
    register_builtin_handlers()

    # 直接在被杀现场的数据库上重启服务
    svc = TaskService(
        Settings(max_concurrency=1, max_capacity=8, db_path=db_path)
    )
    svc.initialize()

    interrupted_task = svc.get(running_id)
    assert interrupted_task.state is TaskState.FAILED
    assert interrupted_task.failure_reason is FailureReason.INTERRUPTED
    print(
        f"请求标识=recovery 任务={running_id} 状态迁移=RUNNING->FAILED"
        f" 判定依据=进程SIGKILL结果不可知 原因=INTERRUPTED"
    )

    # QUEUED 任务被重新入队，应执行到 SUCCEEDED（不静默丢弃）
    deadline = time.monotonic() + 5
    requeued = svc.get(queued_id)
    while time.monotonic() < deadline and not requeued.state.is_terminal:
        time.sleep(0.05)
        requeued = svc.get(queued_id)
    assert requeued.state is TaskState.SUCCEEDED, (
        f"QUEUED 任务重启后应重放成功，实际={requeued.state}"
    )
    print(
        f"请求标识=recovery 任务={queued_id} 状态迁移=QUEUED->(RUNNING)->SUCCEEDED"
        f" 判定依据=启动恢复扫描到QUEUED重新入队"
    )

    # 重启后幂等键仍生效
    replay, replayed = svc.submit(
        "later", "", idempotency_key="queued-key", request_id="recovery-idem"
    )
    assert replayed is True and replay.task_id == queued_id

    svc.shutdown()


def test_graceful_shutdown_leaves_no_dangling(service_factory, tmp_path) -> None:
    """优雅停机对照：等待任务终态后，重启时无悬空任务。"""
    db_path = tmp_path / "graceful.db"
    proc, running_id, queued_id = _start_recoveree(db_path, "shutdown")
    proc.wait(timeout=10)
    assert proc.returncode == 0, proc.stderr.read()

    from app.handlers import register_builtin_handlers
    from app.service import TaskService
    from app.settings import Settings

    register_builtin_handlers()
    svc = TaskService(
        Settings(max_concurrency=1, max_capacity=8, db_path=db_path)
    )
    svc.initialize()
    # shutdown 模式下两个任务都会收到 cancel 信号，终态为 CANCELLED
    for tid in (running_id, queued_id):
        task = svc.get(tid)
        assert task.state is TaskState.CANCELLED, f"{tid}: {task.state}"
    svc.shutdown()
