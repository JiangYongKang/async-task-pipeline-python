"""中断恢复测试：进程停止时正在执行的任务不被静默丢弃，
重启后状态确定且可查询；PENDING 任务被重新执行。"""
from __future__ import annotations

import logging
import threading

from taskpipe.config import Settings
from taskpipe.executor import Executor
from taskpipe.models import FailureReason, TaskState
from taskpipe.service import TaskService
from taskpipe.store import TaskStore
from tests.conftest import wait_until

logger = logging.getLogger("taskpipe.test")


def _simulate_crash(store: TaskStore, executor: Executor) -> None:
    """模拟进程中断：执行器不等待、存储直接关闭。"""
    executor.shutdown(wait=False)
    store.close()


def test_running_task_recovered_as_failed_interrupted(tmp_db, request_id):
    settings = Settings(db_path=tmp_db, handler_timeout=30)
    store = TaskStore(tmp_db)
    service = TaskService(store, settings)
    block = threading.Event()

    def handler(payload):
        block.wait(60)  # 永不返回，模拟被中断打断的执行
        return None

    executor = Executor(store, settings, handler)
    task, _ = service.submit("k1", {}, request_id=request_id)
    executor.dispatch(task)
    wait_until(lambda: store.get(task.task_id).state is TaskState.RUNNING)

    _simulate_crash(store, executor)
    logger.info(
        "request=%s 模拟进程中断：task=%s 停止时状态=RUNNING",
        request_id, task.task_id,
    )

    # 重启：新实例加载同一数据库
    store2 = TaskStore(tmp_db)
    recovered = store2.recover_interrupted()
    assert [t.task_id for t in recovered] == [task.task_id]
    final = store2.get(task.task_id)
    assert final.state is TaskState.FAILED
    assert final.failure_reason is FailureReason.INTERRUPTED
    logger.info(
        "request=%s decision verified: 重启后 task=%s state=%s reason=%s "
        "（状态确定且可查询，未被静默丢弃）",
        request_id, task.task_id, final.state.value,
        final.failure_reason.value,
    )
    store2.close()


def test_pending_task_is_redispatched_after_restart(tmp_db, request_id):
    settings = Settings(db_path=tmp_db, max_concurrency=1, handler_timeout=5)
    store = TaskStore(tmp_db)
    service = TaskService(store, settings)
    block = threading.Event()

    def handler(payload):
        if payload.get("block"):
            block.wait(60)
        return {"echo": payload}

    executor = Executor(store, settings, handler)
    t1, _ = service.submit("running", {"block": True}, request_id=request_id)
    t2, _ = service.submit("queued", {}, request_id=request_id)
    executor.dispatch(t1)
    executor.dispatch(t2)  # 并发度=1，t2 停在 PENDING
    wait_until(lambda: store.get(t1.task_id).state is TaskState.RUNNING)
    assert store.get(t2.task_id).state is TaskState.PENDING

    _simulate_crash(store, executor)

    # 重启
    store2 = TaskStore(tmp_db)
    service2 = TaskService(store2, settings)
    executor2 = Executor(store2, settings, lambda p: {"echo": p})
    recovered = store2.recover_interrupted()
    redispatched = executor2.dispatch_pending()
    assert [t.task_id for t in recovered] == [t1.task_id]
    assert redispatched == 1
    wait_until(lambda: store2.get(t2.task_id).state is TaskState.SUCCEEDED)
    assert store2.get(t2.task_id).result == {"echo": {}}
    logger.info(
        "request=%s decision verified: 重启后 RUNNING->INTERRUPTED, "
        "PENDING 重新执行并成功 task=%s",
        request_id, t2.task_id,
    )
    executor2.shutdown(wait=True)
    store2.close()


def test_terminal_states_untouched_by_recovery(tmp_db, request_id):
    settings = Settings(db_path=tmp_db, handler_timeout=5)
    store = TaskStore(tmp_db)
    service = TaskService(store, settings)
    executor = Executor(store, settings, lambda p: {"echo": p})
    t_ok, _ = service.submit("ok", {}, request_id=request_id)
    t_cancel, _ = service.submit("cancel", {}, request_id=request_id)
    executor.dispatch(t_ok)
    wait_until(lambda: store.get(t_ok.task_id).state is TaskState.SUCCEEDED)
    service.cancel(t_cancel.task_id, request_id=request_id)
    v_ok, v_cancel = t_ok_version = (
        store.get(t_ok.task_id).version,
        store.get(t_cancel.task_id).version,
    )
    _simulate_crash(store, executor)

    store2 = TaskStore(tmp_db)
    recovered = store2.recover_interrupted()
    assert recovered == []  # 终态任务不受影响
    assert store2.get(t_ok.task_id).state is TaskState.SUCCEEDED
    assert store2.get(t_ok.task_id).version == v_ok
    assert store2.get(t_cancel.task_id).state is TaskState.CANCELLED
    assert store2.get(t_cancel.task_id).version == v_cancel
    logger.info(
        "request=%s decision verified: 恢复不触碰终态任务（版本号未变）",
        request_id,
    )
    store2.close()
