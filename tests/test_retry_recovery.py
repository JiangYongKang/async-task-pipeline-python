"""重试与重启恢复测试：
* 停止时正在执行的那次算作一次尝试，重启后按策略决定继续重试/终态失败；
* 停止时处于 WAITING_RETRY 的任务按原计划（next_run_at）接着跑，
  不重复执行、不重复计数；
* 不带策略的任务保持原有恢复行为（FAILED/INTERRUPTED）。
"""
from __future__ import annotations

import logging
import threading

from taskpipe.config import Settings
from taskpipe.executor import Executor
from taskpipe.models import AttemptResult, FailureReason, RetryPolicy, TaskState
from taskpipe.service import TaskService
from taskpipe.store import TaskStore
from tests.conftest import wait_until

logger = logging.getLogger("taskpipe.test")


def _simulate_crash(
    store: TaskStore, executor: Executor, release=None
) -> None:
    executor.shutdown(wait=False)
    store.close()
    # 释放仍阻塞在处理器里的残留 worker，使其在已关闭的库上快速结束
    # （后续写入直接抛 sqlite 异常并被 future 吞掉），避免测试日志噪音。
    if release is not None:
        release.set()


def test_interrupted_attempt_counts_and_retries_after_restart(
    tmp_db, request_id
):
    settings = Settings(db_path=tmp_db, handler_timeout=30)
    store = TaskStore(tmp_db)
    service = TaskService(store, settings)
    block = threading.Event()
    executor = Executor(store, settings, lambda p: block.wait(60))
    task, _ = service.submit(
        "k1", {}, request_id=request_id,
        retry_policy=RetryPolicy(max_retries=2, interval=0.05),
    )
    executor.dispatch(task)
    wait_until(lambda: store.get(task.task_id).state is TaskState.RUNNING)
    assert store.get(task.task_id).attempt_count == 1
    _simulate_crash(store, executor, release=block)
    logger.info(
        "request=%s 模拟中断：task=%s RUNNING 中停止（第 1 次尝试进行中）",
        request_id, task.task_id,
    )

    store2 = TaskStore(tmp_db)
    executor2 = Executor(store2, settings, lambda p: {"echo": p})
    recovered = store2.recover_interrupted(executor2.retry_decider)
    assert len(recovered) == 1
    rec = store2.get(task.task_id)
    assert rec.state is TaskState.WAITING_RETRY
    assert rec.attempt_count == 1
    records = store2.list_attempts(task.task_id)
    assert records[0].result is AttemptResult.FAILURE
    assert records[0].failure_reason is FailureReason.INTERRUPTED

    executor2.dispatch_pending()
    final = wait_until(
        lambda: (
            t if (t := store2.get(task.task_id)).state
            is TaskState.SUCCEEDED else None
        )
    )
    assert final.attempt_count == 2
    records2 = store2.list_attempts(task.task_id)
    assert [a.attempt_no for a in records2] == [1, 2]
    assert records2[1].result is AttemptResult.SUCCESS
    logger.info(
        "request=%s decision verified: 被打断尝试计数=1，重启后续跑成功 task=%s",
        request_id, task.task_id,
    )
    executor2.shutdown(wait=True)
    store2.close()


def test_interrupted_attempt_exhausts_retries_to_final_fail(
    tmp_db, request_id
):
    settings = Settings(db_path=tmp_db, handler_timeout=30)
    store = TaskStore(tmp_db)
    service = TaskService(store, settings)
    block = threading.Event()
    executor = Executor(store, settings, lambda p: block.wait(60))
    task, _ = service.submit(
        "k1", {}, request_id=request_id,
        retry_policy=RetryPolicy(max_retries=1, interval=0.0),
    )
    executor.dispatch(task)
    wait_until(lambda: store.get(task.task_id).state is TaskState.RUNNING)
    _simulate_crash(store, executor)

    store2 = TaskStore(tmp_db)
    exec2 = Executor(store2, settings, lambda p: block.wait(60))
    rec = store2.recover_interrupted(exec2.retry_decider)
    assert rec[0].state is TaskState.WAITING_RETRY
    wait_until(lambda: store2.get(task.task_id).state is TaskState.RUNNING)
    wait_until(lambda: store2.get(task.task_id).attempt_count == 2)
    _simulate_crash(store2, exec2, release=block)

    store3 = TaskStore(tmp_db)
    exec3 = Executor(store3, settings, lambda p: {"echo": p})
    store3.recover_interrupted(exec3.retry_decider)
    final = store3.get(task.task_id)
    assert final.state is TaskState.FAILED
    assert final.failure_reason is FailureReason.INTERRUPTED
    assert final.attempt_count == 2
    assert len(store3.list_attempts(task.task_id)) == 2
    logger.info(
        "request=%s decision verified: 两次打断用尽次数，终态=FAILED/INTERRUPTED",
        request_id,
    )
    exec3.shutdown(wait=True)
    store3.close()


def test_waiting_retry_resumes_on_schedule_after_restart(tmp_db, request_id):
    settings = Settings(db_path=tmp_db, handler_timeout=5)
    store = TaskStore(tmp_db)
    service = TaskService(store, settings)
    runs = {"n": 0}
    lock = threading.Lock()

    def fail_once(payload):
        with lock:
            runs["n"] += 1
            n = runs["n"]
        if n == 1:
            raise RuntimeError("first fails")
        return {"n": n}

    executor = Executor(store, settings, fail_once)
    task, _ = service.submit(
        "k1", {}, request_id=request_id,
        retry_policy=RetryPolicy(max_retries=2, interval=0.3),
    )
    executor.dispatch(task)
    waiting = wait_until(
        lambda: (
            t if (t := store.get(task.task_id)).state
            is TaskState.WAITING_RETRY else None
        )
    )
    due = waiting.next_run_at
    _simulate_crash(store, executor)
    assert runs["n"] == 1

    store2 = TaskStore(tmp_db)
    executor2 = Executor(store2, settings, fail_once)
    recovered = store2.recover_interrupted(executor2.retry_decider)
    assert recovered == []
    executor2.dispatch_pending()
    resumed = store2.get(task.task_id)
    assert resumed.state is TaskState.WAITING_RETRY
    assert abs(resumed.next_run_at - due) < 1e-6

    final = wait_until(
        lambda: (
            t if (t := store2.get(task.task_id)).state
            is TaskState.SUCCEEDED else None
        )
    )
    assert final.attempt_count == 2 and final.result == {"n": 2}
    logger.info(
        "request=%s decision verified: WAITING_RETRY 重启后按原计划续跑一次 task=%s",
        request_id, task.task_id,
    )
    executor2.shutdown(wait=True)
    store2.close()


def test_no_policy_interrupted_still_final_interrupted(tmp_db, request_id):
    settings = Settings(db_path=tmp_db, handler_timeout=30)
    store = TaskStore(tmp_db)
    service = TaskService(store, settings)
    block = threading.Event()
    executor = Executor(store, settings, lambda p: block.wait(60))
    task, _ = service.submit("k1", {}, request_id=request_id)
    executor.dispatch(task)
    wait_until(lambda: store.get(task.task_id).state is TaskState.RUNNING)
    _simulate_crash(store, executor, release=block)

    store2 = TaskStore(tmp_db)
    executor2 = Executor(store2, settings, lambda p: {"echo": p})
    recovered = store2.recover_interrupted(executor2.retry_decider)
    assert recovered[0].state is TaskState.FAILED
    assert recovered[0].failure_reason is FailureReason.INTERRUPTED
    assert store2.get(task.task_id).state is TaskState.FAILED
    executor2.shutdown(wait=True)
    store2.close()
