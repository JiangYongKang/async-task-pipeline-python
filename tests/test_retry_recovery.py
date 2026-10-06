"""重试与中断恢复测试：

* 等待重试中进程停止：重启后按原计划接着跑（错过的间隔不补等）；
* 停止时正在执行的那次尝试：算不算一次尝试、是否继续重试均按策略决定，
  且不会卡在中间状态、不会重复执行或重复计数；
* 无策略任务的中断恢复保持旧行为（FAILED/INTERRUPTED）。
"""
from __future__ import annotations

import logging
import threading

from taskpipe.config import Settings
from taskpipe.executor import Executor
from taskpipe.models import (
    AttemptStatus,
    FailureReason,
    RetryPolicy,
    TaskState,
)
from taskpipe.service import TaskService
from taskpipe.store import TaskStore
from tests.conftest import wait_until

logger = logging.getLogger("taskpipe.test")


def _simulate_crash(store: TaskStore, executor: Executor) -> None:
    executor.shutdown(wait=False)
    store.close()


def test_retry_waiting_continues_after_restart(tmp_db, request_id):
    settings = Settings(db_path=tmp_db, handler_timeout=5)
    store = TaskStore(tmp_db)
    service = TaskService(store, settings)

    def fail(payload):
        raise RuntimeError("boom")

    executor = Executor(store, settings, fail)
    task, _ = service.submit(
        "k1", {},
        retry_policy=RetryPolicy(max_retries=2, base_delay=60.0),
        request_id=request_id,
    )
    executor.dispatch(task)
    waiting = wait_until(
        lambda: (
            t if (t := store.get(task.task_id)).state
            is TaskState.RETRY_WAITING else None
        )
    )
    due_before = waiting.next_run_at

    _simulate_crash(store, executor)
    logger.info(
        "request=%s 模拟进程中断：task=%s 停止时状态=RETRY_WAITING",
        request_id, task.task_id,
    )

    # 重启：间隔 60s 远未到，但必须重新挂上定时器；这里用极小间隔验证"按原计划"，
    # 另造一个短间隔场景验证到期后继续执行（见下）。
    store2 = TaskStore(tmp_db)
    executor2 = Executor(store2, settings, lambda p: {"echo": p})
    recovered = store2.recover_interrupted()
    assert recovered == []  # 等待中的任务不被中断恢复改写
    still = store2.get(task.task_id)
    assert still.state is TaskState.RETRY_WAITING
    assert still.next_run_at == due_before  # 原计划时刻保持不变
    armed = executor2.schedule_retry_waiting()
    assert armed == 1
    executor2.shutdown(wait=False)
    store2.close()
    logger.info(
        "request=%s decision verified: 等待重试任务重启后保持原计划 task=%s",
        request_id, task.task_id,
    )


def test_retry_waiting_due_during_stop_runs_immediately_after_restart(
    tmp_db, request_id
):
    settings = Settings(db_path=tmp_db, handler_timeout=5)
    store = TaskStore(tmp_db)
    service = TaskService(store, settings)

    def fail(payload):
        raise RuntimeError("boom")

    executor = Executor(store, settings, fail)
    task, _ = service.submit(
        "k1", {},
        retry_policy=RetryPolicy(max_retries=1, base_delay=0.05),
        request_id=request_id,
    )
    executor.dispatch(task)
    wait_until(
        lambda: store.get(task.task_id).state is TaskState.RETRY_WAITING
    )
    _simulate_crash(store, executor)

    # 重启时等待间隔已过：定时器立即触发，第 2 次尝试成功
    store2 = TaskStore(tmp_db)
    service2 = TaskService(store2, settings)
    executor2 = Executor(store2, settings, lambda p: {"echo": p})
    store2.recover_interrupted()
    executor2.schedule_retry_waiting()
    executor2.dispatch_pending()
    final = wait_until(
        lambda: (
            t if (t := store2.get(task.task_id)).state
            is TaskState.SUCCEEDED else None
        )
    )
    assert final.attempt_no == 2
    records = service2.get_attempts(task.task_id)
    assert [a.status for a in records] == [
        AttemptStatus.FAILED, AttemptStatus.SUCCEEDED
    ]
    logger.info(
        "request=%s decision verified: 停止期间到期的等待，重启后立即继续 "
        "并成功 task=%s", request_id, task.task_id,
    )
    executor2.shutdown(wait=True)
    store2.close()


def test_interrupted_attempt_counted_and_retried_by_policy(tmp_db, request_id):
    settings = Settings(db_path=tmp_db, handler_timeout=30)
    store = TaskStore(tmp_db)
    service = TaskService(store, settings)
    block = threading.Event()

    def handler(payload):
        block.wait(60)

    executor = Executor(store, settings, handler)
    task, _ = service.submit(
        "k1", {},
        # 中断计数（默认）：被打断的第 1 次消耗一次预算
        retry_policy=RetryPolicy(max_retries=2, base_delay=0.02),
        request_id=request_id,
    )
    executor.dispatch(task)
    wait_until(lambda: store.get(task.task_id).state is TaskState.RUNNING)
    _simulate_crash(store, executor)

    store2 = TaskStore(tmp_db)
    service2 = TaskService(store2, settings)
    executor2 = Executor(store2, settings, lambda p: {"echo": p})
    decided = store2.recover_interrupted()
    assert len(decided) == 1
    after = decided[0]
    assert after.state is TaskState.RETRY_WAITING
    assert after.attempt_no == 1 and after.remaining_retries == 1
    records = service2.get_attempts(task.task_id)
    assert len(records) == 1
    assert records[0].status is AttemptStatus.FAILED
    assert records[0].failure_reason is FailureReason.INTERRUPTED
    assert records[0].counted is True
    assert records[0].next_run_at is not None  # 下一次尝试时间明确

    # 重启后定时器到点：执行第 2 次并成功，不会重复执行第 1 次
    executor2.schedule_retry_waiting()
    final = wait_until(
        lambda: (
            t if (t := store2.get(task.task_id)).state
            is TaskState.SUCCEEDED else None
        )
    )
    assert final.attempt_no == 2
    assert [a.attempt_no for a in service2.get_attempts(task.task_id)] == [1, 2]
    logger.info(
        "request=%s decision verified: 被打断尝试计数=1 次，按策略继续重试 "
        "task=%s", request_id, task.task_id,
    )
    executor2.shutdown(wait=True)
    store2.close()


def test_interrupted_attempt_uncounted_does_not_consume_budget(
    tmp_db, request_id
):
    settings = Settings(db_path=tmp_db, handler_timeout=30)
    store = TaskStore(tmp_db)
    service = TaskService(store, settings)
    block = threading.Event()
    executor = Executor(store, settings, lambda p: block.wait(60))
    task, _ = service.submit(
        "k1", {},
        retry_policy=RetryPolicy(
            max_retries=1, base_delay=0.02,
            count_interruption_as_attempt=False,
        ),
        request_id=request_id,
    )
    executor.dispatch(task)
    wait_until(lambda: store.get(task.task_id).state is TaskState.RUNNING)
    _simulate_crash(store, executor)

    store2 = TaskStore(tmp_db)
    service2 = TaskService(store2, settings)
    executor2 = Executor(store2, settings, lambda p: {"echo": p})
    decided = store2.recover_interrupted()
    after = decided[0]
    # 被打断的尝试保留审计记录但不消耗预算：剩余次数仍为 1
    assert after.state is TaskState.RETRY_WAITING
    assert after.attempt_no == 1 and after.remaining_retries == 1
    records = service2.get_attempts(task.task_id)
    assert records[0].counted is False
    executor2.schedule_retry_waiting()

    final = wait_until(
        lambda: (
            t if (t := store2.get(task.task_id)).state
            is TaskState.SUCCEEDED else None
        )
    )
    assert final.attempt_no == 2 and final.remaining_retries == 1
    logger.info(
        "request=%s decision verified: 中断不计数，预算未消耗 task=%s",
        request_id, task.task_id,
    )
    executor2.shutdown(wait=True)
    store2.close()


def test_interrupted_without_retry_budget_becomes_final_failed(
    tmp_db, request_id
):
    # 直接构造"第 2 次尝试 RUNNING 中、预算已尽"的崩溃现场，避免阻塞线程泄漏。
    settings = Settings(db_path=tmp_db, handler_timeout=30)
    store = TaskStore(tmp_db)
    service = TaskService(store, settings)
    executor = Executor(store, settings, lambda p: {"echo": p})
    task, _ = service.submit(
        "k1", {},
        retry_policy=RetryPolicy(max_retries=1, base_delay=0.02),
        request_id=request_id,
    )
    # 第 1 次尝试：失败并安排重试（remaining_retries 1 -> 0）
    claimed = store.start_attempt(task.task_id, task.version)
    waiting = store.schedule_retry(
        task.task_id, claimed.version, FailureReason.HANDLER_ERROR,
        next_run_at=claimed.created_at + 100, consume_retry=True,
    )
    assert waiting.remaining_retries == 0
    # 第 2 次尝试已开始（RUNNING + IN_PROGRESS 尝试记录），随后进程停止
    running = store.start_attempt(task.task_id, waiting.version)
    assert running.attempt_no == 2
    _simulate_crash(store, executor)

    store2 = TaskStore(tmp_db)
    decided = store2.recover_interrupted()
    final = decided[0]
    assert final.state is TaskState.FAILED
    assert final.failure_reason is FailureReason.INTERRUPTED
    assert final.attempt_no == 2
    records = store2.list_attempts(task.task_id)
    assert [a.attempt_no for a in records] == [1, 2]
    assert records[1].failure_reason is FailureReason.INTERRUPTED
    logger.info(
        "request=%s decision verified: 预算用尽后被中断 -> 最终失败 task=%s",
        request_id, task.task_id,
    )
    store2.close()


def test_interrupted_retry_disabled_becomes_final_failed(tmp_db, request_id):
    settings = Settings(db_path=tmp_db, handler_timeout=30)
    store = TaskStore(tmp_db)
    service = TaskService(store, settings)
    block = threading.Event()
    executor = Executor(store, settings, lambda p: block.wait(60))
    task, _ = service.submit(
        "k1", {},
        retry_policy=RetryPolicy(
            max_retries=3, base_delay=0.02, retry_on_interruption=False,
        ),
        request_id=request_id,
    )
    executor.dispatch(task)
    wait_until(lambda: store.get(task.task_id).state is TaskState.RUNNING)
    _simulate_crash(store, executor)

    store2 = TaskStore(tmp_db)
    decided = store2.recover_interrupted()
    final = decided[0]
    assert final.state is TaskState.FAILED
    assert final.failure_reason is FailureReason.INTERRUPTED
    records = store2.list_attempts(task.task_id)
    assert len(records) == 1 and records[0].counted is True
    store2.close()
    logger.info(
        "request=%s decision verified: 策略不重试中断 -> 最终失败 task=%s",
        request_id, task.task_id,
    )


def test_legacy_database_migrates_and_recovers(tmp_db, request_id):
    """旧版本（无重试字段、无 attempts 表）的数据库可直接被新版本打开，
    遗留的 RUNNING 任务补记第 1 次尝试并判定为 FAILED/INTERRUPTED。"""
    import sqlite3

    conn = sqlite3.connect(tmp_db)
    conn.execute(
        "CREATE TABLE tasks (task_id TEXT PRIMARY KEY, "
        "idempotency_key TEXT NOT NULL UNIQUE, payload TEXT NOT NULL, "
        "state TEXT NOT NULL, failure_reason TEXT, result TEXT, "
        "version INTEGER NOT NULL, created_at REAL NOT NULL, "
        "updated_at REAL NOT NULL)"
    )
    conn.execute(
        "INSERT INTO tasks VALUES ('t1','k1','{}','PENDING',NULL,NULL,"
        "0,1.0,1.0)"
    )
    conn.execute(
        "INSERT INTO tasks VALUES ('t2','k2','{}','RUNNING',NULL,NULL,"
        "0,1.0,1.0)"
    )
    conn.commit()
    conn.close()

    store = TaskStore(tmp_db)
    t1 = store.get("t1")
    assert t1.state is TaskState.PENDING
    assert t1.retry_policy is None and t1.attempt_no == 0
    assert t1.remaining_retries == 0 and t1.next_run_at is None

    recovered = store.recover_interrupted()
    assert [t.task_id for t in recovered] == ["t2"]
    t2 = store.get("t2")
    assert t2.state is TaskState.FAILED
    assert t2.failure_reason is FailureReason.INTERRUPTED
    records = store.list_attempts("t2")
    assert len(records) == 1
    assert records[0].attempt_no == 1
    assert records[0].status is AttemptStatus.FAILED
    logger.info(
        "request=%s decision verified: 旧库自动迁移并恢复遗留 RUNNING 任务",
        request_id,
    )
    store.close()
