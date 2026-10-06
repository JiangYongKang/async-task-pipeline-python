"""失败自动重试测试：次数用尽落最终失败、指数退避、逐次尝试追溯、
成功于某次重试后停止，以及不带策略任务的兼容行为。"""
from __future__ import annotations

import logging
import threading

from taskpipe.config import Settings
from taskpipe.models import (
    AttemptStatus,
    BackoffStrategy,
    FailureReason,
    RetryPolicy,
    TaskState,
)
from tests.conftest import wait_until

logger = logging.getLogger("taskpipe.test")


def test_retry_exhausted_then_final_failed(make_stack, request_id):
    attempts: list[int] = []
    lock = threading.Lock()

    def handler(payload):
        with lock:
            attempts.append(1)
        raise RuntimeError("boom")

    policy = RetryPolicy(max_retries=2, base_delay=0.02)
    store, service, executor, _ = make_stack(
        Settings(handler_timeout=5), handler=handler
    )
    task, duplicated = service.submit(
        "k1", {}, retry_policy=policy, request_id=request_id
    )
    assert duplicated is False
    assert task.remaining_retries == 2
    executor.dispatch(task)

    final = wait_until(
        lambda: (
            t if (t := store.get(task.task_id)).state is TaskState.FAILED
            else None
        )
    )
    # 首次 + 2 次重试，共 3 次尝试
    assert len(attempts) == 3
    assert final.failure_reason is FailureReason.HANDLER_ERROR
    assert final.attempt_no == 3
    assert final.remaining_retries == 0
    assert final.next_run_at is None

    records = service.get_attempts(task.task_id)
    assert [a.attempt_no for a in records] == [1, 2, 3]
    assert [a.status for a in records] == [AttemptStatus.FAILED] * 3
    assert records[0].is_retry is False
    assert records[1].is_retry is True and records[2].is_retry is True
    # 失败且安排了下一次的尝试带有预计开始时间；最后一次没有
    assert records[0].next_run_at is not None
    assert records[1].next_run_at is not None
    assert records[2].next_run_at is None
    logger.info(
        "request=%s decision verified: 重试用尽 task=%s attempts=%s 终态=FAILED",
        request_id, task.task_id, len(records),
    )


def test_retry_succeeds_on_second_attempt_and_stops(make_stack, request_id):
    calls = {"n": 0}
    lock = threading.Lock()

    def handler(payload):
        with lock:
            calls["n"] += 1
            n = calls["n"]
        if n == 1:
            raise RuntimeError("transient")
        return {"ok": True, "n": n}

    policy = RetryPolicy(max_retries=3, base_delay=0.02)
    store, service, executor, _ = make_stack(
        Settings(handler_timeout=5), handler=handler
    )
    task, _ = service.submit(
        "k1", {}, retry_policy=policy, request_id=request_id
    )
    executor.dispatch(task)

    final = wait_until(
        lambda: (
            t if (t := store.get(task.task_id)).state
            is TaskState.SUCCEEDED else None
        )
    )
    assert calls["n"] == 2  # 第二次成功，不再继续重试
    assert final.attempt_no == 2
    assert final.remaining_retries == 2  # 只用掉一次预算
    assert final.result == {"ok": True, "n": 2}

    records = service.get_attempts(task.task_id)
    assert [a.status for a in records] == [
        AttemptStatus.FAILED, AttemptStatus.SUCCEEDED
    ]
    assert records[0].failure_reason is FailureReason.HANDLER_ERROR
    assert records[1].failure_reason is None
    assert records[1].finished_at is not None and records[1].next_run_at is None
    logger.info(
        "request=%s decision verified: 第 2 次尝试成功后停止 task=%s",
        request_id, task.task_id,
    )


def test_retry_waiting_visible_between_attempts(make_stack, request_id):
    def handler(payload):
        raise RuntimeError("fail until cancelled")

    policy = RetryPolicy(max_retries=2, base_delay=5.0)
    store, service, executor, _ = make_stack(
        Settings(handler_timeout=5), handler=handler
    )
    task, _ = service.submit(
        "k1", {}, retry_policy=policy, request_id=request_id
    )
    executor.dispatch(task)

    waiting = wait_until(
        lambda: (
            t if (t := store.get(task.task_id)).state
            is TaskState.RETRY_WAITING else None
        )
    )
    assert waiting.attempt_no == 1
    assert waiting.remaining_retries == 1
    assert waiting.next_run_at is not None
    # 尝试追溯里第 1 次已失败、标注下一次预计开始时间
    records = service.get_attempts(task.task_id)
    assert len(records) == 1
    assert records[0].status is AttemptStatus.FAILED
    assert records[0].next_run_at == waiting.next_run_at
    logger.info(
        "request=%s decision verified: 等待重试可见 task=%s attempt=%s "
        "remaining=%s next_run_at=%.3f",
        request_id, task.task_id, waiting.attempt_no,
        waiting.remaining_retries, waiting.next_run_at,
    )


def test_exponential_backoff_delays():
    policy = RetryPolicy(
        max_retries=4, base_delay=0.1,
        strategy=BackoffStrategy.EXPONENTIAL, factor=2.0, max_delay=1.0,
    )
    assert policy.delay_for(2) == 0.1
    assert policy.delay_for(3) == 0.2
    assert policy.delay_for(4) == 0.4
    assert policy.delay_for(5) == 0.8
    assert policy.delay_for(6) == 1.0  # 被 max_delay 截断


def test_no_policy_task_fails_immediately_like_before(make_stack, request_id):
    def handler(payload):
        raise RuntimeError("boom")

    store, service, executor, _ = make_stack(
        Settings(handler_timeout=5), handler=handler
    )
    task, _ = service.submit("k1", {}, request_id=request_id)
    executor.dispatch(task)
    final = wait_until(
        lambda: (
            t if (t := store.get(task.task_id)).state is TaskState.FAILED
            else None
        )
    )
    assert final.attempt_no == 1 and final.remaining_retries == 0
    assert final.failure_reason is FailureReason.HANDLER_ERROR
    records = service.get_attempts(task.task_id)
    assert len(records) == 1 and records[0].status is AttemptStatus.FAILED
    logger.info(
        "request=%s decision verified: 无策略任务行为不变 task=%s",
        request_id, task.task_id,
    )


def test_duplicate_submit_with_retry_policy_keeps_first(make_stack, request_id):
    store, service, _, _ = make_stack()
    first, dup1 = service.submit(
        "k1", {}, retry_policy=RetryPolicy(max_retries=3, base_delay=0.01),
        request_id=request_id,
    )
    assert dup1 is False
    # 重复提交即便带不同策略，也返回首次任务、不新建
    second, dup2 = service.submit(
        "k1", {}, retry_policy=RetryPolicy(max_retries=99),
        request_id=request_id,
    )
    assert dup2 is True and second.task_id == first.task_id
    assert second.remaining_retries == 3
    assert len(store.list_all()) == 1
