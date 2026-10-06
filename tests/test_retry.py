"""自动重试与尝试追溯测试：次数用尽落终态、等待期取消、重试与取消撞车、
尝试记录可查、不可重试失败立即落终态、重试不突破容量。"""
from __future__ import annotations

import logging
import threading
import time

from taskpipe.config import Settings
from taskpipe.models import (
    AttemptKind,
    AttemptResult,
    BackoffKind,
    FailureReason,
    RetryPolicy,
    TaskState,
)
from taskpipe.retry import NonRetryableError
from tests.conftest import wait_until

logger = logging.getLogger("taskpipe.test")


def test_retry_succeeds_before_exhaustion(make_stack, request_id):
    attempts: list[int] = []
    lock = threading.Lock()

    def handler(payload):
        with lock:
            attempts.append(1)
            n = len(attempts)
        if n < 3:
            raise RuntimeError(f"transient failure #{n}")
        return {"ok": True, "tries": n}

    store, service, executor, _ = make_stack(
        Settings(handler_timeout=5), handler=handler
    )
    task, _ = service.submit(
        "k1", {}, request_id=request_id,
        retry_policy=RetryPolicy(max_retries=3, interval=0.05),
    )
    executor.dispatch(task)
    final = wait_until(
        lambda: (
            t if (t := store.get(task.task_id)).state is TaskState.SUCCEEDED
            else None
        )
    )
    assert final.result == {"ok": True, "tries": 3}
    assert final.attempt_count == 3
    records = service.get_attempts(task.task_id)
    assert [a.attempt_no for a in records] == [1, 2, 3]
    assert records[0].kind is AttemptKind.INITIAL
    assert records[1].kind is AttemptKind.RETRY
    assert [a.result for a in records[:2]] == [
        AttemptResult.FAILURE, AttemptResult.FAILURE
    ]
    assert records[0].failure_reason is FailureReason.HANDLER_ERROR
    assert records[2].result is AttemptResult.SUCCESS
    logger.info(
        "request=%s decision verified: 第 3 次尝试成功，前 2 次失败留痕 task=%s",
        request_id, task.task_id,
    )


def test_retries_exhausted_then_final_failed(make_stack, request_id):
    counter = {"n": 0}
    lock = threading.Lock()

    def handler(payload):
        with lock:
            counter["n"] += 1
        raise RuntimeError("always fails")

    store, service, executor, _ = make_stack(
        Settings(handler_timeout=5), handler=handler
    )
    task, _ = service.submit(
        "k1", {}, request_id=request_id,
        retry_policy=RetryPolicy(max_retries=2, interval=0.03),
    )
    executor.dispatch(task)
    final = wait_until(
        lambda: (
            t if (t := store.get(task.task_id)).state is TaskState.FAILED
            else None
        )
    )
    assert final.failure_reason is FailureReason.HANDLER_ERROR
    # 共 3 次尝试：首次 + 2 次重试，全部留痕
    assert counter["n"] == 3
    records = service.get_attempts(task.task_id)
    assert len(records) == 3
    assert all(a.result is AttemptResult.FAILURE for a in records)
    assert all(a.finished_at is not None for a in records)
    logger.info(
        "request=%s decision verified: 3 次尝试用尽后落 FAILED task=%s",
        request_id, task.task_id,
    )


def test_task_view_shows_attempt_progress_and_next_run(make_stack, request_id):
    def handler(payload):
        time.sleep(5)  # 必定超过 handler_timeout，首轮快速归一化为超时失败

    store, service, executor, _ = make_stack(
        Settings(handler_timeout=0.1), handler=handler
    )
    task, _ = service.submit(
        "k1", {}, request_id=request_id,
        retry_policy=RetryPolicy(max_retries=2, interval=5.0),
    )
    executor.dispatch(task)
    waiting = wait_until(
        lambda: (
            t if (t := store.get(task.task_id)).state
            is TaskState.WAITING_RETRY else None
        )
    )
    # 等待期间一眼可见：当前第几次、还剩几次、下次重试时间
    assert waiting.attempt_count == 1
    assert waiting.max_retries - waiting.attempt_count == 1
    assert waiting.next_run_at is not None
    assert waiting.next_run_at > time.time()
    cancelled = service.cancel(task.task_id, request_id=request_id)
    assert cancelled.state is TaskState.CANCELLED


def test_cancel_during_waiting_retry(make_stack, request_id):
    started = threading.Event()

    def handler(payload):
        started.set()
        raise RuntimeError("fail then wait")

    store, service, executor, _ = make_stack(
        Settings(handler_timeout=5), handler=handler
    )
    task, _ = service.submit(
        "k1", {}, request_id=request_id,
        retry_policy=RetryPolicy(max_retries=3, interval=60.0),
    )
    executor.dispatch(task)
    wait_until(lambda: started.is_set())
    wait_until(
        lambda: store.get(task.task_id).state is TaskState.WAITING_RETRY
    )
    cancelled = service.cancel(task.task_id, request_id=request_id)
    assert cancelled.state is TaskState.CANCELLED
    # 等待很久后也不会再执行：取消后尝试总数停在 1
    time.sleep(0.2)
    again = store.get(task.task_id)
    assert again.state is TaskState.CANCELLED and again.attempt_count == 1
    records = service.get_attempts(task.task_id)
    assert len(records) == 1 and records[0].result is AttemptResult.FAILURE
    logger.info(
        "request=%s decision verified: 等待重试期间取消生效，不再重试 task=%s",
        request_id, task.task_id,
    )


def test_cancel_racing_retry_single_outcome(make_stack, request_id):
    # 取消正好撞上某一次重试开始：多轮重复下最终结论必须唯一且稳定。

    for round_no in range(8):
        gate = threading.Event()

        def handler(payload):
            gate.wait(2)
            raise RuntimeError("fail")

        store, service, executor, _ = make_stack(
            Settings(handler_timeout=5), handler=handler
        )
        task, _ = service.submit(
            f"race-{round_no}", {}, request_id=request_id,
            retry_policy=RetryPolicy(max_retries=5, interval=0.02),
        )
        executor.dispatch(task)
        wait_until(
            lambda: store.get(task.task_id).state
            is TaskState.WAITING_RETRY
        )
        # 下一次重试马上到点；在窗口内循环发起取消，直到状态确定
        deadline = time.time() + 2
        while time.time() < deadline:
            current = store.get(task.task_id)
            if current.state in (
                TaskState.CANCELLED, TaskState.FAILED, TaskState.SUCCEEDED
            ):
                break
            if current.state in (
                TaskState.WAITING_RETRY, TaskState.RUNNING
            ):
                try:
                    service.cancel(task.task_id, request_id=request_id)
                except Exception:
                    pass
            time.sleep(0.005)
        gate.set()
        executor.shutdown(wait=True)
        final = store.get(task.task_id)
        assert final.state is TaskState.CANCELLED  # 次数用不尽，取消必赢
        # 结论稳定
        assert store.get(task.task_id).state is TaskState.CANCELLED
        # 每条尝试记录要么 FAILURE（等满重试间隔）要么 CANCELLED（撞上）
        records = service.get_attempts(task.task_id)
        assert all(
            a.result in (AttemptResult.FAILURE, AttemptResult.CANCELLED)
            for a in records
        )
        store.close()
    logger.info(
        "request=%s decision verified: 取消撞重试 8 轮终态唯一=CANCELLED",
        request_id,
    )


def test_fatal_failure_not_retried(make_stack, request_id):
    counter = {"n": 0}
    lock = threading.Lock()

    def handler(payload):
        with lock:
            counter["n"] += 1
        raise NonRetryableError("deterministic bad input")

    store, service, executor, _ = make_stack(
        Settings(handler_timeout=5), handler=handler
    )
    task, _ = service.submit(
        "k1", {}, request_id=request_id,
        retry_policy=RetryPolicy(max_retries=5, interval=0.01),
    )
    executor.dispatch(task)
    final = wait_until(
        lambda: (
            t if (t := store.get(task.task_id)).state is TaskState.FAILED
            else None
        )
    )
    assert final.failure_reason is FailureReason.HANDLER_FATAL
    assert counter["n"] == 1  # 确定性失败不重试
    time.sleep(0.1)
    assert counter["n"] == 1
    logger.info(
        "request=%s decision verified: HANDLER_FATAL 不重试，一次落终态 task=%s",
        request_id, task.task_id,
    )


def test_timeout_is_retried(make_stack, request_id):
    slow = {"n": 0}
    lock = threading.Lock()

    def handler(payload):
        with lock:
            slow["n"] += 1
            n = slow["n"]
        if n == 1:
            time.sleep(1)  # 首轮超过超时
        return {"n": n}

    store, service, executor, _ = make_stack(
        Settings(handler_timeout=0.1), handler=handler
    )
    task, _ = service.submit(
        "k1", {}, request_id=request_id,
        retry_policy=RetryPolicy(max_retries=1, interval=0.03),
    )
    executor.dispatch(task)
    final = wait_until(
        lambda: (
            t if (t := store.get(task.task_id)).state
            in (TaskState.SUCCEEDED, TaskState.FAILED) else None
        )
    )
    assert final.state is TaskState.SUCCEEDED and final.result == {"n": 2}
    records = service.get_attempts(task.task_id)
    assert records[0].failure_reason is FailureReason.HANDLER_TIMEOUT
    logger.info(
        "request=%s decision verified: 首轮超时重试后成功 task=%s",
        request_id, task.task_id,
    )


def test_no_policy_task_fails_immediately(make_stack, request_id):
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
    assert final.failure_reason is FailureReason.HANDLER_ERROR
    time.sleep(0.1)  # 不带策略：不会有第二次尝试
    assert store.get(task.task_id).attempt_count == 1
    assert len(service.get_attempts(task.task_id)) == 1
    logger.info(
        "request=%s decision verified: 不带策略行为不变，一次失败即终态",
        request_id,
    )


def test_waiting_retry_tasks_keep_capacity_and_reject_new(make_stack, request_id):
    # 等待重试的任务保留位置（计入容量）：槽位被它们占满时，
    # 新请求仍然明确拒绝；重试本身不产生新任务、不重新过容量判定。
    def handler(payload):
        raise RuntimeError("fail into waiting")

    store, service, executor, _ = make_stack(
        Settings(max_pending=2, handler_timeout=5), handler=handler
    )
    t1, _ = service.submit(
        "k1", {}, request_id=request_id,
        retry_policy=RetryPolicy(max_retries=3, interval=60.0),
    )
    t2, _ = service.submit(
        "k2", {}, request_id=request_id,
        retry_policy=RetryPolicy(max_retries=3, interval=60.0),
    )
    executor.dispatch(t1)
    executor.dispatch(t2)
    wait_until(
        lambda: store.get(t1.task_id).state is TaskState.WAITING_RETRY
    )
    wait_until(
        lambda: store.get(t2.task_id).state is TaskState.WAITING_RETRY
    )
    # 两个等待重试任务占满 2 个槽位：新请求立即被拒绝
    from taskpipe.service import ServiceRejected
    from taskpipe.models import RejectReason
    try:
        service.submit("k3", {}, request_id=request_id)
        raise AssertionError("expected OVER_CAPACITY")
    except ServiceRejected as exc:
        assert exc.reason is RejectReason.OVER_CAPACITY

    # 重试不产生新任务：库里始终只有 2 个任务
    time.sleep(0.1)
    assert {t.task_id for t in store.list_all()} == {t1.task_id, t2.task_id}
    service.cancel(t1.task_id, request_id=request_id)
    service.cancel(t2.task_id, request_id=request_id)
    # 取消释放槽位后新请求可进入
    t3, dup = service.submit("k3", {}, request_id=request_id)
    assert dup is False
    logger.info(
        "request=%s decision verified: 等待重试占容量，新请求被拒；取消后释放",
        request_id,
    )
    executor.dispatch(t3)


def test_retrying_task_remains_one_task_across_attempts(make_stack, request_id):
    counter = {"n": 0}
    lock = threading.Lock()

    def handler(payload):
        with lock:
            counter["n"] += 1
            n = counter["n"]
        if n < 3:
            raise RuntimeError("transient")
        return {"n": n}

    store, service, executor, _ = make_stack(
        Settings(handler_timeout=5), handler=handler
    )
    task, _ = service.submit(
        "k1", {}, request_id=request_id,
        retry_policy=RetryPolicy(max_retries=3, interval=0.02),
    )
    executor.dispatch(task)
    wait_until(lambda: store.get(task.task_id).state is TaskState.SUCCEEDED)
    assert len(store.list_all()) == 1  # 多次尝试始终对应同一个任务
    # 结束后重复提交：幂等返回同一任务，不重新执行
    again, duplicated = service.submit("k1", {}, request_id=request_id)
    assert duplicated is True and again.task_id == task.task_id
    time.sleep(0.1)
    assert store.get(task.task_id).attempt_count == 3
    logger.info(
        "request=%s decision verified: 重试全程一个任务，终态后重复提交仍幂等",
        request_id,
    )


def test_duplicate_submit_during_waiting_returns_same_task(make_stack, request_id):
    started = threading.Event()

    def handler(payload):
        started.set()
        raise RuntimeError("fail")

    store, service, executor, _ = make_stack(
        Settings(handler_timeout=5), handler=handler
    )
    task, _ = service.submit(
        "k1", {"v": 1}, request_id=request_id,
        retry_policy=RetryPolicy(max_retries=2, interval=60.0),
    )
    executor.dispatch(task)
    wait_until(lambda: started.is_set())
    wait_until(
        lambda: store.get(task.task_id).state is TaskState.WAITING_RETRY
    )
    # 等待重试期间重复提交（即便带不同 payload/策略）：返回同一任务
    again, duplicated = service.submit(
        "k1", {"v": 999}, request_id=request_id,
        retry_policy=RetryPolicy(max_retries=99, interval=1.0),
    )
    assert duplicated is True
    assert again.task_id == task.task_id
    stored = store.get(task.task_id)
    assert stored.max_retries == 2  # 沿用首次提交的策略，不被重复提交改写
    service.cancel(task.task_id, request_id=request_id)
    logger.info(
        "request=%s decision verified: 等待中重复提交幂等，策略不被覆盖 task=%s",
        request_id, task.task_id,
    )


def test_retry_respects_concurrency_bound(make_stack, request_id):
    current = 0
    peak = 0
    lock = threading.Lock()

    def handler(payload):
        nonlocal current, peak
        with lock:
            current += 1
            peak = max(peak, current)
        time.sleep(0.05)
        with lock:
            current -= 1
        if payload.get("flaky"):
            raise RuntimeError("transient")
        return None

    store, service, executor, _ = make_stack(
        Settings(max_concurrency=2, max_pending=32, handler_timeout=5),
        handler=handler,
    )
    tasks = [
        service.submit(
            f"k{i}", {"flaky": True}, request_id=request_id,
            retry_policy=RetryPolicy(max_retries=2, interval=0.0),
        )[0]
        for i in range(6)
    ]
    for t in tasks:
        executor.dispatch(t)
    wait_until(
        lambda: all(
            store.get(t.task_id).state is TaskState.FAILED for t in tasks
        )
    )
    assert peak == 2  # 含重试在内，并发度始终不超过 max_concurrency
    for t in tasks:
        assert store.get(t.task_id).attempt_count == 3
    logger.info(
        "request=%s decision verified: 重试场景峰值并发=%s 未突破上限",
        request_id, peak,
    )
