"""重试策略纯逻辑测试：次数语义、固定/指数间隔、默认可重试范围。"""
from __future__ import annotations

from taskpipe.models import BackoffKind, FailureReason, RetryPolicy
from taskpipe.retry import (
    DEFAULT_RETRYABLE_REASONS,
    delay_for,
    should_retry,
)


def test_no_policy_never_retries():
    assert should_retry(None, 1, FailureReason.HANDLER_ERROR) is False
    assert should_retry(RetryPolicy(max_retries=0), 1,
                        FailureReason.HANDLER_TIMEOUT) is False


def test_retry_until_attempts_exhausted():
    p = RetryPolicy(max_retries=2, interval=1.0)
    # 第 1 次（首次执行）失败：还可重试 2 次
    assert should_retry(p, 1, FailureReason.HANDLER_ERROR) is True
    # 第 2 次失败：还可重试 1 次
    assert should_retry(p, 2, FailureReason.HANDLER_ERROR) is True
    # 第 3 次失败：次数用尽（共 3 次尝试 = 1 次首次 + 2 次重试）
    assert should_retry(p, 3, FailureReason.HANDLER_ERROR) is False
    assert should_retry(p, 4, FailureReason.HANDLER_ERROR) is False


def test_default_retryable_reasons_scope():
    assert FailureReason.HANDLER_ERROR in DEFAULT_RETRYABLE_REASONS
    assert FailureReason.HANDLER_TIMEOUT in DEFAULT_RETRYABLE_REASONS
    assert FailureReason.INTERRUPTED in DEFAULT_RETRYABLE_REASONS
    # HANDLER_FATAL 默认不可重试
    assert FailureReason.HANDLER_FATAL not in DEFAULT_RETRYABLE_REASONS
    p = RetryPolicy(max_retries=5)
    assert should_retry(p, 1, FailureReason.HANDLER_FATAL) is False


def test_fixed_delay_is_constant():
    p = RetryPolicy(max_retries=3, interval=0.5, backoff=BackoffKind.FIXED)
    assert delay_for(p, 1) == 0.5
    assert delay_for(p, 2) == 0.5
    assert delay_for(p, 3) == 0.5


def test_exponential_delay_grows_with_attempt():
    p = RetryPolicy(max_retries=3, interval=0.5,
                    backoff=BackoffKind.EXPONENTIAL)
    assert delay_for(p, 1) == 0.5
    assert delay_for(p, 2) == 1.0
    assert delay_for(p, 3) == 2.0


def test_custom_retryable_scope_override():
    p = RetryPolicy(max_retries=3)
    # 调用方可缩小可重试范围
    assert should_retry(p, 1, FailureReason.HANDLER_ERROR,
                        retryable=frozenset()) is False
