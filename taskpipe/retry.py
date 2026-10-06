"""重试策略判定（纯逻辑，不依赖存储与执行）。

默认重试规则（工程默认值，可通过 ``should_retry`` 的参数覆盖）：

* 可重试：``HANDLER_ERROR``（处理器抛异常，多为偶发）、
  ``HANDLER_TIMEOUT``（超时，常因下游瞬时抖动）、
  ``INTERRUPTED``（进程中断打断的那一次，重启后按策略继续）；
* 不可重试：``HANDLER_FATAL``（处理器显式抛出 :class:`NonRetryableError`
  标记的确定性失败，重试无意义）。

重试次数语义：``max_retries`` 是"首次执行之外最多再试的次数"。
因此一次尝试失败后，当 ``used_attempts <= max_retries`` 时仍有下一次。
"""
from __future__ import annotations

from .models import BackoffKind, FailureReason, RetryPolicy

# 默认视为可重试的失败范围（工程默认规则）
DEFAULT_RETRYABLE_REASONS: frozenset[FailureReason] = frozenset(
    {
        FailureReason.HANDLER_ERROR,
        FailureReason.HANDLER_TIMEOUT,
        FailureReason.INTERRUPTED,
    }
)


class NonRetryableError(Exception):
    """业务处理器用它显式标记"确定性失败，不要重试"。

    执行器会把它归一化为 ``HANDLER_FATAL``，内部细节只进日志。
    """


def delay_for(policy: RetryPolicy, used_attempts: int) -> float:
    """计算第 ``used_attempts`` 次尝试失败后，到下一次尝试的等待秒数。

    * ``FIXED``：恒为 ``interval``；
    * ``EXPONENTIAL``：``interval * 2**(used_attempts - 1)``
      （第 1 次失败后等 interval，第 2 次后等 2*interval，依此类推）。
    """
    if policy.backoff is BackoffKind.EXPONENTIAL:
        factor = 2 ** max(0, used_attempts - 1)
        return policy.interval * factor
    return policy.interval


def should_retry(
    policy: RetryPolicy | None,
    used_attempts: int,
    reason: FailureReason,
    *,
    retryable: frozenset[FailureReason] = DEFAULT_RETRYABLE_REASONS,
) -> bool:
    """判定一次失败后是否应继续重试。

    :param policy: 任务携带的重试策略；None（或 max_retries=0）表示
        不带策略，永远不重试，保持原有行为。
    :param used_attempts: 已发生的尝试次数（含刚失败的这一次，从 1 起）。
    :param reason: 本次失败的归一化原因。
    """
    if policy is None or policy.max_retries <= 0:
        return False
    if reason not in retryable:
        return False
    return used_attempts <= policy.max_retries
