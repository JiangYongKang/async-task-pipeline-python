"""任务领域模型：状态、失败原因、拒绝原因与任务记录。"""
from __future__ import annotations

import enum
from dataclasses import dataclass


class TaskState(str, enum.Enum):
    """任务状态机中的状态。"""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRY_WAITING = "RETRY_WAITING"  # 一次尝试失败，等待间隔后自动重试
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class FailureReason(str, enum.Enum):
    """执行失败的归一化分类（不向外透出内部异常细节）。"""

    HANDLER_ERROR = "HANDLER_ERROR"        # 业务处理器抛出的异常
    HANDLER_TIMEOUT = "HANDLER_TIMEOUT"    # 执行超时
    INTERRUPTED = "INTERRUPTED"            # 进程中断导致执行被打断（重启后判定）


class BackoffStrategy(str, enum.Enum):
    """两次尝试之间的间隔计算方式。"""

    FIXED = "FIXED"                # 固定间隔 base_delay
    EXPONENTIAL = "EXPONENTIAL"    # 指数退避 base_delay * factor**(n-2)，上限 max_delay


class AttemptStatus(str, enum.Enum):
    """单次尝试的结果状态。"""

    IN_PROGRESS = "IN_PROGRESS"    # 已开始尚未结束（含崩溃时未闭合的尝试）
    SUCCEEDED = "SUCCEEDED"        # 本次尝试成功
    FAILED = "FAILED"              # 本次尝试失败（failure_reason 给出分类）
    CANCELLED = "CANCELLED"        # 本次尝试因任务被取消而作废（输掉竞态）


# 默认判定为"适合自动重试"的失败：业务异常与超时通常具有偶发性；
# INTERRUPTED（进程中断）是否重试由 RetryPolicy.retry_on_interruption 单独决定。
DEFAULT_RETRYABLE_REASONS: frozenset[FailureReason] = frozenset(
    {FailureReason.HANDLER_ERROR, FailureReason.HANDLER_TIMEOUT}
)


@dataclass(frozen=True)
class RetryPolicy:
    """失败自动重试策略。

    * ``max_retries``：首次执行之外最多再试多少次（总尝试次数 = 1 + max_retries）；
      为 0 时表示不重试，任务保持与旧版本完全一致的行为。
    * 间隔：``FIXED`` 固定 ``base_delay``；``EXPONENTIAL`` 第 n 次尝试前等待
      ``min(max_delay, base_delay * factor**(n-2))``（n 从 2 起）。
    * ``retry_on_interruption``：进程在某次尝试执行期间被停掉，重启后是否继续重试；
    * ``count_interruption_as_attempt``：被中断的那次执行是否消耗一次尝试预算
      （审计记录始终保留，此开关只影响预算计数）。
    """

    max_retries: int = 0
    base_delay: float = 1.0
    strategy: BackoffStrategy = BackoffStrategy.FIXED
    factor: float = 2.0
    max_delay: float = 60.0
    retry_on_interruption: bool = True
    count_interruption_as_attempt: bool = True

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("delay must be >= 0")
        if self.strategy is BackoffStrategy.EXPONENTIAL and self.factor < 1:
            raise ValueError("factor must be >= 1 for EXPONENTIAL backoff")

    @property
    def max_attempts(self) -> int:
        """总共允许的尝试次数（首次 + 重试）。"""
        return self.max_retries + 1

    def is_retryable(self, reason: FailureReason) -> bool:
        """运行期失败是否适合自动重试（中断走 retry_on_interruption 单独判定）。"""
        return reason in DEFAULT_RETRYABLE_REASONS

    def delay_for(self, upcoming_attempt_no: int) -> float:
        """计算第 ``upcoming_attempt_no`` 次尝试（>=2）开始前的等待秒数。"""
        if self.strategy is BackoffStrategy.FIXED:
            return self.base_delay
        exponent = max(0, upcoming_attempt_no - 2)
        return min(self.max_delay, self.base_delay * self.factor**exponent)

    def to_dict(self) -> dict:
        return {
            "max_retries": self.max_retries,
            "base_delay": self.base_delay,
            "strategy": self.strategy.value,
            "factor": self.factor,
            "max_delay": self.max_delay,
            "retry_on_interruption": self.retry_on_interruption,
            "count_interruption_as_attempt": self.count_interruption_as_attempt,
        }

    @classmethod
    def from_dict(cls, data: dict) -> RetryPolicy:
        return cls(
            max_retries=int(data["max_retries"]),
            base_delay=float(data["base_delay"]),
            strategy=BackoffStrategy(data.get("strategy", BackoffStrategy.FIXED.value)),
            factor=float(data.get("factor", 2.0)),
            max_delay=float(data.get("max_delay", 60.0)),
            retry_on_interruption=bool(data.get("retry_on_interruption", True)),
            count_interruption_as_attempt=bool(
                data.get("count_interruption_as_attempt", True)
            ),
        )


@dataclass
class AttemptRecord:
    """一次尝试（attempt）的追溯记录：第几次、何时跑、结果/失败分类、
    是首次执行还是重试、若将继续重试则给出下一次预计开始时间。"""

    task_id: str
    attempt_no: int                  # 从 1 开始的执行序号
    status: AttemptStatus
    is_retry: bool                   # False=首次执行；True=重试
    failure_reason: FailureReason | None = None
    counted: bool = True             # 是否消耗尝试预算（中断尝试可配置为不计数）
    started_at: float = 0.0
    finished_at: float | None = None
    next_run_at: float | None = None  # 本次失败后下一次尝试预计开始时间（无则 None）


class RejectReason(str, enum.Enum):
    """提交/状态迁移被拒绝的可区分原因。"""

    OVER_CAPACITY = "OVER_CAPACITY"                # 超出承载能力
    ILLEGAL_TRANSITION = "ILLEGAL_TRANSITION"      # 非法状态迁移
    TERMINAL_STATE = "TERMINAL_STATE"              # 终态不可改写
    TASK_NOT_FOUND = "TASK_NOT_FOUND"              # 任务不存在


class TransitionError(Exception):
    """非法状态迁移异常，携带可区分的拒绝原因。"""

    def __init__(self, reason: RejectReason, message: str) -> None:
        super().__init__(message)
        self.reason = reason


@dataclass
class Task:
    """任务记录。"""

    task_id: str
    idempotency_key: str
    payload: dict
    state: TaskState = TaskState.PENDING
    failure_reason: FailureReason | None = None
    result: object | None = None
    version: int = 0  # 乐观锁版本号，用于并发迁移裁决
    created_at: float = 0.0
    updated_at: float = 0.0
    # 重试与尝试追溯（无策略的旧任务保持默认值，行为与以前一致）
    retry_policy: RetryPolicy | None = None
    attempt_no: int = 0              # 已开始的尝试序号（当前/最近一次）
    remaining_retries: int = 0       # 剩余可重试次数
    next_run_at: float | None = None  # RETRY_WAITING 时下一次尝试预计开始时间
