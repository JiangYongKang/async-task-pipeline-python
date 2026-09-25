"""任务状态机：定义合法迁移并裁决迁移请求。

状态图::

    PENDING  -> RUNNING | CANCELLED
    RUNNING  -> SUCCEEDED | FAILED | CANCELLED
    SUCCEEDED / FAILED / CANCELLED 为终态，不得再被改写。

取消与完成几乎同时发生时，由存储层的条件更新（乐观锁）保证
只有一个迁移生效，最终结论唯一且稳定。
"""
from __future__ import annotations

import logging

from .models import RejectReason, TaskState, TransitionError

logger = logging.getLogger("taskpipe.state_machine")

# 合法迁移表：当前状态 -> 允许的目标状态集合
_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.PENDING: frozenset({TaskState.RUNNING, TaskState.CANCELLED}),
    TaskState.RUNNING: frozenset(
        {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}
    ),
    TaskState.SUCCEEDED: frozenset(),
    TaskState.FAILED: frozenset(),
    TaskState.CANCELLED: frozenset(),
}

TERMINAL_STATES: frozenset[TaskState] = frozenset(
    state for state, targets in _TRANSITIONS.items() if not targets
)


def is_terminal(state: TaskState) -> bool:
    """判断状态是否为终态。"""
    return state in TERMINAL_STATES


def validate_transition(current: TaskState, target: TaskState) -> None:
    """校验 current -> target 是否合法。

    :raises TransitionError: 携带 ``TERMINAL_STATE`` 或 ``ILLEGAL_TRANSITION``
        原因，便于调用方区分"终态不可改写"与"非法迁移"。
    """
    if is_terminal(current):
        logger.info(
            "transition rejected: %s -> %s reason=%s (终态不可改写)",
            current.value, target.value, RejectReason.TERMINAL_STATE.value,
        )
        raise TransitionError(
            RejectReason.TERMINAL_STATE,
            f"task already in terminal state {current.value}, "
            f"cannot transition to {target.value}",
        )
    if target not in _TRANSITIONS[current]:
        logger.info(
            "transition rejected: %s -> %s reason=%s (非法迁移)",
            current.value, target.value, RejectReason.ILLEGAL_TRANSITION.value,
        )
        raise TransitionError(
            RejectReason.ILLEGAL_TRANSITION,
            f"illegal transition {current.value} -> {target.value}",
        )
