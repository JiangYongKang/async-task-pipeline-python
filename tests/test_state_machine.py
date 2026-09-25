"""状态机单元测试：合法迁移、非法迁移、终态不可改写。"""
from __future__ import annotations

import pytest

from taskpipe.models import RejectReason, TaskState, TransitionError
from taskpipe.state_machine import is_terminal, validate_transition


@pytest.mark.parametrize(
    "current,target",
    [
        (TaskState.PENDING, TaskState.RUNNING),
        (TaskState.PENDING, TaskState.CANCELLED),
        (TaskState.RUNNING, TaskState.SUCCEEDED),
        (TaskState.RUNNING, TaskState.FAILED),
        (TaskState.RUNNING, TaskState.CANCELLED),
    ],
)
def test_legal_transitions(current, target):
    validate_transition(current, target)  # 不抛异常即合法


@pytest.mark.parametrize("terminal", [TaskState.SUCCEEDED, TaskState.FAILED,
                                      TaskState.CANCELLED])
def test_terminal_states_reject_any_transition(terminal, request_id):
    with pytest.raises(TransitionError) as exc:
        validate_transition(terminal, TaskState.RUNNING)
    assert exc.value.reason is RejectReason.TERMINAL_STATE
    assert is_terminal(terminal)


@pytest.mark.parametrize(
    "current,target",
    [
        (TaskState.PENDING, TaskState.SUCCEEDED),
        (TaskState.PENDING, TaskState.FAILED),
        (TaskState.RUNNING, TaskState.PENDING),
    ],
)
def test_illegal_transitions_are_distinguishable(current, target):
    with pytest.raises(TransitionError) as exc:
        validate_transition(current, target)
    assert exc.value.reason is RejectReason.ILLEGAL_TRANSITION
