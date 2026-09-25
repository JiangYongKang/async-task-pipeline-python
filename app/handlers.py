"""处理函数注册表与内置示例处理函数。

处理函数约定::

    def fn(payload: str, cancel_event: threading.Event) -> str: ...

* 返回值字符串即为任务结果（SUCCEEDED.result）；
* 抛出任意异常被归一化为 HANDLER_ERROR，异常类名进入受控 error_message，
  堆栈只写服务端日志，不返回给客户端；
* 长时间任务应周期性 ``cancel_event.is_set()`` 主动退出，
  退出方式由服务层的 CAS 裁决（已完成则完成，已取消则 CANCELLED）。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Callable

HandlerFn = Callable[[str, threading.Event], str]


@dataclass(frozen=True)
class HandlerEntry:
    fn: HandlerFn
    timeout_seconds: float | None = None


_HANDLERS: dict[str, HandlerEntry] = {}


def register_handler(
    name: str, fn: HandlerFn, *, timeout_seconds: float | None = None
) -> None:
    _HANDLERS[name] = HandlerEntry(fn=fn, timeout_seconds=timeout_seconds)


def get_handler(name: str) -> HandlerEntry | None:
    return _HANDLERS.get(name)


def handler_names() -> list[str]:
    return sorted(_HANDLERS)


def _echo(payload: str, cancel_event: threading.Event) -> str:
    """原样返回载荷。"""
    return payload


def _sleep(payload: str, cancel_event: threading.Event) -> str:
    """睡眠 payload 指定秒数，期间响应协作式取消。"""
    seconds = float(payload)
    if cancel_event.wait(seconds):
        raise RuntimeError("cancelled by client")
    return f"slept {seconds}s"


def _fail(payload: str, cancel_event: threading.Event) -> str:
    """总是抛出异常，用于演示 HANDLER_ERROR 归一化。"""
    raise ValueError(f"intentional failure: {payload[:64]}")


def register_builtin_handlers() -> None:
    register_handler("echo", _echo)
    register_handler("sleep", _sleep)
    register_handler("fail", _fail)
