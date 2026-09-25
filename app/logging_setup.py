"""统一日志。

每行日志包含：时间、级别、请求标识 request_id、logger 名、消息。
请求标识通过 contextvar 绑定（HTTP 中间件/单测均可设置），
状态迁移与判定依据由调用方写进消息正文，便于复盘“为什么这么判”。
"""

from __future__ import annotations

import logging
from contextvars import ContextVar
from contextlib import contextmanager
from typing import Iterator

_request_id_var: ContextVar[str] = ContextVar("request_id", default="-")

_LOG_FORMAT = "%(asctime)s %(levelname)-7s [req=%(request_id)s] %(name)s: %(message)s"


class _RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = _request_id_var.get()
        return True


_configured = False


def configure_logging(level: int = logging.INFO) -> None:
    global _configured
    if _configured:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    handler.addFilter(_RequestIdFilter())
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(level)
    _configured = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def bind_request_id(request_id: str | None) -> str:
    """把 request_id 绑定到当前执行上下文，返回设置后的 token 无关值。"""
    _request_id_var.set(request_id or "-")
    return request_id or "-"


@contextmanager
def request_context(request_id: str | None) -> Iterator[None]:
    token = _request_id_var.set(request_id or "-")
    try:
        yield
    finally:
        _request_id_var.reset(token)
