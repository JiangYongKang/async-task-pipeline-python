"""可配置项：并发度、容量上限、存储路径等。"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Settings:
    """服务配置。"""

    max_concurrency: int = 4        # 同时执行的任务数上限
    max_pending: int = 16           # 排队（PENDING）任务数上限，超出即拒绝
    handler_timeout: float = 30.0   # 单个任务执行超时（秒）
    db_path: str = "taskpipe.db"    # SQLite 持久化路径
