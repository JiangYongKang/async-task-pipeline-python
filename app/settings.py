"""可配置项（均可通过环境变量覆盖，前缀 TASKPIPELINE_）。

环境变量
----------------  ---------------------------------------------
TASKPIPELINE_MAX_CONCURRENCY   最大并发执行线程数（默认 4）
TASKPIPELINE_MAX_CAPACITY      在制任务容量上限，含排队+执行中（默认 32）
TASKPIPELINE_DB_PATH           SQLite 数据库路径（默认 data/tasks.db）
TASKPIPELINE_DEFAULT_TIMEOUT   单个任务默认超时秒数；0 表示不限（默认不限）
TASKPIPELINE_POLL_INTERVAL     取消/完成协作轮询间隔秒（默认 0.05）
----------------  ---------------------------------------------

容量语义：状态为 QUEUED 或 RUNNING 的任务总数达到 max_capacity 时，
新提交立即被拒绝（CAPACITY_FULL），不会无限排队。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    max_concurrency: int = 4
    max_capacity: int = 32
    db_path: Path = Path("data/tasks.db")
    default_timeout_seconds: float | None = None
    poll_interval_seconds: float = 0.05

    def __post_init__(self) -> None:
        if self.max_concurrency <= 0:
            raise ValueError("max_concurrency must be > 0")
        if self.max_capacity < self.max_concurrency:
            raise ValueError(
                "max_capacity must be >= max_concurrency "
                f"(capacity={self.max_capacity}, concurrency={self.max_concurrency})"
            )
        if self.poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be > 0")


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _env_float(name: str, default: float | None) -> float | None:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


def load_settings() -> Settings:
    timeout = _env_float("TASKPIPELINE_DEFAULT_TIMEOUT", None)
    if timeout == 0.0:          # 0 显式表示不限时
        timeout = None
    return Settings(
        max_concurrency=_env_int("TASKPIPELINE_MAX_CONCURRENCY", 4),
        max_capacity=_env_int("TASKPIPELINE_MAX_CAPACITY", 32),
        db_path=Path(os.environ.get("TASKPIPELINE_DB_PATH", "data/tasks.db")),
        default_timeout_seconds=timeout,
        poll_interval_seconds=_env_float(
            "TASKPIPELINE_POLL_INTERVAL", 0.05  # type: ignore[arg-type]
        ),
    )
