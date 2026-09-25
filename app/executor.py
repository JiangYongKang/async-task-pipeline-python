"""有界执行器。

* 并发度 = 常驻工作线程数 ``max_concurrency``；
* 待执行任务放在容量为 ``max_capacity`` 的有界队列中，队列满时
  ``submit`` 立即抛 :class:`CapacityFullError`，绝不无限排队；
* ``in_flight`` = 排队中 + 执行中，作为容量裁决的即时依据
  （持久层中的 QUEUED/RUNNING 计数是崩溃恢复后的依据，二者互补）。
"""

from __future__ import annotations

import logging
import queue
import threading
from concurrent.futures import Future
from typing import Callable

_SENTINEL = object()


class CapacityFullError(Exception):
    """执行器内部队列已满（服务层转译为 CAPACITY_FULL 拒绝）。"""


class BoundedExecutor:
    def __init__(self, max_concurrency: int, max_capacity: int) -> None:
        if max_concurrency <= 0:
            raise ValueError("max_concurrency must be > 0")
        if max_capacity < max_concurrency:
            raise ValueError("max_capacity must be >= max_concurrency")
        self._max_concurrency = max_concurrency
        # 可排队槽位 = 总容量 - 正在执行的槽位
        self._queue: queue.Queue[object] = queue.Queue(
            maxsize=max(1, max_capacity - max_concurrency + 1)
        )
        self._in_flight = 0
        self._lock = threading.Lock()
        self._workers: list[threading.Thread] = []
        self._started = False
        self._log = logging.getLogger("pipeline.executor")

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        for i in range(self._max_concurrency):
            t = threading.Thread(
                target=self._worker_loop,
                name=f"task-worker-{i}",
                daemon=True,
            )
            t.start()
            self._workers.append(t)

    def submit(self, fn: Callable[..., object], *args: object) -> Future:
        if not self._started:
            raise RuntimeError("executor not started")
        future: Future = Future()
        try:
            self._queue.put_nowait((fn, args, future))
        except queue.Full:
            future.cancel()
            raise CapacityFullError("execution queue is full") from None
        with self._lock:
            self._in_flight += 1
        return future

    def shutdown(self, wait: bool = True) -> None:
        if not self._started:
            return
        for _ in self._workers:
            self._queue.put(_SENTINEL)
        if wait:
            for t in self._workers:
                t.join(timeout=5)
        self._started = False

    @property
    def in_flight(self) -> int:
        with self._lock:
            return self._in_flight

    def _worker_loop(self) -> None:
        while True:
            item = self._queue.get()
            if item is _SENTINEL:
                self._queue.task_done()
                return
            fn, args, future = item  # type: ignore[misc]
            try:
                if future.set_running_or_notify_cancel():
                    result = fn(*args)
                    future.set_result(result)
            except BaseException as exc:  # noqa: BLE001 - 工作线程必须吞掉异常
                future.set_exception(exc)
                self._log.exception("worker task raised: %s", exc)
            finally:
                with self._lock:
                    self._in_flight -= 1
                self._queue.task_done()
