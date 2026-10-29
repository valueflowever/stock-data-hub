"""Bounded, isolated native calls that keep their slot until the work finishes."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import Future
from contextvars import ContextVar, copy_context
from math import isfinite
from threading import BoundedSemaphore, Event, Lock, Thread
from time import monotonic
from typing import TypeVar

T = TypeVar("T")
_CANCELLED: ContextVar[Event | None] = ContextVar("hub_worker_cancelled", default=None)
_DEADLINE: ContextVar[float | None] = ContextVar("hub_worker_deadline", default=None)


def remaining_timeout(default: float) -> float:
    cancelled = _CANCELLED.get()
    deadline = _DEADLINE.get()
    remaining = deadline - monotonic() if deadline is not None else default
    if (cancelled is not None and cancelled.is_set()) or remaining <= 0:
        raise TimeoutError("Native call budget exhausted")
    return min(default, remaining)


class WorkerBusy(RuntimeError):
    """Local admission failed; this is not evidence of an upstream outage."""


class BoundedWorkers:
    def __init__(self, capacity: int, *, name: str, max_pending: int | None = None) -> None:
        if capacity < 1 or (max_pending is not None and max_pending < 0):
            raise ValueError("Invalid worker limits")
        self.capacity = capacity
        self.name = name
        self._slots = BoundedSemaphore(capacity)
        self._admission = BoundedSemaphore(
            capacity + (capacity * 4 if max_pending is None else max_pending)
        )
        self._lock = Lock()
        self._active = 0

    @property
    def active(self) -> int:
        with self._lock:
            return self._active

    async def run(
        self,
        call: Callable[[], T],
        *,
        timeout: float | None = None,
        queue_timeout: float = 1.0,
    ) -> T:
        if timeout is not None and (not isfinite(timeout) or timeout <= 0):
            raise ValueError("timeout must be finite and positive")
        if not isfinite(queue_timeout) or queue_timeout < 0:
            raise ValueError("queue_timeout must be finite and nonnegative")
        deadline = monotonic() + timeout if timeout is not None else None
        if not self._admission.acquire(blocking=False):
            raise WorkerBusy(f"{self.name} request queue is full")
        try:
            queue_deadline = monotonic() + queue_timeout
            if deadline is not None:
                queue_deadline = min(queue_deadline, deadline)
            while not self._slots.acquire(blocking=False):
                if monotonic() >= queue_deadline:
                    raise WorkerBusy(f"{self.name} has no available worker slot")
                await asyncio.sleep(min(0.01, max(queue_deadline - monotonic(), 0)))
            native: Future[T] = Future()
            context = copy_context()
            cancelled = Event()
            with self._lock:
                self._active += 1

            def finished(_: Future[T]) -> None:
                with self._lock:
                    self._active -= 1
                self._slots.release()

            native.add_done_callback(finished)

            def invoke() -> T:
                _CANCELLED.set(cancelled)
                _DEADLINE.set(deadline)
                return call()

            def work() -> None:
                if not native.set_running_or_notify_cancel():
                    return
                try:
                    result = context.run(invoke)
                except BaseException as exc:  # noqa: BLE001 - transport worker failures to the waiter
                    native.set_exception(exc)
                else:
                    native.set_result(result)

            try:
                # A stuck third-party SDK must not block interpreter shutdown.
                # Threads are bounded and never share asyncio's default executor.
                Thread(target=work, name=self.name, daemon=True).start()
            except BaseException:
                native.cancel()
                raise
            wrapped = asyncio.wrap_future(native)
            wrapped.add_done_callback(_consume_exception)
            try:
                remaining = max(deadline - monotonic(), 0) if deadline is not None else None
                return await asyncio.wait_for(asyncio.shield(wrapped), timeout=remaining)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                # Only cancels work which has not started. The native future's
                # completion callback owns slot release even when the caller leaves.
                cancelled.set()
                native.cancel()
                raise
        finally:
            self._admission.release()


def _consume_exception(future: asyncio.Future) -> None:
    if not future.cancelled():
        future.exception()
