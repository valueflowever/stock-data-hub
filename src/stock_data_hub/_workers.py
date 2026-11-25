"""Bounded, isolated native calls that keep their slot until the work finishes."""

from __future__ import annotations

import asyncio
import math
import sys
from collections.abc import Callable, Iterator
from concurrent.futures import Future
from contextlib import contextmanager, suppress
from contextvars import ContextVar, copy_context
from math import isfinite
from queue import Empty, SimpleQueue
from threading import BoundedSemaphore, Event, Lock, Thread, local
from time import monotonic
from typing import Any, TypeVar

T = TypeVar("T")
_CANCELLED: ContextVar[Event | None] = ContextVar("hub_worker_cancelled", default=None)
_DEADLINE: ContextVar[float | None] = ContextVar("hub_worker_deadline", default=None)

_STDOUT_LOCK = Lock()
_STDOUT_STATE = local()
_STDOUT_REFCOUNT = 0
_STDOUT_ORIGINAL: Any = None
_STDOUT_PROXY: _ThreadLocalStdoutProxy | None = None


class _ThreadLocalStdoutProxy:
    """Suppress stdout only for threads inside ``suppress_thread_stdout``."""

    def __init__(self, target: Any) -> None:
        self._target = target

    def write(self, text: str) -> int:
        if getattr(_STDOUT_STATE, "depth", 0) > 0:
            return len(text)
        return self._target.write(text)

    def writelines(self, lines: Any) -> None:
        if getattr(_STDOUT_STATE, "depth", 0) > 0:
            return
        self._target.writelines(lines)

    def flush(self) -> None:
        if getattr(_STDOUT_STATE, "depth", 0) > 0:
            return
        self._target.flush()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._target, name)


@contextmanager
def suppress_thread_stdout() -> Iterator[None]:
    """Thread-safe replacement for ``contextlib.redirect_stdout``.

    Concurrent provider threads can safely silence noisy third-party SDK
    constructors without racing on or permanently corrupting ``sys.stdout``.
    """
    global _STDOUT_REFCOUNT, _STDOUT_ORIGINAL, _STDOUT_PROXY
    with _STDOUT_LOCK:
        if _STDOUT_REFCOUNT == 0:
            _STDOUT_ORIGINAL = sys.stdout
            _STDOUT_PROXY = _ThreadLocalStdoutProxy(_STDOUT_ORIGINAL)
            sys.stdout = _STDOUT_PROXY
        _STDOUT_REFCOUNT += 1
    _STDOUT_STATE.depth = getattr(_STDOUT_STATE, "depth", 0) + 1
    try:
        yield
    finally:
        _STDOUT_STATE.depth = max(getattr(_STDOUT_STATE, "depth", 1) - 1, 0)
        with _STDOUT_LOCK:
            _STDOUT_REFCOUNT -= 1
            if _STDOUT_REFCOUNT == 0:
                if sys.stdout is _STDOUT_PROXY:
                    sys.stdout = _STDOUT_ORIGINAL
                _STDOUT_ORIGINAL = None
                _STDOUT_PROXY = None


def remaining_timeout(default: float) -> float:
    cancelled = _CANCELLED.get()
    deadline = _DEADLINE.get()
    remaining = deadline - monotonic() if deadline is not None else default
    if (cancelled is not None and cancelled.is_set()) or remaining <= 0:
        raise TimeoutError("Native call budget exhausted")
    return min(default, remaining)


class WorkerBusy(RuntimeError):
    """Local admission failed; this is not evidence of an upstream outage."""


class RateLimiter:
    """Thread-safe token-bucket rate limiter with zero-wait initial burst."""

    def __init__(self, rate_per_second: float, *, burst: int | None = None) -> None:
        if not isfinite(rate_per_second) or rate_per_second <= 0:
            raise ValueError("rate_per_second must be finite and positive")
        resolved_burst = max(1, math.ceil(rate_per_second)) if burst is None else burst
        if resolved_burst < 1:
            raise ValueError("burst must be at least one")
        self.rate_per_second = float(rate_per_second)
        self.burst = float(resolved_burst)
        self._tokens = float(resolved_burst)
        self._updated_at = monotonic()
        self._lock = Lock()

    def reserve(self, max_wait: float | None = None) -> float | None:
        """Reserve one token; return wait seconds, or None if max_wait would be exceeded."""
        with self._lock:
            now = monotonic()
            elapsed = max(now - self._updated_at, 0.0)
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate_per_second)
            self._updated_at = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return 0.0
            wait_seconds = (1.0 - self._tokens) / self.rate_per_second
            if max_wait is not None and wait_seconds > max_wait:
                return None
            self._tokens -= 1.0
            return wait_seconds


class BoundedWorkers:
    def __init__(
        self,
        capacity: int,
        *,
        name: str,
        max_pending: int | None = None,
        max_rps: float | None = None,
        idle_thread_ttl: float = 30.0,
    ) -> None:
        if capacity < 1 or (max_pending is not None and max_pending < 0):
            raise ValueError("Invalid worker limits")
        if max_rps is not None and (not isfinite(max_rps) or max_rps <= 0):
            raise ValueError("max_rps must be finite and positive")
        self.capacity = capacity
        self.name = name
        self._slots = BoundedSemaphore(capacity)
        self._admission = BoundedSemaphore(
            capacity + (capacity * 4 if max_pending is None else max_pending)
        )
        self._rate_limiter = (
            RateLimiter(max_rps, burst=max(capacity, math.ceil(max_rps)))
            if max_rps is not None
            else None
        )
        self._idle_ttl = max(idle_thread_ttl, 0.05)
        self._work_queue: SimpleQueue[Callable[[], None]] = SimpleQueue()
        self._lock = Lock()
        self._active = 0
        self._live_threads = 0
        self._busy_threads = 0

    @property
    def active(self) -> int:
        with self._lock:
            return self._active

    def _worker_loop(self) -> None:
        while True:
            try:
                job = self._work_queue.get(timeout=self._idle_ttl)
            except Empty:
                with self._lock:
                    if self._work_queue.empty():
                        self._live_threads -= 1
                        return
                    continue
            with self._lock:
                self._busy_threads += 1
            try:
                job()
            finally:
                with self._lock:
                    self._busy_threads -= 1

    def _submit_work(self, job: Callable[[], None]) -> None:
        with self._lock:
            self._work_queue.put(job)
            needed = self._busy_threads + self._work_queue.qsize()
            if self._live_threads < needed:
                self._live_threads += 1
                try:
                    Thread(target=self._worker_loop, name=self.name, daemon=True).start()
                except BaseException:
                    self._live_threads -= 1
                    with suppress(Empty):
                        self._work_queue.get_nowait()
                    raise

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
        slot_acquired = False
        submitted = False
        try:
            queue_deadline = monotonic() + queue_timeout
            if deadline is not None:
                queue_deadline = min(queue_deadline, deadline)
            while not self._slots.acquire(blocking=False):
                if monotonic() >= queue_deadline:
                    raise WorkerBusy(f"{self.name} has no available worker slot")
                await asyncio.sleep(min(0.01, max(queue_deadline - monotonic(), 0)))
            slot_acquired = True

            if self._rate_limiter is not None:
                max_rate_wait = max(deadline - monotonic(), 0.0) if deadline is not None else None
                wait_seconds = self._rate_limiter.reserve(max_wait=max_rate_wait)
                if wait_seconds is None:
                    raise WorkerBusy(f"{self.name} rate limit budget exhausted")
                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)

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
                # Threads are bounded, reused across calls, and never share asyncio's default executor.
                self._submit_work(work)
                submitted = True
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
            if slot_acquired and not submitted:
                self._slots.release()
            self._admission.release()


def _consume_exception(future: asyncio.Future) -> None:
    if not future.cancelled():
        future.exception()
