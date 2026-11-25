"""Source health and circuit-breaker state owned by the data layer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import RLock


@dataclass(frozen=True)
class ProviderHealth:
    provider: str
    route: str | None
    status: str
    consecutive_failures: int
    success_count: int
    failure_count: int
    average_latency_ms: float | None
    last_error: str | None
    last_success_at: datetime | None
    circuit_open_until: datetime | None
    timeout_count: int = 0
    capacity_rejections: int = 0


@dataclass
class _Runtime:
    consecutive_failures: int = 0
    success_count: int = 0
    failure_count: int = 0
    total_latency_ms: float = 0.0
    last_error: str | None = None
    last_success_at: datetime | None = None
    circuit_open_until: datetime | None = None
    timeout_count: int = 0
    capacity_rejections: int = 0
    open_trips: int = 0
    probe_in_flight: bool = False


class SourceHealthRegistry:
    def __init__(
        self,
        failure_threshold: int = 2,
        cooldown: timedelta = timedelta(seconds=30),
        *,
        max_cooldown: timedelta = timedelta(minutes=10),
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be at least one")
        self.failure_threshold = failure_threshold
        self.cooldown = cooldown
        self.max_cooldown = max(max_cooldown, cooldown)
        self._providers: set[str] = set()
        self._runtimes: dict[tuple[str, str], _Runtime] = {}
        self._lock = RLock()

    def ensure(self, provider: str) -> None:
        with self._lock:
            self._providers.add(provider)

    def can_attempt(self, provider: str, route: str) -> bool:
        with self._lock:
            runtime = self._runtime(provider, route)
            if runtime.probe_in_flight:
                return False
            return runtime.circuit_open_until is None or _utc_now() >= runtime.circuit_open_until

    def try_acquire_attempt(self, provider: str, route: str) -> tuple[bool, bool]:
        """Return ``(allowed, is_half_open_probe)`` with single-probe gating."""
        with self._lock:
            runtime = self._runtime(provider, route)
            if runtime.circuit_open_until is None:
                return True, False
            if _utc_now() < runtime.circuit_open_until:
                return False, False
            if runtime.probe_in_flight:
                return False, False
            runtime.probe_in_flight = True
            return True, True

    def finish_probe(self, provider: str, route: str) -> None:
        with self._lock:
            runtime = self._runtime(provider, route)
            runtime.probe_in_flight = False

    def record_success(self, provider: str, route: str, latency_ms: float) -> None:
        with self._lock:
            runtime = self._runtime(provider, route)
            runtime.consecutive_failures = 0
            runtime.open_trips = 0
            runtime.probe_in_flight = False
            runtime.success_count += 1
            runtime.total_latency_ms += latency_ms
            runtime.last_error = None
            runtime.last_success_at = _utc_now()
            runtime.circuit_open_until = None

    def record_failure(self, provider: str, route: str, error: BaseException) -> None:
        with self._lock:
            runtime = self._runtime(provider, route)
            runtime.consecutive_failures += 1
            runtime.failure_count += 1
            runtime.last_error = _short_error(error)
            runtime.probe_in_flight = False
            if runtime.consecutive_failures >= self.failure_threshold:
                runtime.open_trips += 1
                backoff_factor = 1 << min(runtime.open_trips - 1, 6)
                effective_cooldown = min(self.cooldown * backoff_factor, self.max_cooldown)
                runtime.circuit_open_until = _utc_now() + effective_cooldown

    def record_timeout(self, provider: str, route: str) -> None:
        with self._lock:
            self._runtime(provider, route).timeout_count += 1

    def record_capacity_rejection(self, provider: str, route: str) -> None:
        with self._lock:
            runtime = self._runtime(provider, route)
            runtime.capacity_rejections += 1
            runtime.probe_in_flight = False

    def snapshots(self) -> tuple[ProviderHealth, ...]:
        with self._lock:
            now = _utc_now()
            values: list[ProviderHealth] = []
            for provider in self._providers:
                routes = [
                    (route, runtime)
                    for (runtime_provider, route), runtime in self._runtimes.items()
                    if runtime_provider == provider
                ]
                if not routes:
                    routes = [(None, _Runtime())]
                for route, runtime in routes:
                    if runtime.probe_in_flight:
                        status = "half_open"
                    elif runtime.circuit_open_until and now < runtime.circuit_open_until:
                        status = "open"
                    else:
                        status = "closed"
                    average = (
                        runtime.total_latency_ms / runtime.success_count
                        if runtime.success_count
                        else None
                    )
                    values.append(
                        ProviderHealth(
                            provider=provider,
                            route=route,
                            status=status,
                            consecutive_failures=runtime.consecutive_failures,
                            success_count=runtime.success_count,
                            failure_count=runtime.failure_count,
                            average_latency_ms=average,
                            last_error=runtime.last_error,
                            last_success_at=runtime.last_success_at,
                            circuit_open_until=runtime.circuit_open_until,
                            timeout_count=runtime.timeout_count,
                            capacity_rejections=runtime.capacity_rejections,
                        )
                    )
            return tuple(sorted(values, key=lambda item: (item.provider, item.route or "")))

    def _runtime(self, provider: str, route: str) -> _Runtime:
        self._providers.add(provider)
        return self._runtimes.setdefault((provider, route), _Runtime())


def _short_error(error: BaseException) -> str:
    message = str(error).strip().replace("\n", " ")
    return f"{type(error).__name__}: {message}"[:500]


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)
