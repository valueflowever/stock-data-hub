from __future__ import annotations

import asyncio
from threading import Event

import pytest

from stock_data_hub._workers import BoundedWorkers, WorkerBusy, remaining_timeout


async def _wait_for(event: Event) -> None:
    async def poll() -> None:
        while not event.is_set():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(poll(), timeout=2)


def test_cancelled_native_work_keeps_its_slot_until_completion() -> None:
    workers = BoundedWorkers(1, name="test-worker", max_pending=0)
    started, release = Event(), Event()
    unexpected = Event()

    def blocking() -> None:
        started.set()
        assert release.wait(3)

    async def run() -> None:
        first = asyncio.create_task(workers.run(blocking))
        try:
            await _wait_for(started)
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
            with pytest.raises(WorkerBusy):
                await workers.run(unexpected.set, queue_timeout=0)
            assert not unexpected.is_set()
            release.set()
            # The next call can run only after the first native function exits.
            assert await workers.run(lambda: "recovered", queue_timeout=1) == "recovered"
        finally:
            release.set()
            await asyncio.gather(first, return_exceptions=True)

    asyncio.run(run())


def test_queue_wait_consumes_the_same_native_deadline() -> None:
    workers = BoundedWorkers(1, name="test-deadline")
    started, release = Event(), Event()

    def blocking() -> None:
        started.set()
        assert release.wait(3)

    async def run() -> None:
        first = asyncio.create_task(workers.run(blocking))
        try:
            await _wait_for(started)
            asyncio.get_running_loop().call_later(0.1, release.set)
            budget = await workers.run(lambda: remaining_timeout(1), timeout=0.3, queue_timeout=0.3)
            assert 0 < budget < 0.25
        finally:
            release.set()
            await first

    asyncio.run(run())
