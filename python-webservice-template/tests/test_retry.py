import time

import pytest
from core.retry import connect_with_backoff, connect_with_backoff_sync


async def test_succeeds_on_first_attempt():
    calls = []

    async def connect():
        calls.append(1)
        return "ok"

    result = await connect_with_backoff(connect, label="test", base_delay=0.001)
    assert result == "ok"
    assert len(calls) == 1


async def test_retries_then_succeeds():
    calls = []

    async def connect():
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("not yet")
        return "ok"

    result = await connect_with_backoff(
        connect, label="test", max_attempts=5, base_delay=0.001
    )
    assert result == "ok"
    assert len(calls) == 3


async def test_raises_after_max_attempts():
    async def connect():
        raise ConnectionError("always fails")

    with pytest.raises(ConnectionError, match="always fails"):
        await connect_with_backoff(
            connect, label="test", max_attempts=3, base_delay=0.001
        )


async def test_delays_are_positive_and_increasing():
    """Verify jitter produces positive, increasing delays.

    Uses the real asyncio.sleep and measures actual elapsed time between
    attempts instead of mocking it. This is safe because the backoff formula
    guarantees attempt 2's delay range ([2*base, 2.5*base)) never overlaps
    attempt 1's ([base, 1.25*base)), so "increasing" holds regardless of
    jitter. The base delay is 100 ms because sleeps overshoot by tens of ms on
    Windows (~15.6 ms timer ticks, more under load); with a 10 ms base that noise
    exceeded the gap between attempts and the comparison flaked.
    """
    timestamps: list[float] = []
    calls = 0

    async def connect():
        nonlocal calls
        timestamps.append(time.monotonic())
        calls += 1
        if calls < 4:
            raise ConnectionError("not yet")
        return "ok"

    await connect_with_backoff(connect, label="test", max_attempts=5, base_delay=0.1)

    deltas = [b - a for a, b in zip(timestamps, timestamps[1:])]
    assert len(deltas) == 3
    assert all(d > 0 for d in deltas)
    assert deltas[1] > deltas[0]


# ── Sync variant (used on the ingest thread to reconnect Solace) ────────────


def test_sync_retries_then_succeeds():
    calls = []

    def connect():
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("not yet")
        return "ok"

    waits: list[float] = []

    def wait(delay: float) -> bool:
        waits.append(delay)
        return False  # not cancelled

    result = connect_with_backoff_sync(
        connect, label="test", max_attempts=5, base_delay=0.01, wait=wait
    )
    assert result == "ok"
    assert len(calls) == 3
    # Same policy as the async version: base * 2^(n-1) plus up to 25% jitter.
    assert 0.01 <= waits[0] < 0.0125
    assert 0.02 <= waits[1] < 0.025


def test_sync_raises_after_max_attempts():
    def connect():
        raise ConnectionError("always fails")

    with pytest.raises(ConnectionError, match="always fails"):
        connect_with_backoff_sync(
            connect, label="test", max_attempts=3, base_delay=0.001
        )


def test_sync_cancelled_wait_reraises_the_last_error_without_retrying():
    calls = []

    def connect():
        calls.append(1)
        raise ConnectionError("down")

    with pytest.raises(ConnectionError, match="down"):
        connect_with_backoff_sync(
            connect, label="test", max_attempts=5, wait=lambda _delay: True
        )
    assert len(calls) == 1
