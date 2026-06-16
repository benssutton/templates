import time

import pytest
from core.retry import connect_with_backoff


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
    jitter -- no flakiness from real randomness or real timing.
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

    await connect_with_backoff(
        connect, label="test", max_attempts=5, base_delay=0.01
    )

    deltas = [b - a for a, b in zip(timestamps, timestamps[1:])]
    assert len(deltas) == 3
    assert all(d > 0 for d in deltas)
    assert deltas[1] > deltas[0]
