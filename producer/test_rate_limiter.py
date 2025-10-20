"""Unit tests for the token bucket rate limiter."""

import threading
import time

import pytest

from rate_limiter import RateLimiter


def test_acquire_consumes_tokens():
    limiter = RateLimiter(rate=1000, capacity=100)
    assert limiter.available_tokens == 100
    limiter.acquire(10)
    assert limiter.available_tokens == pytest.approx(90, abs=1)


def test_try_acquire_returns_false_when_empty():
    limiter = RateLimiter(rate=1, capacity=1)
    assert limiter.try_acquire() is True   # drains the bucket
    assert limiter.try_acquire() is False  # empty


def test_tokens_refill_over_time():
    limiter = RateLimiter(rate=1000, capacity=1000)
    limiter.acquire(1000)  # drain completely
    time.sleep(0.05)       # wait 50ms → ~50 tokens added
    assert limiter.available_tokens >= 40  # some tolerance for timing


def test_rate_limiter_enforces_throughput():
    """End-to-end: 200 acquires at 1000/s should take ~200ms."""
    limiter = RateLimiter(rate=1000, capacity=100)
    # Drain initial tokens
    for _ in range(100):
        limiter.try_acquire()

    start = time.monotonic()
    for _ in range(100):
        limiter.acquire()
    elapsed = time.monotonic() - start

    # Should take ~100ms (100 tokens at 1000/s), allow generous bounds for CI
    assert 0.05 < elapsed < 0.5


def test_thread_safety():
    limiter = RateLimiter(rate=10_000, capacity=10_000)
    results = []
    lock = threading.Lock()

    def worker():
        for _ in range(100):
            limiter.acquire()
            with lock:
                results.append(1)

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(results) == 500


def test_invalid_rate_raises():
    with pytest.raises(ValueError):
        RateLimiter(rate=0)
    with pytest.raises(ValueError):
        RateLimiter(rate=-1)
