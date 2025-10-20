"""
Token bucket rate limiter.

Backfill jobs can process months of data and saturate Kafka or the network.
A token bucket limits throughput: tokens accumulate at `rate` per second up
to `capacity`, and each published message consumes one token. When the bucket
is empty the caller sleeps until enough tokens are available.

This implementation is thread-safe via a lock, so it can be shared across
producer threads if needed.

Usage:
    limiter = RateLimiter(rate=5000, capacity=10000)  # 5k msg/s, burst of 10k
    for event in events:
        limiter.acquire()
        producer.send(topic, value=event)
"""

import threading
import time


class RateLimiter:
    """
    Token bucket rate limiter.

    Args:
        rate:     Tokens (messages) added per second.
        capacity: Maximum burst size (bucket ceiling).
    """

    def __init__(self, rate: float, capacity: float | None = None):
        if rate <= 0:
            raise ValueError(f"rate must be positive, got {rate}")
        self._rate = rate
        self._capacity = capacity if capacity is not None else rate
        self._tokens = self._capacity
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        """Add tokens proportional to elapsed time since last refill."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        added = elapsed * self._rate
        self._tokens = min(self._capacity, self._tokens + added)
        self._last_refill = now

    def acquire(self, n: int = 1) -> None:
        """
        Block until n tokens are available, then consume them.
        Typically called with n=1 (one message per call).
        """
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= n:
                    self._tokens -= n
                    return
                # Calculate how long to sleep before enough tokens accumulate
                deficit = n - self._tokens
                sleep_time = deficit / self._rate

            time.sleep(sleep_time)

    def try_acquire(self, n: int = 1) -> bool:
        """Non-blocking acquire. Returns True if tokens were available."""
        with self._lock:
            self._refill()
            if self._tokens >= n:
                self._tokens -= n
                return True
            return False

    @property
    def rate(self) -> float:
        return self._rate

    @property
    def available_tokens(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens
