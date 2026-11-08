"""Token-bucket rate limiting.

Every API key owns a bucket holding up to `burst` tokens, refilled at
`rate` tokens per second. Each request takes one token; with an empty
bucket the request is rejected with 429 and a Retry-After header. This
allows short bursts while enforcing the long-run average rate.

The buckets live in the memory of each gateway instance. With several
gateway replicas behind a load balancer each enforces the limit on its own
(a shared store such as Redis would be needed for one global limit).
"""

import threading
import time


class TokenBucket:
    def __init__(self, rate: float, burst: float):
        self.rate = float(rate)
        self.capacity = float(burst)
        self.tokens = float(burst)
        self.updated = time.monotonic()

    def take(self, cost: float = 1.0, now: float | None = None):
        """Returns (allowed, tokens_left, retry_after_seconds)."""
        now = time.monotonic() if now is None else now
        elapsed = max(0.0, now - self.updated)       # never negative
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.updated = now
        if self.tokens >= cost:
            self.tokens -= cost
            return True, self.tokens, 0.0
        return False, self.tokens, (cost - self.tokens) / self.rate


class RateLimiter:
    def __init__(self, default_rate: float, burst_factor: float = 2.0):
        self.default_rate = default_rate
        self.burst_factor = burst_factor
        self._buckets: dict[str, TokenBucket] = {}
        self._lock = threading.Lock()

    def take(self, key: str, rate: float | None = None):
        rate = rate or self.default_rate
        burst = max(1.0, rate * self.burst_factor)
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None or bucket.rate != rate:
                bucket = self._buckets[key] = TokenBucket(rate, burst)
            allowed, left, retry = bucket.take()
        return allowed, int(left), retry, int(burst)
