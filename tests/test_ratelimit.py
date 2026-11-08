from gateway.ratelimit import RateLimiter, TokenBucket


def test_bucket_allows_burst_then_limits():
    b = TokenBucket(rate=2, burst=4)
    now = 1000.0
    assert all(b.take(now=now)[0] for _ in range(4))     # burst of 4
    allowed, _, retry = b.take(now=now)
    assert not allowed and 0 < retry <= 0.5             # 1 token = 0.5 s


def test_bucket_refills_over_time():
    b = TokenBucket(rate=2, burst=4)
    for _ in range(4):
        b.take(now=0.0)
    assert not b.take(now=0.1)[0]
    assert b.take(now=0.6)[0]                           # 0.5 s -> 1 token


def test_limiter_is_per_key_and_uses_key_rate():
    lim = RateLimiter(default_rate=100)
    for _ in range(2):
        assert lim.take("slow", rate=1)[0]              # burst 2
    assert not lim.take("slow", rate=1)[0]
    assert lim.take("other")[0]                         # independent bucket
