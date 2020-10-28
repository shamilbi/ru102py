# Uncomment for Challenge #7
import time
#import datetime
#import random

from redisolar.dao.base import RateLimiterDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema
# Uncomment for Challenge #7
from redisolar.dao.base import RateLimitExceededException
from redis.client import Redis


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""
    def __init__(self,
                 window_size_ms: float,
                 max_hits: int,
                 redis_client: Redis,
                 key_schema: KeySchema = None,
                 **kwargs):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        # START Challenge #7
        ns = time.time_ns()
        score_ms: float = ns / 1_000_000
        p = self.redis.pipeline(transaction=True)
        key = self.key_schema.sliding_window_rate_limiter_key(
            name, self.window_size_ms, self.max_hits)
        p.zadd(key, {f'{name}-{ns}': score_ms})
        p.zremrangebyscore(key, '-inf', score_ms - self.window_size_ms)
        p.zcard(key)
        *_, count = p.execute()
        if count > self.max_hits:
            raise RateLimitExceededException(f'{count}, {score_ms}')
        # END Challenge #7
