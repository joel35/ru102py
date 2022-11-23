# Uncomment for Challenge #7
import datetime
import random
from redis.client import Redis

from redisolar.dao.base import RateLimiterDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema

# Uncomment for Challenge #7
from redisolar.dao.base import RateLimitExceededException


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""

    def __init__(
        self,
        window_size_ms: float,
        max_hits: int,
        redis_client: Redis,
        key_schema: KeySchema = None,
        **kwargs,
    ):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        # START Challenge #7

        key = self.key_schema.sliding_window_rate_limiter_key(
            name=name, window_size_ms=self.window_size_ms, max_hits=self.max_hits
        )

        ts = datetime.datetime.utcnow().timestamp() * 1000
        member = ts + random.random()
        max_score = ts - self.window_size_ms

        p = self.redis.pipeline(transaction=True)
        p.zadd(name=key, mapping={member: ts})
        p.zremrangebyscore(name=key, min=0, max=max_score)
        p.zcard(name=key)
        _, _, hits = p.execute()

        if hits > self.max_hits:
            raise RateLimitExceededException(f"Too many hits: {hits} > {self.max_hits}")

        # END Challenge #7
