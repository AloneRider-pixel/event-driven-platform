"""Redis-backed sliding window rate limiter."""
import time


class RateLimiter:
    """Sliding window rate limiter using Redis sorted sets."""

    def __init__(self, redis_client, max_requests: int = 100, window_seconds: int = 60):
        self.redis = redis_client
        self.max_requests = max_requests
        self.window_seconds = window_seconds

    async def check(self, user_id: str) -> bool:
        """Check if request is allowed under rate limit."""
        key = f"ratelimit:{user_id}"
        now = time.time()
        window_start = now - self.window_seconds

        pipe = self.redis.pipeline()
        pipe.zremrangebyscore(key, 0, window_start)
        pipe.zadd(key, {str(now): now})
        pipe.zcard(key)
        pipe.expire(key, self.window_seconds)

        results = await pipe.execute()
        request_count = results[2]

        return request_count <= self.max_requests

    async def get_remaining(self, user_id: str) -> int:
        """Get remaining requests in current window."""
        key = f"ratelimit:{user_id}"
        now = time.time()
        window_start = now - self.window_seconds

        await self.redis.zremrangebyscore(key, 0, window_start)
        count = await self.redis.zcard(key)
        return max(0, self.max_requests - count)
