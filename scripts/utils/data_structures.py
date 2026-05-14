"""This module contains of data structures used for utility purposes across the dataset factory."""

from collections import deque
import time
import logging
import threading

logger = logging.getLogger(__name__)

class RateLimiter:
    """
    A semaphore with a token bucket rate limiter.
    Tasks that exceed the rate limit are queued and executed when tokens are available.
    """

    def __init__(self, rate: int, per: float, bucket_size: int, max_workers: int = 3) -> None:
        self.rate = rate
        self.per = per
        self.bucket_size = bucket_size
        self.tokens = bucket_size
        self.tasks = deque()
        self.semaphore = threading.Semaphore(max_workers)
        self.last_refill = time.monotonic()

    def refill_tokens(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        if elapsed >= self.per:
            # in our use case, the bucket is refilled to full capacity every time
            self.tokens = self.bucket_size
            self.last_refill = now

    def enough_tokens(self, tokens_needed: int) -> bool:
        self.refill_tokens()
        return self.tokens >= tokens_needed

    def execute(self, task: callable, *args, tokens_needed: int) -> None:
        while not self.enough_tokens(tokens_needed):
            time.sleep(1)
            logger.debug("Rate limit exceeded, waiting to execute task...")

        self.tokens -= tokens_needed

        with self.semaphore:
            task(*args)