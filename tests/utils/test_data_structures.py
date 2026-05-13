"""This module is responsible for testing the data structures used in the dataset factory."""

import time
from scripts.utils.data_structures import RateLimiter


class TestRateLimiter:
    def test_full_bucket_adds_to_queue(self):
        limiter = RateLimiter(rate=2, per=10, bucket_size=2)
        limiter.execute("task 1")
        limiter.execute("task 2")
        assert len(limiter.queue) == 0
        limiter.execute("task 3")
        assert len(limiter.queue) == 1
        assert limiter.queue[0][1] == "task 3"

    def test_bucket_refills_over_time(self):
        limiter = RateLimiter(rate=1, per=1, bucket_size=2)
        limiter.execute("task 1")
        limiter.execute("task 2")
        assert len(limiter.queue) == 0
        limiter.execute("task 3")
        assert len(limiter.queue) == 1
        time.sleep(1.1)
        limiter.execute("task 4")
        assert len(limiter.queue) == 1
        assert limiter.queue[0][1] == "task 4"