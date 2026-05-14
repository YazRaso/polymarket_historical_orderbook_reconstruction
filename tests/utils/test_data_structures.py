"""This module is responsible for testing the data structures used in the dataset factory."""

import time
from scripts.utils.data_structures import RateLimiter


class TestRateLimiter:
    def test_full_bucket_consumes_tokens(self):
        limiter = RateLimiter(rate=2, per=10, bucket_size=2)
        calls = []

        def task(label):
            calls.append(label)

        limiter.execute(task, "task 1", tokens_needed=1)
        limiter.execute(task, "task 2", tokens_needed=1)

        assert calls == ["task 1", "task 2"]
        assert limiter.tokens == 0

    def test_bucket_refills_over_time(self):
        limiter = RateLimiter(rate=1, per=1, bucket_size=2)
        calls = []

        def task(label):
            calls.append(label)

        limiter.execute(task, "task 1", tokens_needed=1)
        limiter.execute(task, "task 2", tokens_needed=1)

        assert limiter.tokens == 0

        time.sleep(1.1)
        limiter.execute(task, "task 3", tokens_needed=1)

        assert calls == ["task 1", "task 2", "task 3"]
        assert limiter.tokens == 1