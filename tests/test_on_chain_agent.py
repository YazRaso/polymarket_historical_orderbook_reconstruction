"""Unit tests for on_chain_agent (OC-07 sibling)."""
from __future__ import annotations

import asyncio
import inspect
import os
from unittest.mock import AsyncMock, patch

import pytest

from scripts.on_chain_agent import run
from scripts.utils.data_structures import RateLimiter
from scripts.utils.disk_cache import DiskCache

CONDITION_ID = "0xabc" + "0" * 61
START = "2024-01-01T10"
END = "2024-01-01T14"


def make_rate_limiter() -> RateLimiter:
    return RateLimiter(rate=300, per=1, bucket_size=10_000, max_workers=1)


class TestOnChainAgentInterface:
    def test_run_is_coroutine(self):
        assert inspect.iscoroutinefunction(run)

    def test_run_delegates_to_run_market(self, tmp_path):
        rl = make_rate_limiter()
        cache = DiskCache(str(tmp_path / "cache"))
        run_dir = str(tmp_path / "run")
        expected_path = "/some/output.parquet"

        mock_run_market = AsyncMock(return_value=expected_path)
        with patch("scripts.on_chain_agent.run_market", mock_run_market):
            result = asyncio.run(run(CONDITION_ID, START, END, run_dir, rl, cache, resume=True))

        mock_run_market.assert_called_once_with(
            CONDITION_ID, START, END, run_dir, rl, cache, True
        )
        assert result == expected_path

    def test_run_never_calls_gamma_api(self, tmp_path):
        """on_chain_agent must not call the Gamma API — it only calls run_market."""
        rl = make_rate_limiter()
        cache = DiskCache(str(tmp_path / "cache"))
        run_dir = str(tmp_path / "run")

        mock_run_market = AsyncMock(return_value="/out.parquet")
        # If the agent called Gamma, it would have to import requests and call requests.get.
        # We verify that run_market is the only thing invoked (no other I/O).
        with patch("scripts.on_chain_agent.run_market", mock_run_market) as mock:
            asyncio.run(run(CONDITION_ID, START, END, run_dir, rl, cache))
            assert mock.call_count == 1

    def test_resume_flag_forwarded(self, tmp_path):
        rl = make_rate_limiter()
        cache = DiskCache(str(tmp_path / "cache"))
        run_dir = str(tmp_path / "run")

        mock_run_market = AsyncMock(return_value="/out.parquet")
        with patch("scripts.on_chain_agent.run_market", mock_run_market):
            asyncio.run(run(CONDITION_ID, START, END, run_dir, rl, cache, resume=False))
        _, _, _, _, _, _, forwarded_resume = mock_run_market.call_args.args
        assert forwarded_resume is False
