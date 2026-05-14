"""Unit tests for run_market orchestrator (OC-06)."""
import asyncio
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from scripts.utils.data_structures import RateLimiter
from scripts.utils.disk_cache import DiskCache
from scripts.onchain import run_market, _generate_hour_boundaries


CONDITION_ID = "0xabc" + "0" * 61
START = "2024-01-01T10"
END = "2024-01-01T14"


def make_rate_limiter() -> RateLimiter:
    return RateLimiter(rate=300, per=1, bucket_size=10_000, max_workers=1)


class TestGenerateHourBoundaries:
    def test_boundaries_are_strictly_between_start_and_end(self):
        boundaries = _generate_hour_boundaries("2024-01-01T10", "2024-01-01T14")
        # T11, T12, T13 (not T10 or T14)
        assert len(boundaries) == 3

    def test_boundaries_spaced_3600_seconds_apart(self):
        boundaries = _generate_hour_boundaries("2024-01-01T00", "2024-01-01T05")
        for a, b in zip(boundaries, boundaries[1:]):
            assert b - a == 3600

    def test_adjacent_hours_produce_empty_list(self):
        assert _generate_hour_boundaries("2024-01-01T10", "2024-01-01T11") == []

    def test_same_start_and_end_produces_empty_list(self):
        assert _generate_hour_boundaries("2024-01-01T10", "2024-01-01T10") == []

    def test_boundary_count_matches_hour_span_minus_one(self):
        # 10h span → 9 boundaries (T11 through T19)
        boundaries = _generate_hour_boundaries("2024-01-01T10", "2024-01-01T20")
        assert len(boundaries) == 9


class TestRunMarket:
    def _patch_deps(self, tmp_path):
        """Return context managers that stub out all Alchemy calls."""
        return (
            patch("scripts.onchain.timestamp_to_block", return_value=1000),
            patch("scripts.onchain.fetch_events", return_value=([], [])),
            patch("scripts.onchain.block_to_timestamp", return_value=0),
        )

    def test_completed_checkpoint_returns_immediately(self, tmp_path):
        rl = make_rate_limiter()
        cache = DiskCache(str(tmp_path / "cache"))
        run_dir = str(tmp_path / "run")
        os.makedirs(os.path.join(run_dir, "checkpoints"), exist_ok=True)

        ckpt_path = os.path.join(run_dir, "checkpoints", f"{CONDITION_ID}.json")
        existing_path = "/some/existing/file.parquet"
        with open(ckpt_path, "w") as f:
            json.dump({"status": "completed", "output_path": existing_path, "timestamp": "t"}, f)

        with patch("scripts.onchain.timestamp_to_block") as mock_ts2b:
            result = asyncio.run(run_market(CONDITION_ID, START, END, run_dir, rl, cache, resume=True))

        mock_ts2b.assert_not_called()
        assert result == existing_path

    def test_completed_checkpoint_not_skipped_when_resume_false(self, tmp_path):
        rl = make_rate_limiter()
        cache = DiskCache(str(tmp_path / "cache"))
        run_dir = str(tmp_path / "run")
        os.makedirs(os.path.join(run_dir, "checkpoints"), exist_ok=True)

        ckpt_path = os.path.join(run_dir, "checkpoints", f"{CONDITION_ID}.json")
        with open(ckpt_path, "w") as f:
            json.dump({"status": "completed", "output_path": "/old.parquet", "timestamp": "t"}, f)

        p1 = patch("scripts.onchain.timestamp_to_block", return_value=1000)
        p2 = patch("scripts.onchain.fetch_events", return_value=([], []))
        p3 = patch("scripts.onchain.block_to_timestamp", return_value=0)

        with p1, p2, p3:
            result = asyncio.run(run_market(CONDITION_ID, START, END, run_dir, rl, cache, resume=False))

        assert result != "/old.parquet"
        assert os.path.exists(result)

    def test_output_parquet_has_exactly_two_columns(self, tmp_path):
        rl = make_rate_limiter()
        cache = DiskCache(str(tmp_path / "cache"))
        run_dir = str(tmp_path / "run")

        p1 = patch("scripts.onchain.timestamp_to_block", return_value=1000)
        p2 = patch("scripts.onchain.fetch_events", return_value=([], []))
        p3 = patch("scripts.onchain.block_to_timestamp", return_value=0)

        with p1, p2, p3:
            output_path = asyncio.run(
                run_market(CONDITION_ID, START, END, run_dir, rl, cache, resume=False)
            )

        df = pd.read_parquet(output_path)
        assert list(df.columns) == ["hour", "oi_usdc"]

    def test_parquet_row_count_matches_hour_boundaries(self, tmp_path):
        rl = make_rate_limiter()
        cache = DiskCache(str(tmp_path / "cache"))
        run_dir = str(tmp_path / "run")

        expected_boundaries = _generate_hour_boundaries(START, END)

        p1 = patch("scripts.onchain.timestamp_to_block", return_value=1000)
        p2 = patch("scripts.onchain.fetch_events", return_value=([], []))
        p3 = patch("scripts.onchain.block_to_timestamp", return_value=0)

        with p1, p2, p3:
            output_path = asyncio.run(
                run_market(CONDITION_ID, START, END, run_dir, rl, cache, resume=False)
            )

        df = pd.read_parquet(output_path)
        assert len(df) == len(expected_boundaries)

    def test_checkpoint_written_after_parquet(self, tmp_path):
        rl = make_rate_limiter()
        cache = DiskCache(str(tmp_path / "cache"))
        run_dir = str(tmp_path / "run")

        write_order = []

        real_to_parquet = pd.DataFrame.to_parquet

        def spy_to_parquet(self, path, **kwargs):
            write_order.append("parquet")
            real_to_parquet(self, path, **kwargs)

        real_save_checkpoint = __import__(
            "scripts.utils.checkpoint", fromlist=["save_checkpoint"]
        ).save_checkpoint

        def spy_save_checkpoint(path, data):
            write_order.append("checkpoint")
            real_save_checkpoint(path, data)

        p1 = patch("scripts.onchain.timestamp_to_block", return_value=1000)
        p2 = patch("scripts.onchain.fetch_events", return_value=([], []))
        p3 = patch("scripts.onchain.block_to_timestamp", return_value=0)
        p4 = patch("pandas.DataFrame.to_parquet", spy_to_parquet)
        p5 = patch("scripts.onchain.save_checkpoint", spy_save_checkpoint)

        with p1, p2, p3, p4, p5:
            asyncio.run(run_market(CONDITION_ID, START, END, run_dir, rl, cache, resume=False))

        assert write_order == ["parquet", "checkpoint"]

    def test_checkpoint_not_written_if_parquet_fails(self, tmp_path):
        rl = make_rate_limiter()
        cache = DiskCache(str(tmp_path / "cache"))
        run_dir = str(tmp_path / "run")

        p1 = patch("scripts.onchain.timestamp_to_block", return_value=1000)
        p2 = patch("scripts.onchain.fetch_events", return_value=([], []))
        p3 = patch("scripts.onchain.block_to_timestamp", return_value=0)
        p4 = patch("pandas.DataFrame.to_parquet", side_effect=IOError("disk full"))

        with p1, p2, p3, p4:
            with patch("scripts.onchain.save_checkpoint") as mock_save:
                with pytest.raises(IOError):
                    asyncio.run(
                        run_market(CONDITION_ID, START, END, run_dir, rl, cache, resume=False)
                    )
                mock_save.assert_not_called()

    def test_checkpoint_not_written_if_fetch_events_fails(self, tmp_path):
        rl = make_rate_limiter()
        cache = DiskCache(str(tmp_path / "cache"))
        run_dir = str(tmp_path / "run")

        p1 = patch("scripts.onchain.timestamp_to_block", return_value=1000)
        p2 = patch("scripts.onchain.fetch_events", side_effect=RuntimeError("alchemy down"))
        p3 = patch("scripts.onchain.block_to_timestamp", return_value=0)

        with p1, p2, p3:
            with patch("scripts.onchain.save_checkpoint") as mock_save:
                with pytest.raises(RuntimeError):
                    asyncio.run(
                        run_market(CONDITION_ID, START, END, run_dir, rl, cache, resume=False)
                    )
                mock_save.assert_not_called()

    def test_run_market_is_coroutine(self):
        import inspect
        assert inspect.iscoroutinefunction(run_market)

    def test_output_parquet_filename_contains_condition_and_dates(self, tmp_path):
        rl = make_rate_limiter()
        cache = DiskCache(str(tmp_path / "cache"))
        run_dir = str(tmp_path / "run")

        p1 = patch("scripts.onchain.timestamp_to_block", return_value=1000)
        p2 = patch("scripts.onchain.fetch_events", return_value=([], []))
        p3 = patch("scripts.onchain.block_to_timestamp", return_value=0)

        with p1, p2, p3:
            output_path = asyncio.run(
                run_market(CONDITION_ID, START, END, run_dir, rl, cache, resume=False)
            )

        filename = os.path.basename(output_path)
        assert CONDITION_ID in filename
        assert START in filename
        assert END in filename
        assert filename.endswith("_onchain.parquet")
