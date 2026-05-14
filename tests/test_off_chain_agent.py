"""Unit tests for off_chain_agent (OC-07 sibling)."""
from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from scripts.off_chain_agent import (
    _combine_hourly_outputs,
    _job_key,
    _load_checkpoint,
    _save_checkpoint,
    run,
)


class TestJobKey:
    def test_basic_format(self):
        assert _job_key("my-slug", "2024-01-01T00", "2024-01-02T00") == (
            "my-slug__2024-01-01T00__2024-01-02T00"
        )

    def test_slashes_replaced(self):
        key = _job_key("a/b", "2024-01-01T00", "2024-01-01T01")
        assert "/" not in key

    def test_different_slugs_produce_different_keys(self):
        assert _job_key("a", "s", "e") != _job_key("b", "s", "e")


class TestCheckpoint:
    def test_load_missing_returns_defaults(self, tmp_path):
        ckpt = _load_checkpoint(str(tmp_path / "nonexistent.json"))
        assert ckpt["completed_urls"] == {}
        assert ckpt["job_status"] == "running"

    def test_round_trip(self, tmp_path):
        path = str(tmp_path / "ckpt.json")
        data = {"completed_urls": {"url1": 5}, "job_status": "running"}
        _save_checkpoint(path, data)
        loaded = _load_checkpoint(path)
        assert loaded["completed_urls"] == {"url1": 5}

    def test_save_is_atomic(self, tmp_path):
        path = str(tmp_path / "ckpt.json")
        _save_checkpoint(path, {"completed_urls": {}, "job_status": "running"})
        assert os.path.exists(path)
        assert not os.path.exists(f"{path}.tmp")

    def test_load_sets_missing_defaults(self, tmp_path):
        path = str(tmp_path / "ckpt.json")
        with open(path, "w") as f:
            json.dump({}, f)
        ckpt = _load_checkpoint(path)
        assert "completed_urls" in ckpt
        assert "job_status" in ckpt


class TestCombineHourlyOutputs:
    def test_empty_list_returns_zero(self, tmp_path):
        import duckdb
        conn = duckdb.connect()
        result = _combine_hourly_outputs(conn, [], str(tmp_path / "out.parquet"))
        assert result == 0

    def test_does_not_create_file_for_empty_list(self, tmp_path):
        import duckdb
        conn = duckdb.connect()
        output_path = str(tmp_path / "out.parquet")
        _combine_hourly_outputs(conn, [], output_path)
        assert not os.path.exists(output_path)


class TestRunNeverCallsGammaApi:
    """The off-chain agent must never call the Gamma API."""

    def test_run_does_not_call_gamma_api(self, tmp_path):
        run_dir = str(tmp_path / "run")
        os.makedirs(run_dir)

        with (
            patch("scripts.off_chain_agent.dataset_factory.urls_for_range", return_value=[]),
            patch("scripts.off_chain_agent.dataset_factory._get_event_token_ids") as mock_gamma,
        ):
            run(
                token_ids=["tok1", "tok2"],
                slug="test-slug",
                start_date="2024-01-01T00",
                end_date="2024-01-01T01",
                run_dir=run_dir,
                resume=False,
            )
            mock_gamma.assert_not_called()

    def test_run_with_no_hours_writes_no_file(self, tmp_path):
        run_dir = str(tmp_path / "run")
        os.makedirs(run_dir)

        with patch("off_chain_agent.dataset_factory.urls_for_range", return_value=[]):
            output_path = run(
                token_ids=["tok1"],
                slug="empty-slug",
                start_date="2024-01-01T00",
                end_date="2024-01-01T00",
                run_dir=run_dir,
                resume=False,
            )

        assert not os.path.exists(output_path)

    def test_run_skips_completed_checkpoint(self, tmp_path):
        run_dir = str(tmp_path / "run")
        checkpoints_dir = os.path.join(run_dir, "checkpoints")
        os.makedirs(checkpoints_dir)

        slug = "some-slug"
        start = "2024-01-01T00"
        end = "2024-01-01T02"
        fake_url = "https://example.com/polymarket_orderbook_2024-01-01T01.parquet"

        hourly_dir = os.path.join(run_dir, "hourly", f"{slug}__{start}__{end}")
        os.makedirs(hourly_dir)
        hourly_file = os.path.join(hourly_dir, "2024-01-01T01.parquet")
        open(hourly_file, "w").close()

        job_key = f"{slug}__{start}__{end}"
        ckpt_path = os.path.join(checkpoints_dir, f"offchain_{job_key}.json")
        with open(ckpt_path, "w") as f:
            json.dump({"completed_urls": {fake_url: 10}, "job_status": "running"}, f)

        with (
            patch("scripts.off_chain_agent.dataset_factory.urls_for_range", return_value=[fake_url]),
            patch("scripts.off_chain_agent._process_hour") as mock_process,
            patch("scripts.off_chain_agent._combine_hourly_outputs", return_value=10),
        ):
            run(
                token_ids=["tok1"],
                slug=slug,
                start_date=start,
                end_date=end,
                run_dir=run_dir,
                resume=True,
            )
            mock_process.assert_not_called()
