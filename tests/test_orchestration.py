"""Unit tests for create_datasets_from_csv parent orchestrator (OC-07)."""
from __future__ import annotations

import asyncio
import csv
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scripts.create_datasets_from_csv import (
    _clean_field,
    _load_manifest,
    _read_jobs,
    _resolve_market_data,
    _run_async,
    _save_manifest,
    _slug_done,
)


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

class TestReadJobs:
    def test_returns_rows(self, tmp_path):
        path = tmp_path / "jobs.csv"
        path.write_text("slug,start_date,end_date\nfoo,2024-01-01T00,2024-01-02T00\n")
        jobs = _read_jobs(str(path))
        assert len(jobs) == 1
        assert jobs[0]["slug"] == "foo"

    def test_missing_column_raises(self, tmp_path):
        path = tmp_path / "bad.csv"
        path.write_text("slug,start_date\nfoo,2024-01-01T00\n")
        with pytest.raises(ValueError, match="CSV must include columns"):
            _read_jobs(str(path))


class TestCleanField:
    def test_strips_whitespace(self):
        assert _clean_field("  hello  ") == "hello"

    def test_strips_quotes(self):
        assert _clean_field('"foo"') == "foo"
        assert _clean_field("'bar'") == "bar"


# ---------------------------------------------------------------------------
# Gamma API resolution
# ---------------------------------------------------------------------------

class TestResolveMarketData:
    def _make_gamma_response(self, markets):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {"markets": markets}
        return mock_resp

    def test_extracts_flat_token_ids_across_markets(self):
        markets = [
            {"clobTokenIds": '["tok1", "tok2"]', "conditionId": "0xaaa"},
            {"clobTokenIds": '["tok3"]', "conditionId": "0xbbb"},
        ]
        with patch("scripts.create_datasets_from_csv.requests.get",
                   return_value=self._make_gamma_response(markets)):
            result = _resolve_market_data("some-slug")

        assert set(result["token_ids"]) == {"tok1", "tok2", "tok3"}

    def test_extracts_condition_id_per_market(self):
        markets = [
            {"clobTokenIds": '["tok1"]', "conditionId": "0xaaa"},
            {"clobTokenIds": '["tok2"]', "conditionId": "0xbbb"},
        ]
        with patch("scripts.create_datasets_from_csv.requests.get",
                   return_value=self._make_gamma_response(markets)):
            result = _resolve_market_data("some-slug")

        condition_ids = [m["condition_id"] for m in result["markets"]]
        assert condition_ids == ["0xaaa", "0xbbb"]

    def test_single_market_produces_one_market_entry(self):
        markets = [{"clobTokenIds": '["tok1"]', "conditionId": "0xccc"}]
        with patch("scripts.create_datasets_from_csv.requests.get",
                   return_value=self._make_gamma_response(markets)):
            result = _resolve_market_data("single")

        assert len(result["markets"]) == 1
        assert result["markets"][0]["condition_id"] == "0xccc"

    def test_empty_markets_raises(self):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {"markets": []}
        with patch("scripts.create_datasets_from_csv.requests.get", return_value=mock_resp):
            with pytest.raises(ValueError, match="No markets found"):
                _resolve_market_data("empty-slug")

    def test_api_failure_raises(self):
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 404
        with patch("scripts.create_datasets_from_csv.requests.get", return_value=mock_resp):
            with pytest.raises(ValueError, match="Gamma API failed"):
                _resolve_market_data("bad-slug")

    def test_gamma_api_called_exactly_once(self):
        markets = [{"clobTokenIds": '["tok1"]', "conditionId": "0xaaa"}]
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {"markets": markets}
        with patch("scripts.create_datasets_from_csv.requests.get",
                   return_value=mock_resp) as mock_get:
            _resolve_market_data("once-slug")
        assert mock_get.call_count == 1


# ---------------------------------------------------------------------------
# Run manifest
# ---------------------------------------------------------------------------

class TestManifest:
    def test_load_missing_returns_empty(self, tmp_path):
        run_dir = str(tmp_path / "run")
        os.makedirs(run_dir)
        manifest = _load_manifest(run_dir)
        assert manifest == {"slugs": {}}

    def test_round_trip(self, tmp_path):
        run_dir = str(tmp_path / "run")
        os.makedirs(run_dir)
        manifest = {"slugs": {"foo": {"status": "completed"}}}
        _save_manifest(run_dir, manifest)
        loaded = _load_manifest(run_dir)
        assert loaded["slugs"]["foo"]["status"] == "completed"

    def test_save_is_atomic(self, tmp_path):
        run_dir = str(tmp_path / "run")
        os.makedirs(run_dir)
        _save_manifest(run_dir, {"slugs": {}})
        manifest_path = os.path.join(run_dir, "manifest.json")
        assert os.path.exists(manifest_path)
        assert not os.path.exists(f"{manifest_path}.tmp")


class TestSlugDone:
    def test_completed_slug_is_done(self):
        manifest = {"slugs": {"foo": {"status": "completed"}}}
        assert _slug_done(manifest, "foo") is True

    def test_missing_slug_is_not_done(self):
        manifest = {"slugs": {}}
        assert _slug_done(manifest, "bar") is False

    def test_non_completed_status_is_not_done(self):
        manifest = {"slugs": {"foo": {"status": "running"}}}
        assert _slug_done(manifest, "foo") is False


# ---------------------------------------------------------------------------
# Full pipeline orchestration
# ---------------------------------------------------------------------------

MARKET_DATA = {
    "token_ids": ["tok1", "tok2"],
    "markets": [
        {"condition_id": "0xccc" + "0" * 61, "token_ids": ["tok1", "tok2"]},
    ],
}


def _make_csv(tmp_path, rows):
    path = tmp_path / "jobs.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["slug", "start_date", "end_date"])
        writer.writeheader()
        writer.writerows(rows)
    return str(path)


class TestRunAsyncOrchestration:
    def _patch_siblings(self):
        return (
            patch("scripts.create_datasets_from_csv.off_chain_agent.run", return_value="/off.parquet"),
            patch("scripts.create_datasets_from_csv.on_chain_agent.run", new_callable=lambda: lambda: AsyncMock(return_value="/on.parquet")),
        )

    def test_gamma_api_called_once_per_slug_not_per_sibling(self, tmp_path):
        csv_path = _make_csv(tmp_path, [
            {"slug": "slug-a", "start_date": "2024-01-01T00", "end_date": "2024-01-01T02"},
        ])
        mock_resolve = MagicMock(return_value=MARKET_DATA)
        with (
            patch("scripts.create_datasets_from_csv._resolve_market_data", mock_resolve),
            patch("scripts.create_datasets_from_csv.off_chain_agent.run", return_value="/off.parquet"),
            patch("scripts.create_datasets_from_csv.on_chain_agent.run", AsyncMock(return_value="/on.parquet")),
        ):
            asyncio.run(_run_async(str(csv_path), str(tmp_path / "data"), None, False, 500))

        assert mock_resolve.call_count == 1

    def test_completed_slug_is_skipped_on_resume(self, tmp_path):
        csv_path = _make_csv(tmp_path, [
            {"slug": "done-slug", "start_date": "2024-01-01T00", "end_date": "2024-01-01T02"},
        ])
        run_id = "testrun123"
        run_dir = os.path.join(str(tmp_path / "data"), f"run{run_id}")
        os.makedirs(os.path.join(run_dir, "checkpoints"), exist_ok=True)

        manifest = {"slugs": {"done-slug": {"status": "completed", "off_chain_output": "/x.parquet", "on_chain_outputs": []}}}
        _save_manifest(run_dir, manifest)

        mock_resolve = MagicMock(return_value=MARKET_DATA)
        with patch("scripts.create_datasets_from_csv._resolve_market_data", mock_resolve):
            asyncio.run(_run_async(str(csv_path), str(tmp_path / "data"), run_id, True, 500))

        mock_resolve.assert_not_called()

    def test_failure_in_sibling_does_not_mark_slug_done(self, tmp_path):
        csv_path = _make_csv(tmp_path, [
            {"slug": "fail-slug", "start_date": "2024-01-01T00", "end_date": "2024-01-01T02"},
        ])
        run_id = "failrun"
        data_dir = str(tmp_path / "data")

        mock_resolve = MagicMock(return_value=MARKET_DATA)
        with (
            patch("scripts.create_datasets_from_csv._resolve_market_data", mock_resolve),
            patch("scripts.create_datasets_from_csv.off_chain_agent.run", side_effect=RuntimeError("disk full")),
            patch("scripts.create_datasets_from_csv.on_chain_agent.run", AsyncMock(return_value="/on.parquet")),
        ):
            asyncio.run(_run_async(str(csv_path), data_dir, run_id, False, 500))

        run_dir = os.path.join(data_dir, f"run{run_id}")
        manifest = _load_manifest(run_dir)
        assert "fail-slug" not in manifest["slugs"]

    def test_multiple_slugs_each_call_gamma_once(self, tmp_path):
        csv_path = _make_csv(tmp_path, [
            {"slug": "slug-a", "start_date": "2024-01-01T00", "end_date": "2024-01-01T01"},
            {"slug": "slug-b", "start_date": "2024-01-01T00", "end_date": "2024-01-01T01"},
        ])
        mock_resolve = MagicMock(return_value=MARKET_DATA)
        with (
            patch("scripts.create_datasets_from_csv._resolve_market_data", mock_resolve),
            patch("scripts.create_datasets_from_csv.off_chain_agent.run", return_value="/off.parquet"),
            patch("scripts.create_datasets_from_csv.on_chain_agent.run", AsyncMock(return_value="/on.parquet")),
        ):
            asyncio.run(_run_async(str(csv_path), str(tmp_path / "data"), None, False, 500))

        assert mock_resolve.call_count == 2

    def test_slug_marked_done_after_both_siblings_succeed(self, tmp_path):
        csv_path = _make_csv(tmp_path, [
            {"slug": "good-slug", "start_date": "2024-01-01T00", "end_date": "2024-01-01T01"},
        ])
        run_id = "goodrun"
        data_dir = str(tmp_path / "data")

        mock_resolve = MagicMock(return_value=MARKET_DATA)
        with (
            patch("scripts.create_datasets_from_csv._resolve_market_data", mock_resolve),
            patch("scripts.create_datasets_from_csv.off_chain_agent.run", return_value="/off.parquet"),
            patch("scripts.create_datasets_from_csv.on_chain_agent.run", AsyncMock(return_value="/on.parquet")),
        ):
            asyncio.run(_run_async(str(csv_path), data_dir, run_id, False, 500))

        run_dir = os.path.join(data_dir, f"run{run_id}")
        manifest = _load_manifest(run_dir)
        assert manifest["slugs"]["good-slug"]["status"] == "completed"

    def test_multi_market_slug_runs_one_on_chain_task_per_market(self, tmp_path):
        csv_path = _make_csv(tmp_path, [
            {"slug": "multi-slug", "start_date": "2024-01-01T00", "end_date": "2024-01-01T01"},
        ])
        multi_market_data = {
            "token_ids": ["tok1", "tok2", "tok3"],
            "markets": [
                {"condition_id": "0xaaa", "token_ids": ["tok1"]},
                {"condition_id": "0xbbb", "token_ids": ["tok2", "tok3"]},
            ],
        }
        mock_on_chain = AsyncMock(return_value="/on.parquet")
        with (
            patch("scripts.create_datasets_from_csv._resolve_market_data", return_value=multi_market_data),
            patch("scripts.create_datasets_from_csv.off_chain_agent.run", return_value="/off.parquet"),
            patch("scripts.create_datasets_from_csv.on_chain_agent.run", mock_on_chain),
        ):
            asyncio.run(_run_async(str(csv_path), str(tmp_path / "data"), None, False, 500))

        assert mock_on_chain.call_count == 2
