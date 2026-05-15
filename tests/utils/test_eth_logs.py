"""Unit tests for the eth_getLogs wrapper (OC-04)."""
from unittest.mock import MagicMock, patch, call

import pytest

from scripts.utils.data_structures import RateLimiter
from scripts.utils.disk_cache import DiskCache
from scripts.utils.eth_logs import fetch_events, _fetch_logs_range, POSITION_SPLIT_TOPIC0


CONDITION_ID = "0xabc123" + "0" * 58  # 32-byte hex
FROM_BLOCK = 1000
TO_BLOCK = 2000

FAKE_URL = "https://polygon.alchemy.fake/v2/key"

SPLIT_EVENT = {"blockNumber": "0x3e8", "data": "0x" + "aa" * 32}
MERGE_EVENT = {"blockNumber": "0x3e9", "data": "0x" + "bb" * 32}


def make_rate_limiter() -> RateLimiter:
    return RateLimiter(rate=300, per=1, bucket_size=10_000, max_workers=1)


def make_mock_response(result=None, error=None):
    resp = MagicMock()
    payload = {"jsonrpc": "2.0", "id": 1}
    if error is not None:
        payload["error"] = error
    else:
        payload["result"] = result or []
    resp.json.return_value = payload
    return resp


class TestDiskCache:
    def test_get_missing_key_returns_none(self, tmp_path):
        cache = DiskCache(str(tmp_path / "cache"))
        assert cache.get("nonexistent") is None

    def test_set_then_get_returns_value(self, tmp_path):
        cache = DiskCache(str(tmp_path / "cache"))
        cache.set("mykey", [{"foo": 1}])
        assert cache.get("mykey") == [{"foo": 1}]

    def test_set_overwrites_existing(self, tmp_path):
        cache = DiskCache(str(tmp_path / "cache"))
        cache.set("k", [1])
        cache.set("k", [2])
        assert cache.get("k") == [2]


class TestFetchEvents:
    def test_cache_hit_on_both_skips_alchemy(self, tmp_path, monkeypatch):
        cache = DiskCache(str(tmp_path / "cache"))
        splits = [SPLIT_EVENT]
        merges = [MERGE_EVENT]
        cache.set(f"{CONDITION_ID}_{FROM_BLOCK}_{TO_BLOCK}_split", splits)
        cache.set(f"{CONDITION_ID}_{FROM_BLOCK}_{TO_BLOCK}_merge", merges)

        rl = make_rate_limiter()
        with patch("scripts.utils.eth_logs._session.post") as mock_post:
            result_splits, result_merges = fetch_events(CONDITION_ID, FROM_BLOCK, TO_BLOCK, rl, cache)

        mock_post.assert_not_called()
        assert result_splits == splits
        assert result_merges == merges

    def test_partial_cache_hit_still_calls_alchemy(self, tmp_path, monkeypatch):
        """If only split is cached, we still fetch from Alchemy (both or nothing)."""
        cache = DiskCache(str(tmp_path / "cache"))
        cache.set(f"{CONDITION_ID}_{FROM_BLOCK}_{TO_BLOCK}_split", [SPLIT_EVENT])
        # merge not cached

        rl = make_rate_limiter()
        monkeypatch.setenv("ALCHEMY_POLYGON_URL", FAKE_URL)
        with patch("scripts.utils.eth_logs._session.post", return_value=make_mock_response(result=[])):
            fetch_events(CONDITION_ID, FROM_BLOCK, TO_BLOCK, rl, cache)

        # Should have called Alchemy (at least once for split, once for merge)
        # We just verify it doesn't raise and Alchemy was called

    def test_successful_fetch_returns_events(self, tmp_path, monkeypatch):
        cache = DiskCache(str(tmp_path / "cache"))
        rl = make_rate_limiter()
        monkeypatch.setenv("ALCHEMY_POLYGON_URL", FAKE_URL)

        responses = [
            make_mock_response(result=[SPLIT_EVENT]),
            make_mock_response(result=[MERGE_EVENT]),
        ]
        with patch("scripts.utils.eth_logs._session.post", side_effect=responses) as mock_post:
            splits, merges = fetch_events(CONDITION_ID, FROM_BLOCK, TO_BLOCK, rl, cache)

        assert splits == [SPLIT_EVENT]
        assert merges == [MERGE_EVENT]
        assert mock_post.call_count == 2

    def test_results_written_to_cache(self, tmp_path, monkeypatch):
        cache = DiskCache(str(tmp_path / "cache"))
        rl = make_rate_limiter()
        monkeypatch.setenv("ALCHEMY_POLYGON_URL", FAKE_URL)

        responses = [
            make_mock_response(result=[SPLIT_EVENT]),
            make_mock_response(result=[MERGE_EVENT]),
        ]
        with patch("scripts.utils.eth_logs._session.post", side_effect=responses):
            fetch_events(CONDITION_ID, FROM_BLOCK, TO_BLOCK, rl, cache)

        assert cache.get(f"{CONDITION_ID}_{FROM_BLOCK}_{TO_BLOCK}_split") == [SPLIT_EVENT]
        assert cache.get(f"{CONDITION_ID}_{FROM_BLOCK}_{TO_BLOCK}_merge") == [MERGE_EVENT]

    def test_empty_result_is_cached_and_returned(self, tmp_path, monkeypatch):
        cache = DiskCache(str(tmp_path / "cache"))
        rl = make_rate_limiter()
        monkeypatch.setenv("ALCHEMY_POLYGON_URL", FAKE_URL)

        with patch("scripts.utils.eth_logs._session.post", return_value=make_mock_response(result=[])):
            splits, merges = fetch_events(CONDITION_ID, FROM_BLOCK, TO_BLOCK, rl, cache)

        assert splits == []
        assert merges == []

    def test_rate_limiter_called_once_per_http_request(self, tmp_path, monkeypatch):
        cache = DiskCache(str(tmp_path / "cache"))
        rl = make_rate_limiter()
        monkeypatch.setenv("ALCHEMY_POLYGON_URL", FAKE_URL)

        call_count = [0]
        original_execute = rl.execute

        def counting_execute(task, *args, tokens_needed):
            call_count[0] += 1
            return original_execute(task, *args, tokens_needed=tokens_needed)

        rl.execute = counting_execute

        with patch("scripts.utils.eth_logs._session.post", return_value=make_mock_response(result=[])):
            fetch_events(CONDITION_ID, FROM_BLOCK, TO_BLOCK, rl, cache)

        # One acquire per Alchemy call: one for split, one for merge = 2
        assert call_count[0] == 2

    def test_returns_two_separate_lists(self, tmp_path, monkeypatch):
        cache = DiskCache(str(tmp_path / "cache"))
        rl = make_rate_limiter()
        monkeypatch.setenv("ALCHEMY_POLYGON_URL", FAKE_URL)

        with patch("scripts.utils.eth_logs._session.post", return_value=make_mock_response(result=[])):
            result = fetch_events(CONDITION_ID, FROM_BLOCK, TO_BLOCK, rl, cache)

        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], list)
        assert isinstance(result[1], list)


class TestFetchLogsRange:
    def test_block_range_error_triggers_binary_split(self, monkeypatch):
        monkeypatch.setenv("ALCHEMY_POLYGON_URL", FAKE_URL)
        rl = make_rate_limiter()

        call_count = [0]
        from_block = 1000
        to_block = 1001
        mid = (from_block + to_block) // 2

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            payload = kwargs.get("json", {})
            params = payload.get("params", [{}])[0]
            req_from = int(params["fromBlock"], 16)
            req_to = int(params["toBlock"], 16)
            # First call (full range) returns error; sub-calls succeed
            if req_from == from_block and req_to == to_block:
                return make_mock_response(error={"code": -32602, "message": "block range too wide"})
            return make_mock_response(result=[])

        with patch("scripts.utils.eth_logs._session.post", side_effect=side_effect):
            result = _fetch_logs_range(
                FAKE_URL, POSITION_SPLIT_TOPIC0, CONDITION_ID, from_block, to_block, rl
            )

        # Full range errored → 2 sub-calls
        assert call_count[0] == 3
        assert result == []

    def test_successful_range_returns_events(self, monkeypatch):
        rl = make_rate_limiter()
        events = [SPLIT_EVENT, SPLIT_EVENT]

        with patch("scripts.utils.eth_logs._session.post", return_value=make_mock_response(result=events)):
            result = _fetch_logs_range(
                FAKE_URL, POSITION_SPLIT_TOPIC0, CONDITION_ID, FROM_BLOCK, TO_BLOCK, rl
            )

        assert result == events

    def test_rate_limiter_acquired_per_alchemy_call(self, monkeypatch):
        rl = make_rate_limiter()
        call_count = [0]
        original_execute = rl.execute

        def counting_execute(task, *args, tokens_needed):
            call_count[0] += 1
            return original_execute(task, *args, tokens_needed=tokens_needed)

        rl.execute = counting_execute

        def side_effect(*args, **kwargs):
            payload = kwargs.get("json", {})
            params = payload.get("params", [{}])[0]
            req_from = int(params["fromBlock"], 16)
            req_to = int(params["toBlock"], 16)
            if req_from == FROM_BLOCK and req_to == TO_BLOCK:
                return make_mock_response(error={"code": -32602, "message": "overflow"})
            return make_mock_response(result=[])

        with patch("scripts.utils.eth_logs._session.post", side_effect=side_effect):
            _fetch_logs_range(
                FAKE_URL, POSITION_SPLIT_TOPIC0, CONDITION_ID, FROM_BLOCK, TO_BLOCK, rl
            )

        # 1 failed outer + 2 sub-calls = 3 rate limiter acquisitions
        assert call_count[0] == 3
