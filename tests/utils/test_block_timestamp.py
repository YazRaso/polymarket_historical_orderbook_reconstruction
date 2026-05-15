"""Unit tests for block_timestamp utilities (OC-06 dependency)."""
from unittest.mock import patch, MagicMock

import pytest

from scripts.utils.data_structures import RateLimiter
from scripts.utils.block_timestamp import block_to_timestamp, timestamp_to_block


FAKE_URL = "https://polygon.alchemy.fake/v2/key"


def make_rate_limiter() -> RateLimiter:
    return RateLimiter(rate=300, per=1, bucket_size=10_000, max_workers=1)


def _block_response(number: int, timestamp: int) -> MagicMock:
    m = MagicMock()
    m.json.return_value = {
        "result": {
            "number": hex(number),
            "timestamp": hex(timestamp),
        }
    }
    return m


class TestBlockToTimestamp:
    def test_returns_timestamp_for_block(self, monkeypatch):
        monkeypatch.setenv("ALCHEMY_POLYGON_URL", FAKE_URL)
        rl = make_rate_limiter()

        with patch("scripts.utils.block_timestamp._session.post", return_value=_block_response(1000, 1_700_000_000)):
            result = block_to_timestamp(1000, rl)

        assert result == 1_700_000_000

    def test_rate_limiter_acquired_once(self, monkeypatch):
        monkeypatch.setenv("ALCHEMY_POLYGON_URL", FAKE_URL)
        rl = make_rate_limiter()
        call_count = [0]
        original = rl.execute

        def counting(task, *args, tokens_needed):
            call_count[0] += 1
            return original(task, *args, tokens_needed=tokens_needed)

        rl.execute = counting
        with patch("scripts.utils.block_timestamp._session.post", return_value=_block_response(5, 999)):
            block_to_timestamp(5, rl)

        assert call_count[0] == 1

    def test_hex_block_number_sent_to_alchemy(self, monkeypatch):
        monkeypatch.setenv("ALCHEMY_POLYGON_URL", FAKE_URL)
        rl = make_rate_limiter()
        captured = []

        def fake_post(url, json=None, **kwargs):
            captured.append(json)
            return _block_response(42, 12345)

        with patch("scripts.utils.block_timestamp._session.post", side_effect=fake_post):
            block_to_timestamp(42, rl)

        assert captured[0]["params"][0] == hex(42)


class TestTimestampToBlock:
    def _make_post_side_effect(self, block_ts_map: dict, latest_block: int):
        """Return a side_effect function for requests.post given a block→timestamp map."""
        def side_effect(url, json=None, **kwargs):
            params = json.get("params", [])
            number_param = params[0] if params else "latest"
            if number_param == "latest":
                ts = block_ts_map[latest_block]
                return _block_response(latest_block, ts)
            block_num = int(number_param, 16)
            ts = block_ts_map.get(block_num, 0)
            return _block_response(block_num, ts)

        return side_effect

    def test_returns_block_at_or_before_timestamp(self, monkeypatch):
        """With blocks at ts 100, 200, 300, asking for ts=250 should return block 2."""
        monkeypatch.setenv("ALCHEMY_POLYGON_URL", FAKE_URL)
        rl = make_rate_limiter()

        # 3-block chain: block 0→ts 0, 1→100, 2→200, 3→300
        block_ts = {0: 0, 1: 100, 2: 200, 3: 300}
        latest = 3

        with patch("scripts.utils.block_timestamp._session.post", side_effect=self._make_post_side_effect(block_ts, latest)):
            result = timestamp_to_block(250, rl)

        assert result == 2

    def test_exact_boundary_match(self, monkeypatch):
        """Asking for exactly block 2's timestamp should return block 2."""
        monkeypatch.setenv("ALCHEMY_POLYGON_URL", FAKE_URL)
        rl = make_rate_limiter()

        block_ts = {0: 0, 1: 100, 2: 200, 3: 300}
        with patch("scripts.utils.block_timestamp._session.post", side_effect=self._make_post_side_effect(block_ts, 3)):
            result = timestamp_to_block(200, rl)

        assert result == 2

    def test_timestamp_before_genesis_returns_zero(self, monkeypatch):
        monkeypatch.setenv("ALCHEMY_POLYGON_URL", FAKE_URL)
        rl = make_rate_limiter()

        block_ts = {0: 50, 1: 100, 2: 200}
        with patch("scripts.utils.block_timestamp._session.post", side_effect=self._make_post_side_effect(block_ts, 2)):
            result = timestamp_to_block(10, rl)

        assert result == 0

    def test_timestamp_after_latest_returns_latest(self, monkeypatch):
        monkeypatch.setenv("ALCHEMY_POLYGON_URL", FAKE_URL)
        rl = make_rate_limiter()

        block_ts = {0: 0, 1: 100, 2: 200, 3: 300}
        with patch("scripts.utils.block_timestamp._session.post", side_effect=self._make_post_side_effect(block_ts, 3)):
            result = timestamp_to_block(999, rl)

        assert result == 3
