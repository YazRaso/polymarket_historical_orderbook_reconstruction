"""Unit tests for compute_hourly_oi (OC-05)."""
import logging

import pytest

from scripts.oi import compute_hourly_oi


def _encode_event_data(amount: int, partition: list[int] | None = None) -> str:
    """Build ABI-encoded event data for (address collateralToken, uint256[] partition, uint256 amount).

    Layout (each slot is 32 bytes = 64 hex chars):
      slot 0 (bytes 0-31):   collateralToken (address, zero-padded)
      slot 1 (bytes 32-63):  offset to partition array = 0x60 (96 bytes from start)
      slot 2 (bytes 64-95):  amount
      slot 3 (bytes 96-127): partition array length
      slot 4+ (bytes 128+):  partition values
    """
    if partition is None:
        partition = [1, 2]

    collateral = "00" * 32
    offset = format(96, "064x")           # 0x60 = offset to partition data
    amount_hex = format(amount, "064x")
    length_hex = format(len(partition), "064x")
    values_hex = "".join(format(v, "064x") for v in partition)

    return "0x" + collateral + offset + amount_hex + length_hex + values_hex


def _make_event(block_number: int, amount: int, partition: list[int] | None = None) -> dict:
    return {
        "blockNumber": hex(block_number),
        "data": _encode_event_data(amount, partition),
    }


def _block_to_ts(block_number_to_ts: dict):
    return lambda block: block_number_to_ts[block]


class TestComputeHourlyOI:
    def test_output_length_matches_hour_boundaries(self):
        result = compute_hourly_oi([], [], [100, 200, 300], lambda b: b)
        assert len(result) == 3

    def test_empty_boundaries_returns_empty_tuple(self):
        result = compute_hourly_oi([], [], [], lambda b: b)
        assert result == ()

    def test_no_events_returns_all_zeros(self):
        result = compute_hourly_oi([], [], [1000, 2000, 3000], lambda b: b)
        assert result == (0.0, 0.0, 0.0)

    def test_single_split_event_reflects_in_oi(self):
        # 1_000_000 raw units = 1 USDC
        events = [_make_event(block_number=5, amount=1_000_000)]
        # block 5 → timestamp 500; boundary at 1000 should see it
        result = compute_hourly_oi(events, [], [1000], lambda b: b * 100)
        assert result == (1.0,)

    def test_merge_decrements_oi(self):
        splits = [_make_event(block_number=1, amount=2_000_000)]
        merges = [_make_event(block_number=2, amount=1_000_000)]
        result = compute_hourly_oi(splits, merges, [1000], lambda b: b * 100)
        assert result == (1.0,)

    def test_carry_forward_between_hours(self):
        # Split at ts=50, boundaries at 100, 200, 300 — all should carry OI forward
        splits = [_make_event(block_number=1, amount=3_000_000)]
        result = compute_hourly_oi(splits, [], [100, 200, 300], lambda b: b * 50)
        assert result == (3.0, 3.0, 3.0)

    def test_no_event_before_first_boundary_is_zero(self):
        # Event at timestamp 200 but first boundary is 100
        splits = [_make_event(block_number=4, amount=1_000_000)]
        result = compute_hourly_oi(splits, [], [100, 300], lambda b: b * 50)
        assert result[0] == 0.0
        assert result[1] == 1.0

    def test_event_after_last_boundary_is_ignored(self):
        splits = [_make_event(block_number=10, amount=5_000_000)]
        result = compute_hourly_oi(splits, [], [100, 200], lambda b: b * 10)
        # block 10 → ts 100 (= boundary, included), block 10 → ts 100
        # block 10 → ts 100 lands exactly on boundary 100
        result2 = compute_hourly_oi(splits, [], [50, 90], lambda b: b * 10)
        # block 10 → ts 100, boundaries are 50 and 90: event is after both, so both zero
        assert result2 == (0.0, 0.0)

    def test_amount_scaled_by_1e6(self):
        # 5_500_000 raw = 5.5 USDC
        splits = [_make_event(block_number=1, amount=5_500_000)]
        result = compute_hourly_oi(splits, [], [1000], lambda b: b)
        assert abs(result[0] - 5.5) < 1e-9

    def test_invalid_partition_skipped_with_warning(self, caplog):
        bad_event = _make_event(block_number=1, amount=1_000_000, partition=[1, 3])
        good_event = _make_event(block_number=2, amount=2_000_000)

        with caplog.at_level(logging.WARNING, logger="scripts.oi"):
            result = compute_hourly_oi([bad_event, good_event], [], [1000], lambda b: b)

        assert result == (2.0,)
        assert any("partition" in msg.lower() for msg in caplog.messages)

    def test_multiple_events_cumulative(self):
        splits = [
            _make_event(block_number=1, amount=1_000_000),
            _make_event(block_number=2, amount=2_000_000),
        ]
        merges = [
            _make_event(block_number=3, amount=500_000),
        ]
        result = compute_hourly_oi(splits, merges, [1000], lambda b: b)
        # 1 + 2 - 0.5 = 2.5 USDC
        assert abs(result[0] - 2.5) < 1e-9

    def test_boundary_at_exact_event_timestamp_includes_event(self):
        splits = [_make_event(block_number=100, amount=1_000_000)]
        result = compute_hourly_oi(splits, [], [100], lambda b: b)
        assert result == (1.0,)

    def test_output_is_tuple(self):
        result = compute_hourly_oi([], [], [1, 2], lambda b: b)
        assert isinstance(result, tuple)
