"""Open interest computation from PositionSplit and PositionsMerge events."""
import logging
from typing import Callable

logger = logging.getLogger(__name__)

_USDC_DECIMALS = 1_000_000
_EXPECTED_PARTITION = [1, 2]


def _parse_hex_int(hex_str: str) -> int:
    return int(hex_str, 16)


def _decode_amount(data_hex: str) -> int:
    """Extract amount from ABI-encoded event data at bytes 64-95 (slot index 2)."""
    data = data_hex[2:] if data_hex.startswith("0x") else data_hex
    return int(data[128:192], 16)


def _decode_partition(data_hex: str) -> list[int]:
    """Extract the partition uint256[] from ABI-encoded event data."""
    data = data_hex[2:] if data_hex.startswith("0x") else data_hex
    length = int(data[192:256], 16)
    return [int(data[256 + i * 64 : 256 + i * 64 + 64], 16) for i in range(length)]


def compute_hourly_oi(
    split_events: list[dict],
    merge_events: list[dict],
    hour_boundaries: list[int],
    block_to_timestamp: Callable[[int], int],
) -> tuple[float, ...]:
    """Compute open interest in USDC at each UTC hour boundary.

    Returns a tuple of floats with length equal to len(hour_boundaries).
    """
    deltas: list[tuple[int, int]] = []

    for event in split_events:
        data = event.get("data", "")
        partition = _decode_partition(data)
        if partition != _EXPECTED_PARTITION:
            logger.warning("Unexpected partition %s in split event, skipping", partition)
            continue
        block_number = _parse_hex_int(event["blockNumber"])
        timestamp = block_to_timestamp(block_number)
        amount = _decode_amount(data)
        deltas.append((timestamp, amount))

    for event in merge_events:
        data = event.get("data", "")
        partition = _decode_partition(data)
        if partition != _EXPECTED_PARTITION:
            logger.warning("Unexpected partition %s in merge event, skipping", partition)
            continue
        block_number = _parse_hex_int(event["blockNumber"])
        timestamp = block_to_timestamp(block_number)
        amount = _decode_amount(data)
        deltas.append((timestamp, -amount))

    deltas.sort(key=lambda x: x[0])

    # Build cumulative sum series: list of (timestamp, cumsum_at_that_point)
    cumsum_series: list[tuple[int, int]] = []
    running = 0
    for ts, delta in deltas:
        running += delta
        cumsum_series.append((ts, running))

    result: list[float] = []

    for boundary in hour_boundaries:
        # Find last event with timestamp <= boundary
        value = 0
        for ts, cum in cumsum_series:
            if ts <= boundary:
                value = cum
            else:
                break
        result.append(value / _USDC_DECIMALS)

    return tuple(result)
