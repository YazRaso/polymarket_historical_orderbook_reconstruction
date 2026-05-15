"""Block/timestamp conversion utilities via Alchemy eth_getBlockByNumber."""
import os

import requests

from scripts.utils.data_structures import RateLimiter

ETH_GETBLOCK_CU_COST = 75


def _get_block(number_hex: str) -> dict:
    url = os.environ["ALCHEMY_POLYGON_URL"]
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
    }
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [number_hex, False],
        "id": 1,
    }
    resp = requests.post(url, json=payload, timeout=30)
    return resp.json()["result"]


def block_to_timestamp(block_number: int, rate_limiter: RateLimiter) -> int:
    """Return the unix timestamp of a Polygon block."""
    result_holder: list = [None]

    def _call() -> None:
        result_holder[0] = _get_block(hex(block_number))

    rate_limiter.execute(_call, tokens_needed=ETH_GETBLOCK_CU_COST)
    return int(result_holder[0]["timestamp"], 16)


def timestamp_to_block(timestamp: int, rate_limiter: RateLimiter) -> int:
    """Return the latest block number whose timestamp is <= the given unix timestamp.

    Uses binary search over eth_getBlockByNumber. Requires O(log N) Alchemy calls.
    """
    latest_holder: list = [None]

    def _get_latest() -> None:
        latest_holder[0] = _get_block("latest")

    rate_limiter.execute(_get_latest, tokens_needed=ETH_GETBLOCK_CU_COST)
    latest_block = int(latest_holder[0]["number"], 16)

    def _get_ts(number: int) -> int:
        ts_holder: list = [None]

        def _call() -> None:
            ts_holder[0] = _get_block(hex(number))

        rate_limiter.execute(_call, tokens_needed=ETH_GETBLOCK_CU_COST)
        return int(ts_holder[0]["timestamp"], 16)

    lo, hi = 0, latest_block
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if _get_ts(mid) <= timestamp:
            lo = mid
        else:
            hi = mid - 1

    return lo
