"""eth_getLogs wrapper for fetching PositionSplit and PositionsMerge events from the Polymarket CTF contract."""
import logging
import os

import requests

from scripts.utils.data_structures import RateLimiter
from scripts.utils.disk_cache import DiskCache

logger = logging.getLogger(__name__)

_session = requests.Session()

CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
POSITION_SPLIT_TOPIC0 = "0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298"
POSITIONS_MERGE_TOPIC0 = "0x6f13ca62553fcc2bcd2372180a43949c1e4cebba603901ede2f4e14f36b282ca"
PARENT_COLLECTION_ID = "0x0000000000000000000000000000000000000000000000000000000000000000"
ETH_GETLOGS_CU_COST = 75


def _fetch_logs_range(
    url: str,
    topic0: str,
    condition_id_padded: str,
    from_block: int,
    to_block: int,
    rate_limiter: RateLimiter,
) -> list[dict]:
    """Fetch logs for a single block range, splitting recursively on Alchemy errors."""
    response_holder: list = [None]

    def _call() -> None:
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [
                {
                    "address": CTF_CONTRACT,
                    "topics": [topic0, None, PARENT_COLLECTION_ID, condition_id_padded],
                    "fromBlock": hex(from_block),
                    "toBlock": hex(to_block),
                }
            ],
            "id": 1,
        }
        resp = _session.post(url, json=payload, timeout=30)
        try:
            response_holder[0] = resp.json()
        except Exception:
            # Empty or non-JSON body — treat as a retriable range error
            logger.warning(
                "eth_getLogs non-JSON response (status=%d) blocks=%d..%d — will split",
                resp.status_code, from_block, to_block,
            )
            response_holder[0] = {"error": {"code": -1, "message": "empty response"}}

    rate_limiter.execute(_call, tokens_needed=ETH_GETLOGS_CU_COST)
    data = response_holder[0]

    if "error" in data:
        if from_block == to_block:
            logger.warning("eth_getLogs error on single block %d: %s", from_block, data["error"])
            return []
        mid = (from_block + to_block) // 2
        left = _fetch_logs_range(url, topic0, condition_id_padded, from_block, mid, rate_limiter)
        right = _fetch_logs_range(url, topic0, condition_id_padded, mid + 1, to_block, rate_limiter)
        return left + right

    return data.get("result", [])


def fetch_events(
    conditionId: str,
    from_block: int,
    to_block: int,
    rate_limiter: RateLimiter,
    cache: DiskCache,
) -> tuple[list[dict], list[dict]]:
    """Fetch PositionSplit and PositionsMerge events for a conditionId over a block range.

    Returns (split_events, merge_events). Results are cached on disk; cache hits bypass Alchemy.
    """
    split_key = f"{conditionId}_{from_block}_{to_block}_split"
    merge_key = f"{conditionId}_{from_block}_{to_block}_merge"

    cached_splits = cache.get(split_key)
    cached_merges = cache.get(merge_key)

    if cached_splits is not None and cached_merges is not None:
        return cached_splits, cached_merges

    url = os.environ["ALCHEMY_POLYGON_URL"]

    condition_id_padded = conditionId if conditionId.startswith("0x") else "0x" + conditionId

    split_events = _fetch_logs_range(
        url, POSITION_SPLIT_TOPIC0, condition_id_padded, from_block, to_block, rate_limiter
    )
    merge_events = _fetch_logs_range(
        url, POSITIONS_MERGE_TOPIC0, condition_id_padded, from_block, to_block, rate_limiter
    )

    cache.set(split_key, split_events)
    cache.set(merge_key, merge_events)

    return split_events, merge_events
