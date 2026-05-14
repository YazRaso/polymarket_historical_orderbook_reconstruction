"""On-chain OI orchestrator for a single Polymarket market (OC-06)."""
import asyncio
import functools
import json
import os
import tempfile
from datetime import datetime, timezone, timedelta

import pandas as pd

from scripts.oi import compute_hourly_oi
from scripts.utils.block_timestamp import block_to_timestamp, timestamp_to_block
from scripts.utils.checkpoint import load_checkpoint, save_checkpoint
from scripts.utils.data_structures import RateLimiter
from scripts.utils.disk_cache import DiskCache
from scripts.utils.eth_logs import fetch_events


def _checkpoint_path(run_dir: str, condition_id: str) -> str:
    return os.path.join(run_dir, "checkpoints", f"{condition_id}.json")


def _generate_hour_boundaries(start_date: str, end_date: str) -> list[int]:
    """Return unix timestamps for every full hour strictly between start and end."""
    start_dt = datetime.strptime(start_date, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end_date, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)

    boundaries: list[int] = []
    current = start_dt + timedelta(hours=1)
    while current < end_dt:
        boundaries.append(int(current.timestamp()))
        current += timedelta(hours=1)
    return boundaries


async def run_market(
    conditionId: str,
    start_date: str,
    end_date: str,
    run_dir: str,
    rate_limiter: RateLimiter,
    cache: DiskCache,
    resume: bool,
) -> str:
    """Orchestrate on-chain OI computation for one market.

    Returns the path of the written parquet file.
    """
    ckpt_path = _checkpoint_path(run_dir, conditionId)
    os.makedirs(os.path.dirname(ckpt_path), exist_ok=True)

    # Step 1 — checkpoint check
    ckpt = load_checkpoint(ckpt_path)
    if resume and ckpt.get("status") == "completed":
        return ckpt["output_path"]

    # Step 2 — generate hour boundaries
    hour_boundaries = _generate_hour_boundaries(start_date, end_date)

    # Step 3 — resolve block range
    start_ts = int(
        datetime.strptime(start_date, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc).timestamp()
    )
    end_ts = int(
        datetime.strptime(end_date, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc).timestamp()
    )
    from_block = await asyncio.to_thread(timestamp_to_block, start_ts, rate_limiter)
    to_block = await asyncio.to_thread(timestamp_to_block, end_ts, rate_limiter)

    # Step 4 — fetch events
    split_events, merge_events = await asyncio.to_thread(
        fetch_events, conditionId, from_block, to_block, rate_limiter, cache
    )

    # Step 5 — compute OI
    b2ts = functools.partial(block_to_timestamp, rate_limiter=rate_limiter)
    oi_values = compute_hourly_oi(split_events, merge_events, hour_boundaries, b2ts)

    # Step 6 — write output parquet
    output_path = os.path.join(
        run_dir, f"{conditionId}_{start_date}_{end_date}_onchain.parquet"
    )
    hour_labels = [
        datetime.fromtimestamp(b, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for b in hour_boundaries
    ]
    df = pd.DataFrame({"hour": hour_labels, "oi_usdc": list(oi_values)})
    df.to_parquet(output_path, index=False)

    # Step 7 — update checkpoint
    new_ckpt = {
        "status": "completed",
        "output_path": output_path,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
    }
    save_checkpoint(ckpt_path, new_ckpt)

    return output_path
