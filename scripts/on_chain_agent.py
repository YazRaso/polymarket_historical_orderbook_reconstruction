"""On-chain OI sibling agent (OC-07).

Receives a pre-resolved conditionId — never calls the Gamma API.
Thin async wrapper over scripts.onchain.run_market.
"""
from __future__ import annotations

from scripts.onchain import run_market
from scripts.utils.data_structures import RateLimiter
from scripts.utils.disk_cache import DiskCache


async def run(
    condition_id: str,
    start_date: str,
    end_date: str,
    run_dir: str,
    rate_limiter: RateLimiter,
    cache: DiskCache,
    resume: bool = True,
) -> str:
    """Run on-chain OI computation for one market.

    Args:
        condition_id: The conditionId field from the Gamma market object.
        start_date: Start timestamp in YYYY-MM-DDTHH format.
        end_date: End timestamp in YYYY-MM-DDTHH format.
        run_dir: Path to the shared run output directory.
        rate_limiter: Shared rate limiter for Alchemy RPC calls.
        cache: Shared disk cache for Alchemy responses.
        resume: Whether to resume from checkpoints.

    Returns:
        Path to the written on-chain OI parquet file.
    """
    return await run_market(
        condition_id, start_date, end_date, run_dir, rate_limiter, cache, resume
    )
