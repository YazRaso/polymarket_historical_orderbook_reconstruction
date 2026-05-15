"""One-off script to run on-chain OI for a specific run directory."""
import asyncio
import os
import sys

# Resolve project root regardless of working directory
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)

# Load .env from project root
_env_path = os.path.join(_PROJECT_ROOT, ".env")
print(f"Loading env from: {_env_path} (exists={os.path.exists(_env_path)})")
with open(_env_path) as _f:
    for _line in _f:
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ[_k.strip()] = _v.strip()

print(f"ALCHEMY_POLYGON_URL = {os.environ.get('ALCHEMY_POLYGON_URL', 'NOT SET')}")

from scripts.onchain import run_market
from scripts.utils.data_structures import RateLimiter
from scripts.utils.disk_cache import DiskCache

RUN_DIR = os.path.join(_PROJECT_ROOT, "data/run528553261d8e40ad86bf4a6482e397fa")
CONDITION_ID = "0x6747bae2269c348786d0c0c0f4286ac006f13f16325d4dcbab954b46de029f19"
START_DATE = "2026-04-20T22"
END_DATE = "2026-05-01T00"


async def main():
    rate_limiter = RateLimiter(rate=500, per=1.0, bucket_size=500, max_workers=3)
    cache = DiskCache(os.path.join(RUN_DIR, "cache"))

    print(f"condition_id: {CONDITION_ID}")
    print(f"run_dir:      {RUN_DIR}")
    print(f"range:        {START_DATE} -> {END_DATE}")

    output = await run_market(
        CONDITION_ID, START_DATE, END_DATE, RUN_DIR, rate_limiter, cache, resume=True
    )
    print(f"Done: {output}")


if __name__ == "__main__":
    asyncio.run(main())
