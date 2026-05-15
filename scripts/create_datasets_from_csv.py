"""Parent orchestration script for the full reconstruction pipeline (OC-07).

Reads the CSV manifest, calls the Gamma API exactly once per slug, then
dispatches pre-resolved arguments to the off-chain and on-chain siblings.
Neither sibling calls the Gamma API or reads the CSV.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import time
import uuid
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

from scripts import dataset_factory
from scripts import off_chain_agent
from scripts import on_chain_agent
from scripts.utils.data_structures import RateLimiter
from scripts.utils.disk_cache import DiskCache


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _read_jobs(csv_path: str) -> List[Dict[str, str]]:
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        required = {"slug", "start_date", "end_date"}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError("CSV must include columns: slug, start_date, end_date")
        return [row for row in reader]


def _clean_field(value: str) -> str:
    return value.strip().strip('"').strip("'")


# ---------------------------------------------------------------------------
# Gamma API resolution (called once per slug by the parent)
# ---------------------------------------------------------------------------

def _resolve_market_data(slug: str) -> Dict[str, Any]:
    """Call the Gamma API once and return token_ids + per-market conditionIds.

    Returns:
        {
            "token_ids": [...],   # flat list across all markets for off-chain
            "markets": [
                {"condition_id": "0x...", "token_ids": [...]}
            ]
        }
    """
    response = requests.get(f"https://gamma-api.polymarket.com/events/slug/{slug}")
    if not response.ok:
        raise ValueError(
            f"Gamma API failed for slug '{slug}': HTTP {response.status_code}"
        )
    data = response.json()
    markets = data.get("markets", [])
    if not markets:
        raise ValueError(f"No markets found for slug: {slug}")

    all_token_ids: List[str] = []
    market_list: List[Dict[str, Any]] = []
    for market in markets:
        token_ids = dataset_factory._normalize_clob_token_ids(market.get("clobTokenIds"))
        condition_id = market.get("conditionId", "")
        all_token_ids.extend(token_ids)
        market_list.append({"condition_id": condition_id, "token_ids": token_ids})

    return {"token_ids": all_token_ids, "markets": market_list}


# ---------------------------------------------------------------------------
# Run manifest (slug-level completion tracking)
# ---------------------------------------------------------------------------

def _manifest_path(run_dir: str) -> str:
    return os.path.join(run_dir, "manifest.json")


def _load_manifest(run_dir: str) -> Dict[str, Any]:
    path = _manifest_path(run_dir)
    if not os.path.exists(path):
        return {"slugs": {}}
    with open(path) as f:
        return json.load(f)


def _save_manifest(run_dir: str, manifest: Dict[str, Any]) -> None:
    path = _manifest_path(run_dir)
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


def _slug_done(manifest: Dict[str, Any], slug: str) -> bool:
    return manifest.get("slugs", {}).get(slug, {}).get("status") == "completed"


# ---------------------------------------------------------------------------
# Per-slug dispatch (runs both siblings concurrently)
# ---------------------------------------------------------------------------

async def _run_slug(
    slug: str,
    start_date: str,
    end_date: str,
    run_dir: str,
    market_data: Dict[str, Any],
    rate_limiter: RateLimiter,
    cache: DiskCache,
    resume: bool,
) -> Dict[str, Any]:
    """Run off-chain and on-chain siblings concurrently for one slug.

    Returns a manifest entry dict (without 'status') on success.
    Raises on failure; caller must not mark the slug as done.
    """
    token_ids = market_data["token_ids"]
    markets = market_data["markets"]

    off_chain_coro = asyncio.to_thread(
        off_chain_agent.run,
        token_ids,
        slug,
        start_date,
        end_date,
        run_dir,
        resume,
    )
    on_chain_coros = [
        on_chain_agent.run(
            m["condition_id"],
            start_date,
            end_date,
            run_dir,
            rate_limiter,
            cache,
            resume,
        )
        for m in markets
    ]

    results = await asyncio.gather(off_chain_coro, *on_chain_coros)
    off_chain_output = results[0]
    on_chain_outputs = list(results[1:])

    return {
        "off_chain_output": off_chain_output,
        "on_chain_outputs": on_chain_outputs,
    }


# ---------------------------------------------------------------------------
# Top-level run
# ---------------------------------------------------------------------------

async def _run_async(
    csv_path: str,
    output_dir: str,
    run_id: Optional[str],
    resume: bool,
    cu_per_second: int,
) -> str:
    os.makedirs(output_dir, exist_ok=True)
    run_id = run_id or uuid.uuid4().hex
    run_dir = os.path.join(output_dir, f"run{run_id}")
    os.makedirs(run_dir, exist_ok=True)
    os.makedirs(os.path.join(run_dir, "checkpoints"), exist_ok=True)
    print(f"Run directory: {run_dir}")

    rate_limiter = RateLimiter(
        rate=cu_per_second,
        per=1.0,
        bucket_size=cu_per_second,
        max_workers=3,
    )
    cache = DiskCache(os.path.join(run_dir, "cache"))

    jobs = _read_jobs(csv_path)
    print(f"Loaded {len(jobs)} job(s) from {csv_path}")

    manifest = _load_manifest(run_dir) if resume else {"slugs": {}}

    completed = 0
    failures: List[str] = []

    for job_idx, job in enumerate(jobs, start=1):
        slug = _clean_field(job["slug"])
        start_date = _clean_field(job["start_date"])
        end_date = _clean_field(job["end_date"])

        if resume and _slug_done(manifest, slug):
            print(f"[job {job_idx}/{len(jobs)}] skip completed slug={slug}")
            completed += 1
            continue

        print(f"[job {job_idx}/{len(jobs)}] resolving market data slug={slug}")
        try:
            market_data = _resolve_market_data(slug)
        except Exception as exc:
            print(f"[job {job_idx}/{len(jobs)}] ERROR resolving slug={slug}: {exc}")
            failures.append(slug)
            continue

        print(
            f"[job {job_idx}/{len(jobs)}] slug={slug} "
            f"token_ids={len(market_data['token_ids'])} "
            f"markets={len(market_data['markets'])} "
            f"range={start_date}..{end_date}"
        )

        started_at = time.time()
        try:
            entry = await _run_slug(
                slug, start_date, end_date, run_dir,
                market_data, rate_limiter, cache, resume,
            )
        except Exception as exc:
            print(f"[job {job_idx}/{len(jobs)}] ERROR running slug={slug}: {exc}")
            failures.append(slug)
            continue

        elapsed = time.time() - started_at
        manifest["slugs"][slug] = {
            "status": "completed",
            "off_chain_output": entry["off_chain_output"],
            "on_chain_outputs": entry["on_chain_outputs"],
            "completed_at_epoch": time.time(),
        }
        _save_manifest(run_dir, manifest)
        completed += 1
        print(
            f"[job {job_idx}/{len(jobs)}] completed slug={slug} elapsed={elapsed:.1f}s"
        )

    print(
        f"\nDone: {completed}/{len(jobs)} slugs completed, "
        f"{len(failures)} failure(s): {failures or 'none'}"
    )
    print(f"Run directory: {run_dir}")
    return run_dir


def run(
    csv_path: str,
    output_dir: str = "data",
    run_id: Optional[str] = None,
    resume: bool = True,
    cu_per_second: int = 500,
) -> str:
    """Process all jobs from a CSV manifest and write outputs to a run directory.

    Args:
        csv_path: Input CSV with columns slug, start_date, end_date.
        output_dir: Base directory for run outputs.
        run_id: Optional run id for checkpoint resume.
        resume: Whether to skip completed slugs and hours.
        cu_per_second: Alchemy compute units per second budget.

    Returns:
        Path to the run output directory.
    """
    return asyncio.run(_run_async(csv_path, output_dir, run_id, resume, cu_per_second))


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Full reconstruction pipeline: off-chain + on-chain per CSV row"
    )
    parser.add_argument(
        "csv_path",
        nargs="?",
        help="CSV path with columns slug,start_date,end_date",
    )
    parser.add_argument(
        "--csv-path",
        dest="csv_path_flag",
        help="CSV path with columns slug,start_date,end_date",
    )
    parser.add_argument("--output-dir", default="data", help="Output directory for run manifest")
    parser.add_argument(
        "--run-id",
        help="Reuse a specific run id for checkpoint resume. If omitted, a new run id is created.",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore checkpoints and reprocess every slug.",
    )
    parser.add_argument(
        "--cu-per-second",
        type=int,
        default=500,
        dest="cu_per_second",
        help="Alchemy compute units per second (default: 500).",
    )
    args = parser.parse_args()

    csv_path = args.csv_path_flag or args.csv_path
    if not csv_path:
        raise ValueError("csv_path is required (positional or --csv-path)")
    if csv_path.startswith("csv_path="):
        csv_path = csv_path.split("=", 1)[1]

    run_dir = run(
        csv_path,
        output_dir=args.output_dir,
        run_id=args.run_id,
        resume=not args.no_resume,
        cu_per_second=args.cu_per_second,
    )
    print(f"Wrote run outputs to {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
