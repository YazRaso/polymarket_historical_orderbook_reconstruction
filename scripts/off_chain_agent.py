"""Off-chain orderbook reconstruction sibling (OC-07).

Receives pre-resolved token_ids and a run_dir — never calls the Gamma API.
"""
from __future__ import annotations

import json
import os
import time
from concurrent.futures import TimeoutError as FutureTimeoutError
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

import duckdb

import dataset_factory


def _job_key(slug: str, start: str, end: str) -> str:
    return f"{slug}__{start}__{end}".replace("/", "_")


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def _load_checkpoint(path: str) -> Dict[str, object]:
    if not os.path.exists(path):
        return {"completed_urls": {}, "job_status": "running"}
    with open(path) as f:
        data = json.load(f)
    data.setdefault("completed_urls", {})
    data.setdefault("job_status", "running")
    return data


def _save_checkpoint(path: str, checkpoint: Dict[str, object]) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(checkpoint, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


def _combine_hourly_outputs(
    conn: duckdb.DuckDBPyConnection,
    hourly_paths: List[str],
    output_path: str,
) -> int:
    if not hourly_paths:
        return 0
    hourly_literal = ", ".join([f"'{_sql_escape(p)}'" for p in hourly_paths])
    output_path_escaped = _sql_escape(output_path)
    query = (
        "COPY ("
        f"SELECT * FROM read_parquet([{hourly_literal}])"
        f") TO '{output_path_escaped}' (FORMAT PARQUET)"
    )
    conn.execute(query)
    return conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{output_path_escaped}')"
    ).fetchone()[0]


def _process_hour(
    conn: duckdb.DuckDBPyConnection,
    url: str,
    token_ids: List[str],
    hourly_output: str,
) -> int:
    result = dataset_factory.filter_parquet_by_asset_ids(
        url=url,
        asset_ids=token_ids,
        conn=conn,
        output_path=hourly_output,
    )
    return int(result.get("rows_written", 0))


def run(
    token_ids: List[str],
    slug: str,
    start_date: str,
    end_date: str,
    run_dir: str,
    resume: bool = True,
) -> str:
    """Process off-chain orderbook data for one slug.

    Args:
        token_ids: Flat list of clobTokenIds across all markets under the slug.
        slug: Market slug used for output file naming.
        start_date: Start timestamp in YYYY-MM-DDTHH format.
        end_date: End timestamp in YYYY-MM-DDTHH format.
        run_dir: Path to the shared run output directory.
        resume: Whether to resume from hour-level checkpoints.

    Returns:
        Path to the combined output parquet file.
    """
    checkpoints_dir = os.path.join(run_dir, "checkpoints")
    os.makedirs(checkpoints_dir, exist_ok=True)

    conn = duckdb.connect()
    job_started_at = time.time()
    urls = dataset_factory.urls_for_range(start_date, end_date)
    print(f"[off_chain {slug}] token_ids={len(token_ids)} hours={len(urls)}")

    output_path = os.path.join(run_dir, f"{slug}_{start_date}_{end_date}.parquet")
    job_key = _job_key(slug, start_date, end_date)
    checkpoint_path = os.path.join(checkpoints_dir, f"offchain_{job_key}.json")
    checkpoint = _load_checkpoint(checkpoint_path) if resume else {"completed_urls": {}}
    completed_urls: Dict[str, int] = checkpoint.get("completed_urls", {})  # type: ignore[assignment]

    hourly_dir = os.path.join(run_dir, "hourly", job_key)
    os.makedirs(hourly_dir, exist_ok=True)

    processed_hours = 0
    for hour_idx, url in enumerate(urls, start=1):
        ts = url.split("_")[-1].replace(".parquet", "")
        hourly_output = os.path.join(hourly_dir, f"{ts}.parquet")
        if resume and url in completed_urls and os.path.exists(hourly_output):
            processed_hours += 1
            print(f"[off_chain {slug} hour {hour_idx}/{len(urls)}] skip checkpointed {ts}")
            continue

        hour_started_at = time.time()
        print(f"[off_chain {slug} hour {hour_idx}/{len(urls)}] start {ts}")
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_process_hour, conn, url, token_ids, hourly_output)
            heartbeat_seconds = 15
            while True:
                try:
                    rows = future.result(timeout=heartbeat_seconds)
                    break
                except FutureTimeoutError:
                    elapsed_heartbeat = time.time() - hour_started_at
                    print(
                        f"[off_chain {slug} hour {hour_idx}/{len(urls)}] "
                        f"running {ts} elapsed={elapsed_heartbeat:.1f}s"
                    )
        elapsed = time.time() - hour_started_at
        processed_hours += 1
        completed_urls[url] = rows
        checkpoint["completed_urls"] = completed_urls
        checkpoint["job_status"] = "running"
        checkpoint["updated_at_epoch"] = time.time()
        _save_checkpoint(checkpoint_path, checkpoint)
        remaining = len(urls) - processed_hours
        print(
            f"[off_chain {slug} hour {hour_idx}/{len(urls)}] "
            f"done {ts} rows={rows} elapsed={elapsed:.1f}s remaining={remaining}"
        )

    hourly_paths = [
        os.path.join(hourly_dir, f"{u.split('_')[-1].replace('.parquet', '')}.parquet")
        for u in urls
        if os.path.exists(
            os.path.join(hourly_dir, f"{u.split('_')[-1].replace('.parquet', '')}.parquet")
        )
    ]
    rows_written = _combine_hourly_outputs(conn, hourly_paths, output_path)
    checkpoint["job_status"] = "completed"
    checkpoint["final_output_path"] = output_path
    checkpoint["final_rows_written"] = rows_written
    checkpoint["updated_at_epoch"] = time.time()
    _save_checkpoint(checkpoint_path, checkpoint)

    job_elapsed = time.time() - job_started_at
    print(
        f"[off_chain {slug}] completed rows={rows_written} "
        f"elapsed={job_elapsed:.1f}s output={output_path}"
    )
    return output_path
