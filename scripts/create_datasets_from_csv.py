"""Batch download filtered orderbook parquets from a CSV manifest."""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
import uuid
from typing import Dict, List
from concurrent.futures import TimeoutError as FutureTimeoutError
from concurrent.futures import ThreadPoolExecutor

import duckdb

import dataset_factory


def _read_jobs(csv_path: str) -> List[Dict[str, str]]:
    """Read job rows from a CSV with columns slug,start_date,end_date.

    Args:
        csv_path: Path to the CSV file.

    Returns:
        List of dicts with keys: slug, start_date, end_date.
    """
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        required = {"slug", "start_date", "end_date"}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError("CSV must include columns: slug, start_date, end_date")
        return [row for row in reader]


def _clean_field(value: str) -> str:
    return value.strip().strip("\"").strip("'")


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def _job_key(slug: str, start: str, end: str) -> str:
    return f"{slug}__{start}__{end}".replace("/", "_")


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
    csv_path: str,
    output_dir: str = "data",
    run_id: str | None = None,
    resume: bool = True,
) -> str:
    """Process jobs and write one filtered parquet per input row.

    Args:
        csv_path: Input CSV path.
        output_dir: Base output directory for run outputs.

    Returns:
        Path to the run output directory.
    """
    os.makedirs(output_dir, exist_ok=True)
    run_id = run_id or uuid.uuid4().hex
    run_dir = os.path.join(output_dir, f"run{run_id}")
    os.makedirs(run_dir, exist_ok=True)
    checkpoints_dir = os.path.join(run_dir, "checkpoints")
    os.makedirs(checkpoints_dir, exist_ok=True)
    print(f"Run directory: {run_dir}")

    conn = duckdb.connect()
    jobs = _read_jobs(csv_path)
    print(f"Loaded {len(jobs)} job(s) from {csv_path}")

    total_rows = 0
    for job_idx, job in enumerate(jobs, start=1):
        job_started_at = time.time()
        slug = _clean_field(job["slug"])
        start = _clean_field(job["start_date"])
        end = _clean_field(job["end_date"])
        print(f"[job {job_idx}/{len(jobs)}] slug={slug} range={start}..{end}")
        token_ids = dataset_factory._get_event_token_ids(slug)
        print(f"[job {job_idx}/{len(jobs)}] token_ids={len(token_ids)}")
        urls = dataset_factory.urls_for_range(start, end)
        print(f"[job {job_idx}/{len(jobs)}] hours={len(urls)}")
        output_path = os.path.join(run_dir, f"{slug}_{start}_{end}.parquet")
        job_key = _job_key(slug, start, end)
        checkpoint_path = os.path.join(checkpoints_dir, f"{job_key}.json")
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
                if processed_hours > 0:
                    elapsed_job = time.time() - job_started_at
                    avg_per_hour = elapsed_job / processed_hours
                    remaining = len(urls) - processed_hours
                    eta_seconds = int(avg_per_hour * remaining)
                    print(
                        f"[job {job_idx}/{len(jobs)} progress] processed={processed_hours}/{len(urls)} "
                        f"remaining={remaining} eta~{eta_seconds}s"
                    )
                print(
                    f"[job {job_idx}/{len(jobs)} hour {hour_idx}/{len(urls)}] "
                    f"skip checkpointed {ts} rows={completed_urls[url]}"
                )
                continue

            hour_started_at = time.time()
            print(
                f"[job {job_idx}/{len(jobs)} hour {hour_idx}/{len(urls)}] "
                f"start {ts}"
            )
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
                            f"[job {job_idx}/{len(jobs)} hour {hour_idx}/{len(urls)}] "
                            f"running {ts} elapsed={elapsed_heartbeat:.1f}s"
                        )
            elapsed = time.time() - hour_started_at
            processed_hours += 1
            completed_urls[url] = rows
            checkpoint["completed_urls"] = completed_urls
            checkpoint["job_status"] = "running"
            checkpoint["updated_at_epoch"] = time.time()
            _save_checkpoint(checkpoint_path, checkpoint)
            elapsed_job = time.time() - job_started_at
            avg_per_hour = elapsed_job / processed_hours
            remaining = len(urls) - processed_hours
            eta_seconds = int(avg_per_hour * remaining)
            print(
                f"[job {job_idx}/{len(jobs)} hour {hour_idx}/{len(urls)}] "
                f"done {ts} rows={rows} elapsed={elapsed:.1f}s "
                f"processed={processed_hours}/{len(urls)} remaining={remaining} eta~{eta_seconds}s"
            )

        hourly_paths = [
            os.path.join(hourly_dir, f"{u.split('_')[-1].replace('.parquet', '')}.parquet")
            for u in urls
            if os.path.exists(os.path.join(hourly_dir, f"{u.split('_')[-1].replace('.parquet', '')}.parquet"))
        ]
        rows_written = _combine_hourly_outputs(conn, hourly_paths, output_path)
        total_rows += rows_written
        checkpoint["job_status"] = "completed"
        checkpoint["final_output_path"] = output_path
        checkpoint["final_rows_written"] = rows_written
        checkpoint["updated_at_epoch"] = time.time()
        _save_checkpoint(checkpoint_path, checkpoint)

        job_elapsed = time.time() - job_started_at
        print(
            f"[job {job_idx}/{len(jobs)}] completed rows={rows_written} "
            f"elapsed={job_elapsed:.1f}s output={output_path}"
        )
        if rows_written == 0:
            print(
                f"Warning: 0 rows written for slug={slug}, start={start}, end={end}. "
                "Check token id parsing and date range."
            )

    print(f"Total rows written across all jobs: {total_rows}")
    return run_dir


def main() -> int:
    """CLI entrypoint for batch downloads from a CSV file."""
    parser = argparse.ArgumentParser(description="Batch download orderbooks from a CSV")
    parser.add_argument("csv_path", nargs="?", help="CSV path with columns slug,start_date,end_date")
    parser.add_argument(
        "--csv-path",
        dest="csv_path_flag",
        help="CSV path with columns slug,start_date,end_date",
    )
    parser.add_argument("--output-dir", default="data", help="Output directory for run manifest")
    parser.add_argument(
        "--run-id",
        help=(
            "Reuse a specific run id for checkpoint resume. "
            "If omitted, a new run id is created."
        ),
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore checkpoints and reprocess every hour.",
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
    )
    print(f"Wrote run outputs to {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
