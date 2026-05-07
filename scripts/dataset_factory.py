"""Utilities for working with Polymarket hourly orderbook parquet files."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from typing import Iterable, List, Dict, Any, Optional

import duckdb
import requests

BASE_URL = "https://r2v2.pmxt.dev/polymarket_orderbook_{}.parquet"


def hourly_strings(start: str, end: str) -> Iterable[str]:
    """Yield hourly timestamp strings in format YYYY-MM-DDTHH inclusive.

    Args:
        start: Start timestamp in format YYYY-MM-DDTHH.
        end: End timestamp in format YYYY-MM-DDTHH.

    Yields:
        Hourly timestamp strings in format YYYY-MM-DDTHH.
    """
    start = start.strip().strip("\"").strip("'")
    end = end.strip().strip("\"").strip("'")
    s = dt.datetime.strptime(start, "%Y-%m-%dT%H")
    e = dt.datetime.strptime(end, "%Y-%m-%dT%H")
    while s <= e:
        yield s.strftime("%Y-%m-%dT%H")
        s += dt.timedelta(hours=1)


def parquet_url(ts: str) -> str:
    """Build the hourly parquet URL for a timestamp string.

    Args:
        ts: Hourly timestamp string in format YYYY-MM-DDTHH.

    Returns:
        The parquet URL for the given timestamp.
    """
    return BASE_URL.format(ts)

def filter_parquet_by_asset_ids(
    url: str,
    asset_ids: List[str],
    asset_id_column: str = "asset_id",
    conn: Optional[duckdb.DuckDBPyConnection] = None,
    download_dir: str = "data",
    output_dir: str = "filtered",
    output_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Filter rows to provided asset ids and write the filtered parquet locally.

    Args:
        url: Parquet URL to filter.
        asset_ids: Token or asset ids to include.
        asset_id_column: Column name containing asset ids.
        conn: Optional DuckDB connection.
        download_dir: Directory for optional downloads.
        output_dir: Directory for filtered parquet outputs.
        output_path: Optional explicit output file path.

    Returns:
        Metadata about the filtered output including path and rows_written.

    Raises:
        RuntimeError: If the parquet cannot be filtered and written.
    """
    conn = conn or duckdb.connect()
    if not asset_ids:
        return {"url": url, "method": "skipped", "path": None, "rows_written": 0}

    placeholders = ", ".join(["?"] * len(asset_ids))
    where_sql = f"{asset_id_column} IN ({placeholders})"

    os.makedirs(output_dir, exist_ok=True)
    output_path = output_path or os.path.join(output_dir, f"filtered_{os.path.basename(url)}")

    try:
        query = (
            f"COPY (SELECT * FROM read_parquet('{url}') WHERE {where_sql}) "
            f"TO '{output_path}' (FORMAT PARQUET)"
        )
        conn.execute(query, asset_ids)
        count = conn.execute(
            f"SELECT COUNT(*) AS n FROM read_parquet('{output_path}')"
        ).fetchone()[0]
        return {"url": url, "method": "read_parquet", "path": output_path, "rows_written": count}
    except Exception:
        raise RuntimeError(f"Failed to filter parquet for url: {url} with asset_ids: {asset_ids}")


def urls_for_range(start: str, end: str) -> List[str]:
    """Build URL list for a time range.

    Args:
        start: Start timestamp in format YYYY-MM-DDTHH.
        end: End timestamp in format YYYY-MM-DDTHH.

    Returns:
        List of hourly parquet URLs.
    """
    return [parquet_url(ts) for ts in hourly_strings(start, end)]


def download_filtered_parquets(
    token_ids: Optional[List[str]],
    slug: str,
    start: str,
    end: str,
    asset_id_column: str = "asset_id",
    output_base_dir: str = "filtered",
) -> List[Dict[str, Any]]:
    """Filter and write parquet files for each hour in a range.

    Args:
        token_ids: Token ids to filter. If None, fetch via event slug.
        slug: Event slug used for output directory naming.
        start: Start timestamp in format YYYY-MM-DDTHH.
        end: End timestamp in format YYYY-MM-DDTHH.
        asset_id_column: Column name containing asset ids.
        output_base_dir: Base directory for filtered parquet outputs.

    Returns:
        List of result metadata for each hour.
    """
    output_dir = os.path.join(output_base_dir, slug)
    results: List[Dict[str, Any]] = []
    token_ids = token_ids or _get_event_token_ids(slug)
    for url in urls_for_range(start, end):
        result = filter_parquet_by_asset_ids(
            url,
            token_ids,
            asset_id_column=asset_id_column,
            output_dir=output_dir,
        )
        results.append(result)
    return results


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entrypoint for filtering Polymarket orderbooks.

    Args:
        argv: Optional argument list for testing.

    Returns:
        Process exit code.
    """
    parser = argparse.ArgumentParser(description="Filter Polymarket orderbooks by token ids")
    parser.add_argument("--token-ids", required=True, help="Comma-separated list of token ids")
    parser.add_argument("--slug", required=True, help="Slug used for output directory naming")
    parser.add_argument("--start", required=True, help="Start timestamp YYYY-MM-DDTHH")
    parser.add_argument("--end", required=True, help="End timestamp YYYY-MM-DDTHH")
    parser.add_argument("--asset-id-column", default="asset_id", help="Asset id column name")
    parser.add_argument("--output-base-dir", default="filtered", help="Base output directory")

    args = parser.parse_args(argv)
    token_ids = [t.strip() for t in args.token_ids.split(",") if t.strip()]

    if not token_ids:
        raise ValueError("token-ids must contain at least one id")

    results = download_filtered_parquets(
        token_ids=token_ids,
        slug=args.slug,
        start=args.start,
        end=args.end,
        asset_id_column=args.asset_id_column,
        output_base_dir=args.output_base_dir,
    )

    total_rows = sum(r.get("rows_written", 0) for r in results)
    print(f"Wrote {len(results)} parquet files with {total_rows} rows total")
    return 0


def _get_event_token_ids(slug: str) -> List[str]:
    """Fetch token ids for a Polymarket event slug.

    Args:
        slug: Event slug identifier for the Polymarket event.

    Returns:
        List of token ids for markets under the event.

    Raises:
        ValueError: If no markets are found for the given event slug.
        ValueError: If the API request fails.
    """
    token_ids: List[str] = []
    response = requests.get(f"https://gamma-api.polymarket.com/events/slug/{slug}")
    if not response.ok:
        raise ValueError(
            f"Failed to fetch event data for slug: {slug}, status code: {response.status_code}"
        )
    data = response.json()
    markets = data.get("markets", [])
    if not markets:
        raise ValueError(f"No markets found for event slug: {slug}")
    for market in markets:
        value = market.get("clobTokenIds")
        token_ids.extend(_normalize_clob_token_ids(value))
    return token_ids


def _normalize_clob_token_ids(value: Any) -> List[str]:
    """Normalize clobTokenIds values from Gamma API into a flat list of ids."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        if raw.startswith("["):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return [str(v).strip() for v in parsed if str(v).strip()]
            except json.JSONDecodeError:
                pass
        return [raw]
    return [str(value).strip()] if str(value).strip() else []


if __name__ == "__main__":
    raise SystemExit(main())
