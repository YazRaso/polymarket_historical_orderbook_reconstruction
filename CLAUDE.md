# Informed Market Research — Developer Guide

## Purpose

This pipeline reconstructs historical Polymarket market data for use in a backtesting research article. The two outputs per market are:

- **Off-chain**: hourly CLOB orderbook snapshots filtered to specific token IDs (from pmxt parquet files via DuckDB)
- **On-chain**: hourly open interest (OI) in USDC, derived from `PositionSplit` / `PositionsMerge` events on the Polygon CTF contract via Alchemy

The user specifies markets via a CSV file. All outputs land in a versioned run directory.

---

## Architecture

```
create_datasets_from_csv.py  (parent orchestrator)
    reads CSV rows: slug, start_date, end_date
    calls Gamma API once per slug → token_ids + conditionId per market
    for each slug:
        asyncio.gather(
            asyncio.to_thread(off_chain_agent.run, token_ids, ...),
            on_chain_agent.run(conditionId, ...)  ×  N markets
        )
    writes manifest.json tracking slug-level completion

off_chain_agent.py     sync, DuckDB — filters pmxt hourly parquets by token ID
on_chain_agent.py      async, thin wrapper over onchain.run_market

scripts/onchain.py     async — orchestrates 7-step OI computation per market
scripts/oi.py          pure computation — decodes events, builds OI step function
scripts/utils/
    eth_logs.py        eth_getLogs wrapper with binary chunking + disk cache
    block_timestamp.py block ↔ timestamp conversion via binary search
    data_structures.py RateLimiter (token bucket semaphore)
    checkpoint.py      atomic JSON checkpoint r/w with per-path thread locks
    disk_cache.py      atomic JSON disk cache
```

### Key invariant

The Gamma API is called **exactly once per slug** in the parent. Neither sibling ever calls it. The parent resolves `token_ids` (for off-chain) and per-market `conditionId` (for on-chain) from the same response, then hands those downstream.

---

## Data Flow

```
CSV row (slug, start_date, end_date)
  │
  ├─ Gamma API → token_ids (flat across all markets), conditionId per market
  │
  ├─ off_chain_agent.run(token_ids, ...)
  │     for each hour in range:
  │       download pmxt parquet from r2v2.pmxt.dev
  │       filter rows WHERE asset_id IN token_ids (DuckDB)
  │       write hourly parquet
  │     merge hourly parquets → data/run<id>/<slug>_<start>_<end>.parquet
  │
  └─ on_chain_agent.run(conditionId, ...)  per market
        timestamp_to_block(start_date) → from_block
        timestamp_to_block(end_date)   → to_block
        fetch_events(conditionId, from_block, to_block) → split_events, merge_events
        compute_hourly_oi(events, hour_boundaries) → tuple[float, ...]
        write → data/run<id>/<conditionId>_<start>_<end>_onchain.parquet
```

---

## On-chain OI Methodology

OI is computed from raw `PositionSplit` / `PositionsMerge` events on the Polymarket CTF contract (`0x4D97DCd97eC945f40cF65F87097ACe5EA0476045`) on Polygon.

- Split → +amount (OI increases)
- Merge → -amount (OI decreases)
- Events are ABI-decoded manually: `amount` lives at bytes 64–95 of the `data` field; the `partition` array length is at bytes 96–127
- OI is sampled at each UTC hour boundary using a step function (carry-forward between events)
- Raw units are USDC with 6 decimal places; output divides by 1e6

---

## Directory Layout

```
data/run<uuid>/
    manifest.json                        slug-level completion tracking
    checkpoints/<conditionId>.json       per-market on-chain checkpoint
    cache/<key>.json                     Alchemy response disk cache
    <conditionId>_<start>_<end>_onchain.parquet
    hourly/<job_key>/<YYYY-MM-DDTHH>.parquet   (off-chain intermediates)
    <slug>_<start>_<end>.parquet         merged off-chain output
```

---

## Environment Variables

| Variable | Required by | Description |
|---|---|---|
| `ALCHEMY_POLYGON_URL` | `eth_logs.py`, `block_timestamp.py` | Alchemy Polygon RPC URL (JSON-RPC endpoint) |

No other external configuration is needed. The Gamma API is public (`gamma-api.polymarket.com`).

---

## Input CSV Format

```csv
slug,start_date,end_date
will-trump-win-2024,2024-10-01T00,2024-11-05T23
```

- `slug`: Polymarket event slug (used to hit Gamma API)
- `start_date` / `end_date`: UTC timestamps in `YYYY-MM-DDTHH` format (inclusive)

---

## Running the Pipeline

```bash
# fresh run
python scripts/create_datasets_from_csv.py --csv-path jobs.csv

# resume a run that was interrupted
python scripts/create_datasets_from_csv.py --csv-path jobs.csv --run-id <uuid>

# reprocess everything, ignore checkpoints
python scripts/create_datasets_from_csv.py --csv-path jobs.csv --no-resume

# cap Alchemy CU spend (default 500 CU/s)
python scripts/create_datasets_from_csv.py --csv-path jobs.csv --cu-per-second 200
```

---

## Resume / Checkpoint Policy

There are three layers of resumability:

| Layer | File | Scope | When skipped |
|---|---|---|---|
| Slug | `manifest.json` | Both siblings for a slug | Both siblings completed |
| Market (on-chain) | `checkpoints/<conditionId>.json` | One `run_market` call | `status == "completed"` |
| Hour (off-chain) | `checkpoints/<job_key>.json` | One hourly parquet fetch | URL in `completed_urls` |

A failure in either sibling leaves the slug absent from `manifest.json`, so the next run retries both. The on-chain checkpoint is only written **after** the parquet write succeeds, ensuring atomicity.

---

## Concurrency Model

- Slugs are processed **sequentially** (bounds resource use)
- Within a slug, off-chain and on-chain run **concurrently** via `asyncio.gather`
- The off-chain agent is synchronous (DuckDB); it runs in `asyncio.to_thread`
- Multiple markets under the same slug each get their own `on_chain_agent.run` coroutine, all gathered together
- Alchemy calls are serialized through `RateLimiter` (token bucket + `threading.Semaphore(max_workers=3)`)

---

## Alchemy Rate Limiter (`RateLimiter`)

`scripts/utils/data_structures.py`

Token bucket that refills to `bucket_size` every `per` seconds. Wraps a `threading.Semaphore(max_workers)` to bound concurrent in-flight calls.

```python
RateLimiter(rate=500, per=1.0, bucket_size=500, max_workers=3)
```

CU costs:
- `eth_getLogs`: 75 CU per call
- `eth_getBlockByNumber`: 16 CU per call

The rate limiter is acquired **per individual HTTP request**, including recursive chunked calls inside `_fetch_logs_range`.

---

## Key Contracts / Event Topics

| Name | Value |
|---|---|
| CTF contract | `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` |
| PositionSplit topic0 | `0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298` |
| PositionsMerge topic0 | `0x6f13ca62553fcc2bcd2372180a43949c1e4cebba603901ede2f4e14f36b282ca` |
| parentCollectionId | `0x000...0` (zero bytes32, always root) |

---

## Testing

```bash
pytest                    # run all tests
pytest tests/test_oi.py   # single file
```

Test guidelines (from `agents/tasks/`):
- Unit test logic, not implementation or third-party libs
- Mock Alchemy / Gamma API calls; do not hit them in tests
- 98 tests currently pass (off-chain, on-chain, orchestration, utils)

Test files:
- `tests/test_oi.py` — OI computation logic
- `tests/test_onchain.py` — `run_market` orchestrator
- `tests/test_off_chain_agent.py` — off-chain agent
- `tests/test_on_chain_agent.py` — on-chain sibling wrapper
- `tests/test_orchestration.py` — parent CSV orchestrator
- `tests/utils/test_block_timestamp.py` — block/timestamp utilities

---

## Data Sources

| Source | URL pattern |
|---|---|
| pmxt hourly orderbook snapshots | `https://r2v2.pmxt.dev/polymarket_orderbook_YYYY-MM-DDTHH.parquet` |
| Gamma event metadata | `https://gamma-api.polymarket.com/events/slug/{slug}` |
| Alchemy Polygon RPC | `$ALCHEMY_POLYGON_URL` |
