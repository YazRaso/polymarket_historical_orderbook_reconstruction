# Informed Market Research

Pipeline to build filtered datasets from historical hourly Polymarket CLOB snapshots on pmxt.

## Quick Start

1. Install deps:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

2. Prepare CSV (`slug,start_date,end_date`)

3. Run batch dataset build:

```bash
.venv/bin/python scripts/create_datasets_from_csv.py \
  --csv-path <path to your csv file> \
```

4. Resume a failed/interrupted run:

```bash
.venv/bin/python scripts/create_datasets_from_csv.py \
  --csv-path <path to your csv file>  \
  --run-id <existing_run_uuid>
```

Outputs are written to `data/run<uuid>/`.
