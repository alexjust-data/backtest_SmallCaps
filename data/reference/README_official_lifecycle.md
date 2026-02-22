# Official Ticker Lifecycle Evidence

This folder stores source-of-truth lifecycle evidence for each ticker.

## Inputs
- `official_ticker_events.csv`: one row per official event/document.

Required columns:
- `ticker`, `cik`, `event_type`, `event_date`, `source_name`, `source_doc_type`, `source_url`, `source_title`, `notes`

Allowed `event_type` values:
- `listed`, `delisted`, `renamed`, `halted`, `suspended`

## Build compiled lifecycle
```powershell
python scripts/build_official_lifecycle.py `
  --events-csv data/reference/official_ticker_events.csv `
  --out-lifecycle-csv data/reference/official_lifecycle_compiled.csv `
  --out-events-parquet data/reference/official_ticker_events.validated.parquet `
  --out-lifecycle-parquet data/reference/official_lifecycle_compiled.parquet
```

## Output usage
- The notebook `02_decisions/01_calendar_definition.ipynb` should consume `official_lifecycle_compiled.csv`.
- If a ticker is not present in compiled lifecycle, reconciliation must fail.
