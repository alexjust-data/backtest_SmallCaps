from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import requests

BASE_URL = "https://api.polygon.io"
DATASET_TO_PATH = {
    "income_statements": "/stocks/financials/v1/income-statements",
    "balance_sheets": "/stocks/financials/v1/balance-sheets",
    "cash_flow_statements": "/stocks/financials/v1/cash-flow-statements",
    "ratios": "/stocks/financials/v1/ratios",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def extract_record_tickers(rec: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    t1 = rec.get("ticker")
    if isinstance(t1, str) and t1.strip():
        out.append(t1.strip().upper())
    t2 = rec.get("tickers")
    if isinstance(t2, str) and t2.strip():
        out.append(t2.strip().upper())
    elif isinstance(t2, list):
        for x in t2:
            if isinstance(x, str) and x.strip():
                out.append(x.strip().upper())
    return sorted(set(out))


def probe_one(
    session: requests.Session,
    dataset: str,
    ticker: str,
    api_key: str,
    limit: int,
    max_pages: int,
    timeout: int,
) -> List[Dict[str, Any]]:
    endpoint = DATASET_TO_PATH[dataset]
    url = f"{BASE_URL}{endpoint}"
    params = {"tickers": ticker, "limit": limit, "apiKey": api_key}
    page = 0
    rows: List[Dict[str, Any]] = []

    while url and page < max_pages:
        page += 1
        r = session.get(url, params=params, timeout=timeout)
        record: Dict[str, Any] = {
            "ts_utc": utc_now(),
            "dataset": dataset,
            "request_ticker": ticker,
            "page": page,
            "status_code": r.status_code,
            "request_url": r.url,
            "result_count": 0,
            "distinct_record_tickers": 0,
            "mismatch_records": 0,
            "missing_ticker_records": 0,
            "sample_record_tickers": [],
            "error": None,
        }

        if r.status_code == 429:
            record["error"] = "http_429"
            rows.append(record)
            break
        if r.status_code == 404:
            record["error"] = "http_404"
            rows.append(record)
            break

        try:
            r.raise_for_status()
            j = r.json()
        except Exception as e:
            record["error"] = f"http_error:{e}"
            rows.append(record)
            break

        res = j.get("results", [])
        if isinstance(res, dict):
            res = [res]
        if not isinstance(res, list):
            res = []

        record["result_count"] = len(res)

        seen_tickers: List[str] = []
        mismatch = 0
        missing = 0
        for rec in res:
            if not isinstance(rec, dict):
                continue
            vals = extract_record_tickers(rec)
            if vals:
                seen_tickers.extend(vals)
                if ticker.upper() not in vals:
                    mismatch += 1
            else:
                missing += 1

        uniq = sorted(set(seen_tickers))
        record["distinct_record_tickers"] = len(uniq)
        record["mismatch_records"] = mismatch
        record["missing_ticker_records"] = missing
        record["sample_record_tickers"] = uniq[:20]
        rows.append(record)

        next_url = j.get("next_url")
        if next_url:
            url = next_url
            params = {"apiKey": api_key}
        else:
            url = ""

    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Probe ticker filter behavior for Polygon fundamentals endpoints")
    ap.add_argument("--tickers", default="AAPL,MSFT,NVDA,JNJ,WMT", help="CSV tickers (5 recomendado)")
    ap.add_argument("--datasets", default="income_statements,balance_sheets,cash_flow_statements,ratios")
    ap.add_argument("--outdir", default=r"D:\financial\_audit")
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--max-pages", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=45)
    ap.add_argument("--api-key", default=os.getenv("POLYGON_API_KEY", ""))
    args = ap.parse_args()

    if not args.api_key:
        raise SystemExit("Falta API key: usa --api-key o POLYGON_API_KEY")

    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]
    for d in datasets:
        if d not in DATASET_TO_PATH:
            raise SystemExit(f"dataset no soportado: {d}")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {args.api_key}"})

    all_rows: List[Dict[str, Any]] = []
    for t in tickers:
        for d in datasets:
            rows = probe_one(
                session=session,
                dataset=d,
                ticker=t,
                api_key=args.api_key,
                limit=args.limit,
                max_pages=args.max_pages,
                timeout=args.timeout,
            )
            all_rows.extend(rows)

    df = pd.DataFrame(all_rows)
    out_csv = outdir / "probe_fundamentals_ticker_filter.csv"
    out_json = outdir / "probe_fundamentals_ticker_filter.summary.json"
    df.to_csv(out_csv, index=False)

    # resumen
    if len(df):
        grp = df.groupby(["dataset", "request_ticker"], as_index=False).agg(
            pages=("page", "max"),
            status_ok_pct=("status_code", lambda s: round((s.eq(200).mean() * 100), 2)),
            total_results=("result_count", "sum"),
            mismatch_records=("mismatch_records", "sum"),
            missing_ticker_records=("missing_ticker_records", "sum"),
            max_distinct_tickers_in_page=("distinct_record_tickers", "max"),
        )
        grp["mismatch_pct"] = (grp["mismatch_records"] / grp["total_results"].replace(0, pd.NA) * 100).round(2)
        grp = grp.fillna(0)
    else:
        grp = pd.DataFrame()

    summary = {
        "tickers": tickers,
        "datasets": datasets,
        "rows": int(len(df)),
        "out_csv": str(out_csv),
        "generated_utc": utc_now(),
    }
    out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("=== PROBE SUMMARY ===")
    print(json.dumps(summary, indent=2))
    if len(grp):
        print("\n=== BY DATASET+TICKER ===")
        print(grp.to_string(index=False))


if __name__ == "__main__":
    main()
