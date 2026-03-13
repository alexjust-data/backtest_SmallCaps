from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests

BASE_URL = "https://api.polygon.io"
DEFAULT_INPUT = Path(
    r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026_upper.parquet"
)
DEFAULT_OUTDIR = Path(r"D:\reference")
EXCHANGES = ["XNAS", "XNYS", "XASE", "ARCX"]
_THREAD_LOCAL = threading.local()

PER_TICKER_DATASETS = {"overview", "events", "splits", "dividends"}
CATALOG_DATASETS = {"ticker_types", "exchanges"}
UNIVERSE_DATASETS = {"all_tickers"}
ALL_DATASETS = PER_TICKER_DATASETS | CATALOG_DATASETS | UNIVERSE_DATASETS


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def get_session(api_key: str) -> requests.Session:
    sess = getattr(_THREAD_LOCAL, "session", None)
    if sess is None:
        sess = requests.Session()
        sess.headers.update({"Authorization": f"Bearer {api_key}"})
        _THREAD_LOCAL.session = sess
    return sess


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def get_with_retry(
    session: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 45,
    retries: int = 6,
) -> requests.Response:
    backoff = 1.8
    last_err: Optional[Exception] = None
    for i in range(retries):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                try:
                    sleep_s = float(retry_after) if retry_after else backoff**i
                except ValueError:
                    sleep_s = backoff**i
                time.sleep(max(0.0, sleep_s))
                continue
            return resp
        except Exception as exc:
            last_err = exc
            time.sleep(backoff**i)
    raise RuntimeError(f"request_failed: {url}") from last_err


def parse_csv_arg(raw: str) -> List[str]:
    out = []
    for item in str(raw).split(","):
        value = item.strip()
        if value:
            out.append(value)
    return out


def month_ends(start: dt.date, end: dt.date) -> List[dt.date]:
    out: List[dt.date] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        next_first = dt.date(y + 1, 1, 1) if m == 12 else dt.date(y, m + 1, 1)
        last_day = next_first - dt.timedelta(days=1)
        if last_day < start:
            pass
        elif last_day > end:
            out.append(end)
        else:
            out.append(last_day)
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return sorted(set(out))


def date_cuts(start: dt.date, end: dt.date, frequency: str) -> List[dt.date]:
    if frequency == "daily":
        out = []
        cur = start
        while cur <= end:
            out.append(cur)
            cur += dt.timedelta(days=1)
        return out
    if frequency == "weekly":
        out = []
        cur = start
        while cur <= end:
            if cur.weekday() == 4:
                out.append(cur)
            cur += dt.timedelta(days=1)
        if not out or out[-1] != end:
            out.append(end)
        return sorted(set(out))
    if frequency == "month_end":
        return month_ends(start, end)
    raise ValueError(f"Unsupported frequency: {frequency}")


def load_input_tickers(input_path: Path) -> pd.DataFrame:
    d = pd.read_parquet(input_path)
    if "ticker" not in d.columns:
        raise ValueError(f"Input sin columna ticker: {input_path}")
    d = d.copy()
    d["ticker"] = d["ticker"].astype("string").str.strip().str.upper()
    d = d.dropna(subset=["ticker"])
    d = d[d["ticker"] != ""]
    if "snapshot_date" in d.columns:
        d["snapshot_date"] = pd.to_datetime(d["snapshot_date"], errors="coerce").dt.date
    else:
        d["snapshot_date"] = pd.NaT
    cols = [c for c in ["ticker", "snapshot_date", "entity_id", "primary_exchange", "status"] if c in d.columns]
    d = d[cols].drop_duplicates(subset=["ticker"], keep="first").sort_values("ticker").reset_index(drop=True)
    return d


def flatten_result_row(result: Dict[str, Any], *, ticker: str, request_date: Optional[str], dataset: str) -> pd.DataFrame:
    payload = pd.json_normalize(result, sep=".")
    if payload.empty:
        payload = pd.DataFrame([{}])
    payload["ticker"] = ticker
    payload["request_date"] = request_date
    payload["_dataset"] = dataset
    payload["_ingested_utc"] = utc_now()
    return payload


def save_dataset_frame(path: Path, df: pd.DataFrame, *, expected_ticker: Optional[str] = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    if out.empty:
        out = pd.DataFrame([{"ticker": expected_ticker, "_empty": True, "_ingested_utc": utc_now()}])
        if "_dataset" not in out.columns:
            out["_dataset"] = path.stem.split("_", 1)[0]
    out.to_parquet(path, index=False)


def fetch_paginated_results(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    *,
    timeout: int,
    max_pages: int,
) -> tuple[List[Dict[str, Any]], int, str, int]:
    pages = 0
    results: List[Dict[str, Any]] = []
    while url and pages < max_pages:
        resp = get_with_retry(session, url, params=params, timeout=timeout)
        pages += 1
        if resp.status_code != 200:
            return results, resp.status_code, f"http_{resp.status_code}", pages
        payload = resp.json()
        chunk = payload.get("results", []) if isinstance(payload, dict) else []
        if isinstance(chunk, dict):
            chunk = [chunk]
        if isinstance(chunk, list):
            results.extend([x for x in chunk if isinstance(x, dict)])
        next_url = payload.get("next_url") if isinstance(payload, dict) else None
        if not next_url:
            break
        url = next_url
        params = {"apiKey": params["apiKey"]}
    if pages >= max_pages:
        return results, 206, f"hit_page_cap:{max_pages}", pages
    return results, 200, "ok", pages


def fetch_overview(session: requests.Session, api_key: str, ticker: str, request_date: Optional[str], timeout: int) -> tuple[pd.DataFrame, int, str, int]:
    url = f"{BASE_URL}/v3/reference/tickers/{ticker}"
    params: Dict[str, Any] = {"apiKey": api_key}
    if request_date:
        params["date"] = request_date
    resp = get_with_retry(session, url, params=params, timeout=timeout)
    if resp.status_code != 200:
        return flatten_result_row({}, ticker=ticker, request_date=request_date, dataset="overview"), resp.status_code, f"http_{resp.status_code}", 1
    payload = resp.json()
    result = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(result, dict):
        result = {}
    return flatten_result_row(result, ticker=ticker, request_date=request_date, dataset="overview"), 200, "ok", 1


def fetch_events(session: requests.Session, api_key: str, ticker: str, timeout: int, max_pages: int) -> tuple[pd.DataFrame, int, str, int]:
    url = f"{BASE_URL}/vX/reference/tickers/{ticker}/events"
    params = {"apiKey": api_key, "limit": 1000}
    rows, status, msg, pages = fetch_paginated_results(session, url, params, timeout=timeout, max_pages=max_pages)
    if rows:
        df = pd.json_normalize(rows, sep=".")
    else:
        df = pd.DataFrame([{}])
    df["ticker"] = ticker
    df["_dataset"] = "events"
    df["_ingested_utc"] = utc_now()
    return df, status, msg, pages


def fetch_ticker_filtered_endpoint(
    session: requests.Session,
    api_key: str,
    ticker: str,
    dataset: str,
    timeout: int,
    max_pages: int,
) -> tuple[pd.DataFrame, int, str, int]:
    if dataset == "splits":
        path = "/v3/reference/splits"
        sort_field = "execution_date"
    elif dataset == "dividends":
        path = "/v3/reference/dividends"
        sort_field = "ex_dividend_date"
    else:
        raise ValueError(dataset)
    url = f"{BASE_URL}{path}"
    params = {"apiKey": api_key, "ticker": ticker, "limit": 1000, "sort": sort_field, "order": "asc"}
    rows, status, msg, pages = fetch_paginated_results(session, url, params, timeout=timeout, max_pages=max_pages)
    if rows:
        df = pd.json_normalize(rows, sep=".")
    else:
        df = pd.DataFrame([{}])
    df["ticker"] = ticker
    df["_dataset"] = dataset
    df["_ingested_utc"] = utc_now()
    return df, status, msg, pages


def classify_dataset_status(dataset: str, http_status: int, msg: str) -> tuple[str, str]:
    if dataset == "events" and http_status == 404:
        return "ok", "no_events"
    if http_status in (200, 206):
        return "ok", msg
    return "error", msg


def is_valid_ticker_file(path: Path, expected_ticker: str) -> bool:
    if not path.exists():
        return False
    try:
        df = pd.read_parquet(path, columns=["ticker"])
    except Exception:
        return False
    if "ticker" not in df.columns:
        return False
    vals = df["ticker"].astype("string").str.strip().dropna().str.upper().unique().tolist()
    return len(vals) == 1 and vals[0] == expected_ticker.upper()


def worker_ticker(
    ticker: str,
    request_date: Optional[str],
    datasets: List[str],
    api_key: str,
    outdir: Path,
    timeout: int,
    max_pages: int,
    resume: bool,
    resume_validate: bool,
) -> List[Dict[str, Any]]:
    sess = get_session(api_key)
    audits: List[Dict[str, Any]] = []

    for dataset in datasets:
        ds_dir = outdir / dataset / f"ticker={ticker}"
        suffix = f"_{request_date}" if request_date and dataset == "overview" else ""
        out_file = ds_dir / f"{dataset}_{ticker}{suffix}.parquet"

        if resume and out_file.exists():
            if not resume_validate or is_valid_ticker_file(out_file, ticker):
                audits.append(
                    {
                        "ticker": ticker,
                        "dataset": dataset,
                        "request_date": request_date,
                        "http_status": 200,
                        "pages": 0,
                        "rows_saved": -1,
                        "status": "resume-skip",
                        "msg": "existing_valid_file",
                        "out_file": str(out_file),
                        "ts_utc": utc_now(),
                    }
                )
                continue

        if dataset == "overview":
            df, http_status, msg, pages = fetch_overview(sess, api_key, ticker, request_date, timeout)
        elif dataset == "events":
            df, http_status, msg, pages = fetch_events(sess, api_key, ticker, timeout, max_pages)
        else:
            df, http_status, msg, pages = fetch_ticker_filtered_endpoint(sess, api_key, ticker, dataset, timeout, max_pages)

        save_dataset_frame(out_file, df, expected_ticker=ticker)
        status_label, status_msg = classify_dataset_status(dataset, http_status, msg)
        audits.append(
            {
                "ticker": ticker,
                "dataset": dataset,
                "request_date": request_date,
                "http_status": http_status,
                "pages": pages,
                "rows_saved": int(max(0, len(df) - int("_empty" in df.columns and bool(df["_empty"].fillna(False).any())))),
                "status": status_label,
                "msg": status_msg,
                "out_file": str(out_file),
                "ts_utc": utc_now(),
            }
        )
    return audits


def fetch_catalog(session: requests.Session, api_key: str, dataset: str, timeout: int) -> pd.DataFrame:
    if dataset == "ticker_types":
        url = f"{BASE_URL}/v3/reference/tickers/types"
        params = {"apiKey": api_key}
    elif dataset == "exchanges":
        url = f"{BASE_URL}/v3/reference/exchanges"
        params = {"apiKey": api_key, "asset_class": "stocks", "locale": "us"}
    else:
        raise ValueError(dataset)
    resp = get_with_retry(session, url, params=params, timeout=timeout)
    payload = resp.json() if resp.status_code == 200 else {}
    results = payload.get("results", []) if isinstance(payload, dict) else []
    if isinstance(results, dict):
        results = [results]
    df = pd.json_normalize(results, sep=".") if results else pd.DataFrame([{}])
    df["_dataset"] = dataset
    df["_ingested_utc"] = utc_now()
    return df


def fetch_all_tickers_for_date(
    session: requests.Session,
    api_key: str,
    snapshot_date: str,
    exchanges: Iterable[str],
    active_filter: str,
    timeout: int,
    max_pages: int,
) -> tuple[pd.DataFrame, int, str, int]:
    frames: List[pd.DataFrame] = []
    total_pages = 0
    statuses: List[int] = []
    for exchange in exchanges:
        url = f"{BASE_URL}/v3/reference/tickers"
        params: Dict[str, Any] = {
            "apiKey": api_key,
            "market": "stocks",
            "locale": "us",
            "type": "CS",
            "exchange": exchange,
            "date": snapshot_date,
            "limit": 1000,
            "sort": "ticker",
            "order": "asc",
        }
        if active_filter == "true":
            params["active"] = "true"
        elif active_filter == "false":
            params["active"] = "false"
        rows, status, msg, pages = fetch_paginated_results(session, url, params, timeout=timeout, max_pages=max_pages)
        total_pages += pages
        statuses.append(status)
        if rows:
            df = pd.json_normalize(rows, sep=".")
            df["snapshot_date"] = snapshot_date
            df["_exchange_filter"] = exchange
            frames.append(df)
    if frames:
        out = pd.concat(frames, ignore_index=True)
        out["_dataset"] = "all_tickers"
        out["_ingested_utc"] = utc_now()
        return out, max(statuses), "ok", total_pages
    return pd.DataFrame([{"snapshot_date": snapshot_date, "_dataset": "all_tickers", "_empty": True, "_ingested_utc": utc_now()}]), max(statuses or [424]), "empty", total_pages


def unique_input_dates(df: pd.DataFrame, start: dt.date, end: dt.date) -> List[dt.date]:
    if "snapshot_date" not in df.columns:
        return []
    out = sorted({x for x in df["snapshot_date"].dropna().tolist() if start <= x <= end})
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Download Polygon Reference / Universe data for a ticker universe")
    ap.add_argument("--input", default=str(DEFAULT_INPUT))
    ap.add_argument("--outdir", default=str(DEFAULT_OUTDIR))
    ap.add_argument(
        "--datasets",
        default="overview,events,splits,dividends,ticker_types,exchanges,all_tickers",
        help="CSV: overview,events,splits,dividends,ticker_types,exchanges,all_tickers",
    )
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--batch-size", type=int, default=200)
    ap.add_argument("--timeout", type=int, default=45)
    ap.add_argument("--max-pages", type=int, default=100)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--resume-validate", action="store_true")
    ap.add_argument("--max-rows", type=int, default=0)
    ap.add_argument("--progress-every", type=int, default=25)
    ap.add_argument("--active-filter", choices=["all", "true", "false"], default="all")
    ap.add_argument("--overview-date-mode", choices=["snapshot_date", "none"], default="snapshot_date")
    ap.add_argument(
        "--all-tickers-date-mode",
        choices=["none", "unique_input_snapshot_dates", "daily", "weekly", "month_end"],
        default="unique_input_snapshot_dates",
    )
    ap.add_argument("--start", default="2005-01-01")
    ap.add_argument("--end", default="2026-12-31")
    ap.add_argument("--max-cuts", type=int, default=0)
    ap.add_argument("--api-key", default=os.getenv("POLYGON_API_KEY", ""))
    args = ap.parse_args()

    if not args.api_key:
        raise SystemExit("Set POLYGON_API_KEY or pass --api-key")

    datasets = parse_csv_arg(args.datasets)
    unknown = sorted(set(datasets) - ALL_DATASETS)
    if unknown:
        raise SystemExit(f"Unknown datasets: {unknown}")

    input_path = Path(args.input)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    run_dir = outdir / "_run"
    run_dir.mkdir(parents=True, exist_ok=True)

    progress_path = run_dir / "download_reference_universe_polygon.progress.json"
    audit_csv = run_dir / "download_reference_universe_polygon.audit.csv"
    errors_csv = run_dir / "download_reference_universe_polygon.errors.csv"

    universe = load_input_tickers(input_path)
    if args.max_rows and args.max_rows > 0:
        universe = universe.head(args.max_rows).copy()

    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)

    audits: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    with requests.Session() as sess:
        sess.headers.update({"Authorization": f"Bearer {args.api_key}"})

        for dataset in sorted(CATALOG_DATASETS & set(datasets)):
            out_file = outdir / dataset / f"{dataset}.parquet"
            if args.resume and out_file.exists():
                audits.append(
                    {
                        "ticker": None,
                        "dataset": dataset,
                        "request_date": None,
                        "http_status": 200,
                        "pages": 0,
                        "rows_saved": -1,
                        "status": "resume-skip",
                        "msg": "existing_file",
                        "out_file": str(out_file),
                        "ts_utc": utc_now(),
                    }
                )
            else:
                df = fetch_catalog(sess, args.api_key, dataset, args.timeout)
                save_dataset_frame(out_file, df)
                audits.append(
                    {
                        "ticker": None,
                        "dataset": dataset,
                        "request_date": None,
                        "http_status": 200,
                        "pages": 1,
                        "rows_saved": int(len(df)),
                        "status": "ok",
                        "msg": "ok",
                        "out_file": str(out_file),
                        "ts_utc": utc_now(),
                    }
                )

        if "all_tickers" in datasets and args.all_tickers_date_mode != "none":
            if args.all_tickers_date_mode == "unique_input_snapshot_dates":
                cuts = unique_input_dates(universe, start, end)
            else:
                cuts = date_cuts(start, end, args.all_tickers_date_mode)
            if args.max_cuts and args.max_cuts > 0:
                cuts = cuts[: args.max_cuts]

            for i, snap in enumerate(cuts, start=1):
                date_str = snap.isoformat()
                out_file = outdir / "all_tickers" / f"snapshot_date={date_str}.parquet"
                if args.resume and out_file.exists():
                    audits.append(
                        {
                            "ticker": None,
                            "dataset": "all_tickers",
                            "request_date": date_str,
                            "http_status": 200,
                            "pages": 0,
                            "rows_saved": -1,
                            "status": "resume-skip",
                            "msg": "existing_file",
                            "out_file": str(out_file),
                            "ts_utc": utc_now(),
                        }
                    )
                    continue
                df, status, msg, pages = fetch_all_tickers_for_date(
                    sess,
                    args.api_key,
                    date_str,
                    EXCHANGES,
                    args.active_filter,
                    args.timeout,
                    args.max_pages,
                )
                save_dataset_frame(out_file, df)
                row = {
                    "ticker": None,
                    "dataset": "all_tickers",
                    "request_date": date_str,
                    "http_status": status,
                    "pages": pages,
                    "rows_saved": int(len(df)),
                    "status": "ok" if status in (200, 206) else "error",
                    "msg": msg,
                    "out_file": str(out_file),
                    "ts_utc": utc_now(),
                }
                audits.append(row)
                if row["status"] == "error":
                    errors.append(row)

        per_ticker_datasets = [d for d in datasets if d in PER_TICKER_DATASETS]
        total = len(universe)
        done = 0
        for b0 in range(0, total, args.batch_size):
            batch = universe.iloc[b0 : b0 + args.batch_size]
            with ThreadPoolExecutor(max_workers=args.workers) as ex:
                futures = {}
                for _, rec in batch.iterrows():
                    ticker = str(rec["ticker"])
                    request_date = None
                    if "overview" in per_ticker_datasets and args.overview_date_mode == "snapshot_date":
                        snap = rec.get("snapshot_date")
                        request_date = snap.isoformat() if pd.notna(snap) else None
                    futures[
                        ex.submit(
                            worker_ticker,
                            ticker,
                            request_date,
                            per_ticker_datasets,
                            args.api_key,
                            outdir,
                            args.timeout,
                            args.max_pages,
                            args.resume,
                            args.resume_validate,
                        )
                    ] = ticker

                for fut in as_completed(futures):
                    ticker = futures[fut]
                    try:
                        rows = fut.result()
                    except Exception as exc:
                        rows = [
                            {
                                "ticker": ticker,
                                "dataset": "worker",
                                "request_date": None,
                                "http_status": -1,
                                "pages": 0,
                                "rows_saved": 0,
                                "status": "error",
                                "msg": f"worker_exception:{exc}",
                                "out_file": None,
                                "ts_utc": utc_now(),
                            }
                        ]
                    done += 1
                    audits.extend(rows)
                    errors.extend([r for r in rows if r["status"] == "error"])
                    if done % max(1, args.progress_every) == 0 or done == total:
                        write_json(
                            progress_path,
                            {
                                "status": "running",
                                "input": str(input_path),
                                "outdir": str(outdir),
                                "datasets": datasets,
                                "done_tickers": done,
                                "total_tickers": total,
                                "progress_pct": round(100.0 * done / total, 2) if total else 100.0,
                                "errors": len(errors),
                                "updated_at_utc": utc_now(),
                            },
                        )
                        print(f"ticker {done}/{total} ({100.0*done/max(1,total):.2f}%) last={ticker} errors={len(errors)}")

    audits_df = pd.DataFrame(audits)
    errors_df = pd.DataFrame(errors)
    audits_df.to_csv(audit_csv, index=False)
    errors_df.to_csv(errors_csv, index=False)

    write_json(
        progress_path,
        {
            "status": "completed",
            "input": str(input_path),
            "outdir": str(outdir),
            "datasets": datasets,
            "done_tickers": int(len(universe)),
            "total_tickers": int(len(universe)),
            "progress_pct": 100.0,
            "errors": int(len(errors_df)),
            "updated_at_utc": utc_now(),
            "audit_csv": str(audit_csv),
            "errors_csv": str(errors_csv),
        },
    )

    print(f"tickers_processed={len(universe):,}")
    print(f"outdir={outdir}")
    print(f"progress={progress_path}")
    print(f"audit={audit_csv}")
    print(f"errors={errors_csv}")


if __name__ == "__main__":
    main()
