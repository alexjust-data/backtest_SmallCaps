from __future__ import annotations

import argparse
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import requests

BASE_URL = "https://api.polygon.io"
DATASET_TO_PATH = {
    "income_statements": "/stocks/financials/v1/income-statements",
    "balance_sheets": "/stocks/financials/v1/balance-sheets",
    "cash_flow_statements": "/stocks/financials/v1/cash-flow-statements",
    "ratios": "/stocks/financials/v1/ratios",
}

_THREAD_LOCAL = threading.local()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_tickers(input_path: Path) -> List[str]:
    d = pd.read_parquet(input_path, columns=["ticker"])
    t = (
        d["ticker"].astype("string").str.strip().dropna().drop_duplicates().sort_values().tolist()
    )
    return [x.upper() for x in t if x]


def write_progress(progress_path: Path, payload: Dict) -> None:
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def get_session(api_key: str) -> requests.Session:
    sess = getattr(_THREAD_LOCAL, "session", None)
    if sess is None:
        sess = requests.Session()
        sess.headers.update({"Authorization": f"Bearer {api_key}"})
        _THREAD_LOCAL.session = sess
    return sess


def extract_record_tickers(rec: Dict) -> List[str]:
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


def is_existing_file_valid(path: Path, expected_ticker: str) -> bool:
    if not path.exists():
        return False

    # 1) ticker canonico
    try:
        d_t = pd.read_parquet(path, columns=["ticker"])
    except Exception:
        return False

    if "ticker" not in d_t.columns:
        return False

    vals = d_t["ticker"].astype("string").str.strip().dropna().str.upper().unique().tolist()
    if len(vals) != 1 or vals[0] != expected_ticker.upper():
        return False

    # 2) guardarrail mezcla historica: >1 CIK en un solo ticker suele indicar contaminacion
    try:
        d_cik = pd.read_parquet(path, columns=["cik"])
        if "cik" in d_cik.columns:
            nun = d_cik["cik"].astype("string").str.strip().dropna().nunique()
            if nun > 1:
                return False
    except Exception:
        pass

    return True


def fetch_all_pages(
    sess: requests.Session,
    endpoint: str,
    ticker: str,
    api_key: str,
    limit: int,
    max_pages: int,
    timeout: int,
    keep_records_without_ticker: bool,
) -> Tuple[List[Dict], int, str, int, int, int]:
    # Polygon fundamentals v1 filtra por 'tickers' (plural)
    params = {"tickers": ticker, "limit": limit, "apiKey": api_key}
    url = f"{BASE_URL}{endpoint}"

    kept: List[Dict] = []
    raw_count = 0
    dropped_mismatch = 0
    dropped_missing_ticker = 0
    pages = 0

    while url and pages < max_pages:
        r = sess.get(url, params=params, timeout=timeout)
        pages += 1

        if r.status_code == 404:
            return kept, 404, "not_found", pages, dropped_mismatch, dropped_missing_ticker

        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", "2"))
            time.sleep(max(1, retry_after))
            pages -= 1
            continue

        try:
            r.raise_for_status()
        except Exception as e:
            return kept, r.status_code, f"http_error: {e}", pages, dropped_mismatch, dropped_missing_ticker

        j = r.json()
        res = j.get("results", [])
        if isinstance(res, dict):
            res = [res]

        if isinstance(res, list):
            for rec in res:
                if not isinstance(rec, dict):
                    continue
                raw_count += 1
                rec_tickers = extract_record_tickers(rec)

                if rec_tickers:
                    if ticker.upper() in rec_tickers:
                        kept.append(rec)
                    else:
                        dropped_mismatch += 1
                else:
                    if keep_records_without_ticker:
                        kept.append(rec)
                    else:
                        dropped_missing_ticker += 1

        next_url = j.get("next_url")
        if next_url:
            url = next_url
            params = {"apiKey": api_key}
        else:
            url = ""

    if pages >= max_pages and raw_count >= limit * max_pages:
        return kept, 206, "hit_page_cap", pages, dropped_mismatch, dropped_missing_ticker

    return kept, 200, "ok", pages, dropped_mismatch, dropped_missing_ticker


def save_ticker_dataset(outdir: Path, dataset: str, ticker: str, rows: List[Dict]) -> Path:
    ds_dir = outdir / dataset / f"ticker={ticker}"
    ds_dir.mkdir(parents=True, exist_ok=True)
    out_file = ds_dir / f"{dataset}_{ticker}.parquet"

    if rows:
        df = pd.json_normalize(rows, sep=".")
    else:
        df = pd.DataFrame()

    if len(df) == 0:
        df = pd.DataFrame([{"ticker": ticker, "_empty": True}])
    else:
        # ticker canonico siempre
        df["ticker"] = ticker

    df["_dataset"] = dataset
    df["_ingested_utc"] = utc_now()
    df.to_parquet(out_file, index=False)
    return out_file


def worker_ticker(
    ticker: str,
    datasets: List[str],
    api_key: str,
    outdir: Path,
    limit: int,
    max_pages: int,
    timeout: int,
    keep_records_without_ticker: bool,
) -> List[Dict]:
    sess = get_session(api_key)
    out: List[Dict] = []

    for dataset in datasets:
        endpoint = DATASET_TO_PATH[dataset]
        rows, http_status, msg, pages, dropped_mismatch, dropped_missing_ticker = fetch_all_pages(
            sess=sess,
            endpoint=endpoint,
            ticker=ticker,
            api_key=api_key,
            limit=limit,
            max_pages=max_pages,
            timeout=timeout,
            keep_records_without_ticker=keep_records_without_ticker,
        )

        out_file = save_ticker_dataset(outdir, dataset, ticker, rows)
        out.append(
            {
                "ticker": ticker,
                "dataset": dataset,
                "http_status": http_status,
                "msg": msg,
                "rows_saved": len(rows),
                "pages": pages,
                "dropped_mismatch": dropped_mismatch,
                "dropped_missing_ticker": dropped_missing_ticker,
                "out_file": str(out_file),
                "ts_utc": utc_now(),
            }
        )

    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Download Polygon fundamentals v1 by ticker")
    ap.add_argument("--input", required=True, help="Parquet con columna ticker")
    ap.add_argument("--outdir", required=True, help="Ej: D:\\financial")
    ap.add_argument(
        "--datasets",
        default="income_statements,balance_sheets,cash_flow_statements,ratios",
        help="CSV de datasets",
    )
    ap.add_argument("--batch-size", type=int, default=200, help="Batch en tickers")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--limit", type=int, default=1000)
    ap.add_argument("--max-pages", type=int, default=50)
    ap.add_argument("--timeout", type=int, default=45)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--resume-validate", action="store_true", help="Revalida parquet existente por ticker")
    ap.add_argument("--max-rows", type=int, default=0, help="0=todos")
    ap.add_argument("--progress-every", type=int, default=25, help="Escritura de progreso cada N tickers")
    ap.add_argument("--keep-records-without-ticker", action="store_true", help="Permite filas sin ticker/tickers")
    ap.add_argument("--api-key", default=os.getenv("POLYGON_API_KEY", ""))
    args = ap.parse_args()

    if not args.api_key:
        raise SystemExit("Falta API key: usa --api-key o variable POLYGON_API_KEY")

    input_path = Path(args.input)
    outdir = Path(args.outdir)
    run_dir = outdir / "_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    progress_path = run_dir / "download_fundamentals_v1.progress.json"
    errors_path = run_dir / "download_fundamentals_v1.errors.csv"

    datasets = [x.strip() for x in args.datasets.split(",") if x.strip()]
    bad = [x for x in datasets if x not in DATASET_TO_PATH]
    if bad:
        raise SystemExit(f"Datasets no soportados: {bad}")

    tickers = load_tickers(input_path)
    if args.max_rows and args.max_rows > 0:
        tickers = tickers[: args.max_rows]

    total_tickers = len(tickers)
    if total_tickers == 0:
        raise SystemExit("No hay tickers en input")

    to_process: List[str] = []
    skipped_valid = 0
    for tkr in tickers:
        if args.resume:
            all_ok = True
            for ds in datasets:
                out_file = outdir / ds / f"ticker={tkr}" / f"{ds}_{tkr}.parquet"
                if not out_file.exists():
                    all_ok = False
                    break
                if args.resume_validate and (not is_existing_file_valid(out_file, tkr)):
                    all_ok = False
                    break
            if all_ok:
                skipped_valid += 1
                continue
        to_process.append(tkr)

    if len(to_process) == 0:
        write_progress(
            progress_path,
            {
                "status": "completed",
                "updated_at_utc": utc_now(),
                "input": str(input_path),
                "outdir": str(outdir),
                "datasets": datasets,
                "done_tickers": 0,
                "total_tickers": 0,
                "progress_pct": 100.0,
                "resume": bool(args.resume),
                "resume_validate": bool(args.resume_validate),
                "skipped_valid": skipped_valid,
                "note": "nothing_to_do",
            },
        )
        print("Nada que hacer (resume completo).")
        return

    done_tickers = 0
    errors: List[Dict] = []
    start_ts = time.time()

    n_batches = (len(to_process) + args.batch_size - 1) // args.batch_size
    for bi in range(n_batches):
        lo = bi * args.batch_size
        hi = min(len(to_process), (bi + 1) * args.batch_size)
        batch_tickers = to_process[lo:hi]

        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
            futs = {
                ex.submit(
                    worker_ticker,
                    tkr,
                    datasets,
                    args.api_key,
                    outdir,
                    args.limit,
                    args.max_pages,
                    args.timeout,
                    bool(args.keep_records_without_ticker),
                ): tkr
                for tkr in batch_tickers
            }

            for fut in as_completed(futs):
                tkr = futs[fut]
                try:
                    results = fut.result()
                except Exception as e:
                    results = [
                        {
                            "ticker": tkr,
                            "dataset": "*",
                            "http_status": 0,
                            "msg": f"worker_exception: {e}",
                            "rows_saved": 0,
                            "pages": 0,
                            "dropped_mismatch": 0,
                            "dropped_missing_ticker": 0,
                            "out_file": "",
                            "ts_utc": utc_now(),
                        }
                    ]

                done_tickers += 1
                for res in results:
                    if res["http_status"] not in (200, 404):
                        errors.append(res)
                    if res.get("dropped_mismatch", 0) > 0 or res.get("dropped_missing_ticker", 0) > 0:
                        errors.append({**res, "msg": f"{res['msg']} | dropped_mismatch={res['dropped_mismatch']} dropped_missing_ticker={res['dropped_missing_ticker']}"})

                elapsed = time.time() - start_ts
                rate = done_tickers / elapsed if elapsed > 0 else 0.0
                eta = int((len(to_process) - done_tickers) / rate) if rate > 0 else 0

                if done_tickers % 10 == 0 or done_tickers == len(to_process):
                    last = results[-1] if results else {"dataset": "-", "rows_saved": 0, "http_status": 0}
                    print(
                        f"ticker {done_tickers}/{len(to_process)} ({done_tickers/len(to_process)*100:.2f}%) "
                        f"last={last['dataset']}:{tkr} rows={last['rows_saved']} status={last['http_status']} eta={eta}s",
                        flush=True,
                    )

                if done_tickers % max(1, args.progress_every) == 0 or done_tickers == len(to_process):
                    write_progress(
                        progress_path,
                        {
                            "status": "running",
                            "updated_at_utc": utc_now(),
                            "input": str(input_path),
                            "outdir": str(outdir),
                            "datasets": datasets,
                            "batch_index": bi + 1,
                            "batch_total": n_batches,
                            "done_tickers": done_tickers,
                            "total_tickers": len(to_process),
                            "progress_pct": round(done_tickers / len(to_process) * 100, 2),
                            "resume": bool(args.resume),
                            "resume_validate": bool(args.resume_validate),
                            "skipped_valid": skipped_valid,
                            "errors_so_far": len(errors),
                            "workers": args.workers,
                            "limit": args.limit,
                            "max_pages": args.max_pages,
                            "keep_records_without_ticker": bool(args.keep_records_without_ticker),
                        },
                    )

        if errors:
            pd.DataFrame(errors).to_csv(errors_path, index=False)

    write_progress(
        progress_path,
        {
            "status": "completed",
            "updated_at_utc": utc_now(),
            "input": str(input_path),
            "outdir": str(outdir),
            "datasets": datasets,
            "done_tickers": done_tickers,
            "total_tickers": len(to_process),
            "progress_pct": 100.0,
            "resume": bool(args.resume),
            "resume_validate": bool(args.resume_validate),
            "skipped_valid": skipped_valid,
            "errors": len(errors),
            "workers": args.workers,
            "limit": args.limit,
            "max_pages": args.max_pages,
            "keep_records_without_ticker": bool(args.keep_records_without_ticker),
        },
    )

    print(f"tickers_processed={len(to_process):,}")
    print(f"tickers_skipped_valid={skipped_valid:,}")
    print(f"outdir={outdir}")
    print(f"progress={progress_path}")
    print(f"errors={errors_path if errors else 'none'}")


if __name__ == "__main__":
    main()


