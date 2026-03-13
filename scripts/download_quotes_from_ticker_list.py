#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TICKERS_FILE = PROJECT_ROOT / "data" / "reference" / "tickers_universe.txt"
DEFAULT_PING_MASTER = PROJECT_ROOT / "data" / "reference" / "ping_range_master.parquet"
DEFAULT_DOWNLOADER = PROJECT_ROOT / "scripts" / "download_quotes.py"
DEFAULT_RUNS_ROOT = PROJECT_ROOT / "runs" / "polygon_realtime_audit"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_tickers(path: Path, column: str | None) -> list[str]:
    suffix = path.suffix.lower()
    if suffix == ".txt":
        tickers = [line.strip().upper() for line in path.read_text(encoding="utf-8").splitlines()]
        return sorted({t for t in tickers if t})

    if suffix == ".csv":
        df = pd.read_csv(path)
    elif suffix == ".parquet":
        df = pd.read_parquet(path)
    else:
        raise ValueError(f"Formato no soportado para tickers: {path}")

    candidate_cols = [column] if column else ["ticker", "symbol", "Ticker", "SYMBOL"]
    ticker_col = next((c for c in candidate_cols if c and c in df.columns), None)
    if ticker_col is None:
        raise RuntimeError(f"No encuentro columna ticker/symbol en {path}")

    vals = (
        df[ticker_col]
        .astype(str)
        .str.strip()
        .str.upper()
        .replace("", pd.NA)
        .dropna()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )
    return vals


def load_ping_master(path: Path) -> pd.DataFrame:
    pm = pd.read_parquet(path)
    required = {"ticker", "has_data", "first_day", "last_day"}
    missing = required - set(pm.columns)
    if missing:
        raise RuntimeError(f"Ping master incompleto. Faltan columnas: {sorted(missing)}")

    pm = pm.copy()
    pm["ticker"] = pm["ticker"].astype(str).str.strip().str.upper()
    pm = pm[pm["ticker"] != ""]
    pm["has_data"] = pm["has_data"].fillna(False).astype(bool)
    pm["first_day"] = pd.to_datetime(pm["first_day"], errors="coerce")
    pm["last_day"] = pd.to_datetime(pm["last_day"], errors="coerce")
    pm = pm.dropna(subset=["first_day", "last_day"])
    pm = pm[pm["has_data"]]
    return pm[["ticker", "first_day", "last_day"]].drop_duplicates(subset=["ticker"])


def build_tasks(
    tickers: list[str],
    date_from: str,
    date_to: str,
    ping_master: pd.DataFrame | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    start = pd.Timestamp(date_from).normalize()
    end = pd.Timestamp(date_to).normalize()
    if end < start:
        raise ValueError("date_to no puede ser anterior a date_from")

    ping_map = {}
    if ping_master is not None:
        ping_map = {
            row.ticker: (row.first_day.normalize(), row.last_day.normalize())
            for row in ping_master.itertuples(index=False)
        }

    task_frames: list[pd.DataFrame] = []
    summary_rows: list[dict[str, object]] = []

    for ticker in tickers:
        eff_start = start
        eff_end = end
        source = "requested_range"

        if ticker in ping_map:
            p0, p1 = ping_map[ticker]
            eff_start = max(start, p0)
            eff_end = min(end, p1)
            source = "ping_master_clipped"
        elif ping_master is not None:
            source = "missing_in_ping_master"

        if eff_end < eff_start:
            summary_rows.append(
                {
                    "ticker": ticker,
                    "effective_start": pd.NaT,
                    "effective_end": pd.NaT,
                    "n_tasks": 0,
                    "window_source": source,
                    "status": "SKIPPED_OUTSIDE_RANGE",
                }
            )
            continue

        days = pd.bdate_range(eff_start, eff_end)
        if len(days) == 0:
            summary_rows.append(
                {
                    "ticker": ticker,
                    "effective_start": eff_start.date().isoformat(),
                    "effective_end": eff_end.date().isoformat(),
                    "n_tasks": 0,
                    "window_source": source,
                    "status": "SKIPPED_NO_BUSINESS_DAYS",
                }
            )
            continue

        task_frames.append(
            pd.DataFrame(
                {
                    "ticker": ticker,
                    "date": pd.Series(days).dt.strftime("%Y-%m-%d"),
                }
            )
        )
        summary_rows.append(
            {
                "ticker": ticker,
                "effective_start": eff_start.date().isoformat(),
                "effective_end": eff_end.date().isoformat(),
                "n_tasks": int(len(days)),
                "window_source": source,
                "status": "READY",
            }
        )

    tasks = (
        pd.concat(task_frames, ignore_index=True)
        if task_frames
        else pd.DataFrame(columns=["ticker", "date"])
    )
    summary = pd.DataFrame(summary_rows)
    return tasks, summary


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Genera tareas y descarga quotes para un listado de tickers.")
    p.add_argument("--tickers", default=str(DEFAULT_TICKERS_FILE), help="TXT/CSV/PARQUET con tickers")
    p.add_argument("--ticker-column", default=None, help="Columna ticker si el input es CSV/PARQUET")
    p.add_argument("--date-from", default="2005-01-01")
    p.add_argument("--date-to", default="2026-02-10")
    p.add_argument("--output-root", required=True, help="Carpeta root de quotes destino")
    p.add_argument("--run-id", default=None, help="Por defecto timestamp")
    p.add_argument("--run-root", default=str(DEFAULT_RUNS_ROOT), help="Raiz donde crear RUN_DIR")
    p.add_argument("--ping-master", default=str(DEFAULT_PING_MASTER), help="Parquet con first_day/last_day por ticker")
    p.add_argument("--ignore-ping-master", action="store_true", help="No recorta tareas con ping master")
    p.add_argument("--downloader-script", default=str(DEFAULT_DOWNLOADER))
    p.add_argument("--concurrent", type=int, default=80)
    p.add_argument("--resume", action="store_true")
    p.add_argument("--max-pages", type=int, default=0)
    p.add_argument("--request-timeout-sec", type=int, default=30)
    p.add_argument("--max-retries-per-page", type=int, default=5)
    p.add_argument("--retry-backoff-sec", type=float, default=1.5)
    p.add_argument("--day-partition", choices=["DD", "YYYY-MM-DD"], default="DD")
    p.add_argument("--market-tz", default="America/New_York")
    p.add_argument("--market-tz-offset", default="-05:00")
    p.add_argument("--session-start", default="04:00:00")
    p.add_argument("--session-end", default="20:00:00")
    p.add_argument("--hash-files", action="store_true")
    p.add_argument("--max-empty-rechecks", type=int, default=2)
    p.add_argument("--dry-run", action="store_true", help="Solo genera tareas y manifest")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key and not args.dry_run:
        print("ERROR: falta POLYGON_API_KEY en el entorno", file=sys.stderr)
        return 2

    tickers_path = Path(args.tickers)
    if not tickers_path.exists():
        print(f"ERROR: no existe listado de tickers: {tickers_path}", file=sys.stderr)
        return 2

    downloader_script = Path(args.downloader_script)
    if not downloader_script.exists():
        print(f"ERROR: no existe downloader_script: {downloader_script}", file=sys.stderr)
        return 2

    run_id = args.run_id or datetime.now().strftime("%Y%m%d_%H%M%S_quotes_bulk")
    run_dir = Path(args.run_root) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    tickers = load_tickers(tickers_path, args.ticker_column)
    ping_master = None
    ping_master_path = Path(args.ping_master)
    if not args.ignore_ping_master and ping_master_path.exists():
        ping_master = load_ping_master(ping_master_path)

    tasks, summary = build_tasks(
        tickers=tickers,
        date_from=args.date_from,
        date_to=args.date_to,
        ping_master=ping_master,
    )

    tasks_csv = run_dir / "quotes_tasks_from_ticker_list.csv"
    summary_csv = run_dir / "quotes_tasks_summary_by_ticker.csv"
    manifest_json = run_dir / "quotes_tasks_manifest.json"

    tasks.to_csv(tasks_csv, index=False, encoding="utf-8")
    summary.to_csv(summary_csv, index=False, encoding="utf-8")

    manifest = {
        "created_at_utc": utc_now(),
        "run_id": run_id,
        "tickers_input_file": str(tickers_path),
        "tickers_count": int(len(tickers)),
        "date_from": args.date_from,
        "date_to": args.date_to,
        "output_root": str(Path(args.output_root)),
        "tasks_csv": str(tasks_csv),
        "summary_csv": str(summary_csv),
        "tasks_total": int(len(tasks)),
        "tickers_ready": int((summary["status"] == "READY").sum()) if len(summary) else 0,
        "tickers_skipped": int((summary["status"] != "READY").sum()) if len(summary) else 0,
        "ping_master_used": bool(ping_master is not None),
        "ping_master_path": str(ping_master_path) if ping_master is not None else None,
        "downloader_script": str(downloader_script),
        "dry_run": bool(args.dry_run),
    }
    manifest_json.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"run_id={run_id}")
    print(f"tickers_count={len(tickers)}")
    print(f"tasks_total={len(tasks)}")
    print(f"tasks_csv={tasks_csv}")
    print(f"summary_csv={summary_csv}")
    print(f"manifest_json={manifest_json}")

    if args.dry_run:
        return 0

    cmd = [
        sys.executable,
        str(downloader_script),
        "--csv",
        str(tasks_csv),
        "--output",
        str(Path(args.output_root)),
        "--concurrent",
        str(args.concurrent),
        "--api-key",
        api_key,
        "--run-id",
        run_id,
        "--run-dir",
        str(run_dir),
        "--max-pages",
        str(args.max_pages),
        "--request-timeout-sec",
        str(args.request_timeout_sec),
        "--max-retries-per-page",
        str(args.max_retries_per_page),
        "--retry-backoff-sec",
        str(args.retry_backoff_sec),
        "--day-partition",
        args.day_partition,
        "--market-tz",
        args.market_tz,
        "--market-tz-offset",
        args.market_tz_offset,
        "--session-start",
        args.session_start,
        "--session-end",
        args.session_end,
        "--max-empty-rechecks",
        str(args.max_empty_rechecks),
    ]
    if args.resume:
        cmd.append("--resume")
    if args.hash_files:
        cmd.append("--hash-files")

    print("launch_cmd=" + " ".join(f'"{x}"' if " " in x else x for x in cmd))
    completed = subprocess.run(cmd)
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
