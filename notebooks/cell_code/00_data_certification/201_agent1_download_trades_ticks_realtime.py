from __future__ import annotations

import argparse
import json
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, time, timezone
from pathlib import Path
from time import perf_counter, sleep
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
REQUIRED_COLUMNS = ["ticker", "date", "timestamp", "price", "size", "exchange", "conditions", "year", "month", "day"]
OK_STATUSES = {"DOWNLOADED_OK", "DOWNLOADED_EMPTY"}
TRANSIENT_HTTP = {429, 500, 502, 503, 504}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding='utf-8')
    os.replace(tmp, path)


def parquet_is_readable(path: Path) -> bool:
    try:
        pf = pq.ParquetFile(path)
        pf.read_row_group(0)
        return True
    except Exception:
        return False


def session_bounds(date_str: str, session: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    d = pd.Timestamp(date_str)
    if session == 'market':
        start_local = pd.Timestamp(datetime.combine(d.date(), time(9, 30), tzinfo=NY))
        end_local = pd.Timestamp(datetime.combine(d.date(), time(16, 0), tzinfo=NY))
    elif session == 'premarket':
        start_local = pd.Timestamp(datetime.combine(d.date(), time(4, 0), tzinfo=NY))
        end_local = pd.Timestamp(datetime.combine(d.date(), time(9, 30), tzinfo=NY))
    else:
        raise ValueError(f'Unsupported session: {session}')
    return start_local.tz_convert('UTC'), end_local.tz_convert('UTC')


def build_output_path(root: Path, ticker: str, date_str: str, session: str) -> Path:
    d = pd.Timestamp(date_str)
    return root / ticker / f'year={d.year:04d}' / f'month={d.month:02d}' / f'day={date_str}' / f'{session}.parquet'


def normalize_results(ticker: str, date_str: str, results: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    d = pd.Timestamp(date_str)
    for r in results:
        ts_ns = r.get('sip_timestamp') or r.get('participant_timestamp') or r.get('trf_timestamp')
        ts = pd.to_datetime(ts_ns, unit='ns', utc=True, errors='coerce') if ts_ns is not None else pd.NaT
        rows.append({
            'ticker': ticker,
            'date': date_str,
            'timestamp': ts,
            'price': pd.to_numeric(r.get('price'), errors='coerce'),
            'size': pd.to_numeric(r.get('size'), errors='coerce'),
            'exchange': pd.to_numeric(r.get('exchange'), errors='coerce'),
            'conditions': r.get('conditions') if isinstance(r.get('conditions'), list) else [],
            'year': int(d.year),
            'month': int(d.month),
            'day': date_str,
        })
    df = pd.DataFrame(rows, columns=REQUIRED_COLUMNS)
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce').dt.tz_convert(None).astype('datetime64[us]')
        df = df.sort_values('timestamp', kind='stable').reset_index(drop=True)
    return df


def recommended_concurrency_policy(concurrent: int) -> str:
    if concurrent <= 8:
        return 'stable_default'
    if concurrent <= 12:
        return 'fast_balanced'
    return 'aggressive_monitor_429'


def retry_sleep_seconds(attempt: int, base_sleep_sec: float, max_sleep_sec: float) -> float:
    jitter = random.uniform(0.85, 1.25)
    return min(max_sleep_sec, base_sleep_sec * (2 ** max(attempt - 1, 0)) * jitter)


def fetch_polygon_trades(
    ticker: str,
    date_str: str,
    session: str,
    api_key: str,
    timeout: int = 60,
    max_retries: int = 4,
    retry_base_sleep_sec: float = 0.75,
    retry_max_sleep_sec: float = 12.0,
) -> list[dict[str, Any]]:
    start_utc, end_utc = session_bounds(date_str, session)
    url = f'https://api.polygon.io/v3/trades/{ticker}'
    params = {
        'timestamp.gte': start_utc.isoformat(),
        'timestamp.lt': end_utc.isoformat(),
        'order': 'asc',
        'sort': 'timestamp',
        'limit': 50000,
        'apiKey': api_key,
    }
    out: list[dict[str, Any]] = []
    with requests.Session() as s:
        while url:
            last_exc: Exception | None = None
            payload: dict[str, Any] | None = None
            for attempt in range(1, max_retries + 1):
                try:
                    resp = s.get(url, params=params if params else None, timeout=timeout)
                    if resp.status_code in TRANSIENT_HTTP:
                        raise requests.HTTPError(f'transient_status={resp.status_code}', response=resp)
                    resp.raise_for_status()
                    payload = resp.json()
                    last_exc = None
                    break
                except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                    last_exc = exc
                    retryable = True
                    if isinstance(exc, requests.HTTPError) and getattr(exc, 'response', None) is not None:
                        retryable = exc.response.status_code in TRANSIENT_HTTP
                    if (not retryable) or attempt >= max_retries:
                        break
                    sleep(retry_sleep_seconds(attempt, retry_base_sleep_sec, retry_max_sleep_sec))
            if payload is None:
                raise last_exc if last_exc is not None else RuntimeError('unknown fetch error')
            out.extend(payload.get('results', []) or [])
            next_url = payload.get('next_url')
            if next_url and 'apiKey=' not in next_url:
                sep = '&' if '?' in next_url else '?'
                next_url = f'{next_url}{sep}apiKey={api_key}'
            url = next_url
            params = None
    return out


def write_parquet_atomic(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, tmp, compression='zstd')
    os.replace(tmp, path)


@dataclass
class DownloadConfig:
    csv_path: Path
    output_root: Path
    run_id: str
    run_dir: Path
    session: str
    concurrent: int
    task_batch_size: int
    resume: bool
    api_key: str
    max_retries: int
    retry_base_sleep_sec: float
    retry_max_sleep_sec: float


def load_current_events(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def load_existing_ok(path: Path) -> set[str]:
    df = load_current_events(path)
    if df.empty or 'task_key' not in df.columns or 'status' not in df.columns:
        return set()
    return set(df[df['status'].isin(list(OK_STATUSES))]['task_key'].astype(str))


def scan_existing_files(tasks: pd.DataFrame, root: Path, session: str) -> list[dict[str, Any]]:
    rows = []
    for row in tasks.itertuples(index=False):
        p = build_output_path(root, row.ticker, row.date, session)
        if p.exists() and parquet_is_readable(p):
            rows.append({
                'task_key': f'{row.ticker}|{row.date}|{session}',
                'ticker': row.ticker,
                'date': row.date,
                'session': session,
                'status': 'DOWNLOADED_OK',
                'rows': None,
                'file': str(p),
                'note': 'resume_existing_file',
                'processed_at_utc': utc_now_iso(),
            })
    return rows


def persist_events(current_path: Path, history_path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    add = pd.DataFrame(rows)
    cur = load_current_events(current_path)
    if not cur.empty and 'task_key' in cur.columns:
        cur = cur[~cur['task_key'].isin(add['task_key'])]
        merged = pd.concat([cur, add], ignore_index=True)
    else:
        merged = add
    merged.to_csv(current_path, index=False)

    hist = load_current_events(history_path)
    if not hist.empty:
        pd.concat([hist, add], ignore_index=True).to_csv(history_path, index=False)
    else:
        add.to_csv(history_path, index=False)


def build_expected_manifest(tasks: pd.DataFrame, output_root: Path, session: str) -> pd.DataFrame:
    exp = tasks.copy()
    exp['task_key'] = exp['ticker'] + '|' + exp['date'] + '|' + session
    exp['session'] = session
    exp['expected_file'] = exp.apply(lambda r: str(build_output_path(output_root, r['ticker'], r['date'], session)), axis=1)
    return exp[['task_key', 'ticker', 'date', 'session', 'expected_file']]


def process_task(cfg: DownloadConfig, ticker: str, date_str: str) -> dict[str, Any]:
    task_key = f'{ticker}|{date_str}|{cfg.session}'
    out_path = build_output_path(cfg.output_root, ticker, date_str, cfg.session)
    t0 = perf_counter()
    try:
        results = fetch_polygon_trades(
            ticker,
            date_str,
            cfg.session,
            cfg.api_key,
            max_retries=cfg.max_retries,
            retry_base_sleep_sec=cfg.retry_base_sleep_sec,
            retry_max_sleep_sec=cfg.retry_max_sleep_sec,
        )
        df = normalize_results(ticker, date_str, results)
        if df.empty:
            return {
                'task_key': task_key,
                'ticker': ticker,
                'date': date_str,
                'session': cfg.session,
                'status': 'DOWNLOADED_EMPTY',
                'rows': 0,
                'file': str(out_path),
                'elapsed_sec': round(perf_counter() - t0, 4),
                'processed_at_utc': utc_now_iso(),
            }
        write_parquet_atomic(df, out_path)
        return {
            'task_key': task_key,
            'ticker': ticker,
            'date': date_str,
            'session': cfg.session,
            'status': 'DOWNLOADED_OK',
            'rows': int(len(df)),
            'file': str(out_path),
            'elapsed_sec': round(perf_counter() - t0, 4),
            'processed_at_utc': utc_now_iso(),
        }
    except Exception as exc:
        return {
            'task_key': task_key,
            'ticker': ticker,
            'date': date_str,
            'session': cfg.session,
            'status': 'DOWNLOAD_FAIL',
            'rows': 0,
            'file': str(out_path),
            'error': repr(exc),
            'elapsed_sec': round(perf_counter() - t0, 4),
            'processed_at_utc': utc_now_iso(),
        }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--csv', required=True)
    ap.add_argument('--output', required=True)
    ap.add_argument('--run-id', required=True)
    ap.add_argument('--run-dir', required=True)
    ap.add_argument('--session', default='market', choices=['market', 'premarket'])
    ap.add_argument('--concurrent', type=int, default=8)
    ap.add_argument('--task-batch-size', type=int, default=200)
    ap.add_argument('--resume', action='store_true', default=True)
    ap.add_argument('--api-key', default=os.getenv('POLYGON_API_KEY', ''))
    ap.add_argument('--max-retries', type=int, default=4)
    ap.add_argument('--retry-base-sleep-sec', type=float, default=0.75)
    ap.add_argument('--retry-max-sleep-sec', type=float, default=12.0)
    args = ap.parse_args()
    if not args.api_key:
        raise SystemExit('Missing POLYGON_API_KEY / --api-key')

    cfg = DownloadConfig(
        Path(args.csv),
        Path(args.output),
        args.run_id,
        Path(args.run_dir),
        args.session,
        args.concurrent,
        args.task_batch_size,
        bool(args.resume),
        args.api_key,
        int(args.max_retries),
        float(args.retry_base_sleep_sec),
        float(args.retry_max_sleep_sec),
    )
    cfg.run_dir.mkdir(parents=True, exist_ok=True)

    current_events = cfg.run_dir / 'download_events_trades_ticks_current.csv'
    history_events = cfg.run_dir / 'download_events_trades_ticks_history.csv'
    live_json = cfg.run_dir / 'live_status_trades_ticks_download.json'
    run_cfg = cfg.run_dir / 'run_config_trades_ticks_download.json'
    expected_manifest = cfg.run_dir / 'expected_manifest_trades_ticks.csv'

    atomic_write_json(run_cfg, {
        'run_id': cfg.run_id,
        'run_dir': str(cfg.run_dir),
        'csv_path': str(cfg.csv_path),
        'output_root': str(cfg.output_root),
        'session': cfg.session,
        'concurrent': cfg.concurrent,
        'task_batch_size': cfg.task_batch_size,
        'resume': cfg.resume,
        'max_retries': cfg.max_retries,
        'retry_base_sleep_sec': cfg.retry_base_sleep_sec,
        'retry_max_sleep_sec': cfg.retry_max_sleep_sec,
        'recommended_concurrency_policy': recommended_concurrency_policy(cfg.concurrent),
    })

    tasks = pd.read_csv(cfg.csv_path)
    tasks['ticker'] = tasks['ticker'].astype(str).str.upper().str.strip()
    tasks['date'] = tasks['date'].astype(str).str.strip()
    tasks = tasks[['ticker', 'date']].drop_duplicates().reset_index(drop=True)
    tasks['task_key'] = tasks['ticker'] + '|' + tasks['date'] + '|' + cfg.session

    build_expected_manifest(tasks[['ticker', 'date']].copy(), cfg.output_root, cfg.session).to_csv(expected_manifest, index=False)

    existing_resume_rows = scan_existing_files(tasks[['ticker', 'date']].copy(), cfg.output_root, cfg.session) if cfg.resume else []
    if existing_resume_rows:
        persist_events(current_events, history_events, existing_resume_rows)

    already_ok = load_existing_ok(current_events) if cfg.resume else set()
    todo = tasks[~tasks['task_key'].isin(already_ok)].reset_index(drop=True)

    print(f'run_id={cfg.run_id}')
    print(f'run_dir={cfg.run_dir}')
    print(f'output_root={cfg.output_root}')
    print(f'csv={cfg.csv_path}')
    print(f'session={cfg.session}')
    print(f'tasks_total={len(tasks)}')
    print(f'tasks_already_ok={len(already_ok)}')
    print(f'tasks_to_process={len(todo)}')
    print(f'concurrency_policy={recommended_concurrency_policy(cfg.concurrent)}')

    current_df = load_current_events(current_events)
    done_ok = int(current_df['status'].isin(list(OK_STATUSES)).sum()) if (not current_df.empty and 'status' in current_df.columns) else 0
    done_bad = int(current_df['status'].eq('DOWNLOAD_FAIL').sum()) if (not current_df.empty and 'status' in current_df.columns) else 0

    for start in range(0, len(todo), cfg.task_batch_size):
        chunk = todo.iloc[start:start + cfg.task_batch_size].copy()
        batch_rows: list[dict[str, Any]] = []
        batch_t0 = perf_counter()
        with ThreadPoolExecutor(max_workers=cfg.concurrent) as ex:
            futs = [ex.submit(process_task, cfg, row.ticker, row.date) for row in chunk.itertuples(index=False)]
            for fut in as_completed(futs):
                row = fut.result()
                batch_rows.append(row)
                if row['status'] in OK_STATUSES:
                    done_ok += 1
                else:
                    done_bad += 1
                if (done_ok + done_bad) % 25 == 0:
                    print(f'processed={done_ok + done_bad}/{len(tasks)}')
        persist_events(current_events, history_events, batch_rows)
        batch_elapsed = perf_counter() - batch_t0
        batch_df = pd.DataFrame(batch_rows) if batch_rows else pd.DataFrame()
        batch_ok = int(batch_df['status'].eq('DOWNLOADED_OK').sum()) if not batch_df.empty else 0
        batch_empty = int(batch_df['status'].eq('DOWNLOADED_EMPTY').sum()) if not batch_df.empty else 0
        batch_fail = int(batch_df['status'].eq('DOWNLOAD_FAIL').sum()) if not batch_df.empty else 0
        batch_rows_sum = int(pd.to_numeric(batch_df.get('rows'), errors='coerce').fillna(0).sum()) if not batch_df.empty else 0
        elapsed_series = pd.to_numeric(batch_df['elapsed_sec'], errors='coerce') if (not batch_df.empty and 'elapsed_sec' in batch_df.columns) else pd.Series(dtype=float)
        batch_mean_elapsed = float(elapsed_series.dropna().mean()) if not elapsed_series.empty and elapsed_series.notna().any() else 0.0
        atomic_write_json(live_json, {
            'run_id': cfg.run_id,
            'updated_utc': utc_now_iso(),
            'tasks_total': int(len(tasks)),
            'done_ok': int(done_ok),
            'done_bad': int(done_bad),
            'pending': int(max(len(tasks) - done_ok - done_bad, 0)),
            'session': cfg.session,
            'csv_path': str(cfg.csv_path),
            'output_root': str(cfg.output_root),
            'concurrent': cfg.concurrent,
            'expected_manifest_csv': str(expected_manifest),
            'recommended_concurrency_policy': recommended_concurrency_policy(cfg.concurrent),
            'batch_metrics': {
                'batch_tasks': int(len(chunk)),
                'batch_ok': batch_ok,
                'batch_empty': batch_empty,
                'batch_fail': batch_fail,
                'batch_rows_total': batch_rows_sum,
                'batch_elapsed_sec': round(batch_elapsed, 4),
                'batch_mean_task_sec': round(batch_mean_elapsed, 4),
                'batch_tasks_per_sec': round(len(chunk) / batch_elapsed, 4) if batch_elapsed > 0 else None,
            },
        })
        tasks_per_sec = (len(chunk) / batch_elapsed) if batch_elapsed > 0 else 0.0
        print(
            'batch_done='
            f'{min(start + len(chunk), len(todo))}/{len(tasks)} '
            f'| ok={batch_ok} empty={batch_empty} fail={batch_fail} '
            f'| rows={batch_rows_sum} elapsed_sec={batch_elapsed:.2f} '
            f'| tasks_per_sec={tasks_per_sec:.2f}'
        )

    atomic_write_json(live_json, {
        'run_id': cfg.run_id,
        'updated_utc': utc_now_iso(),
        'tasks_total': int(len(tasks)),
        'done_ok': int(done_ok),
        'done_bad': int(done_bad),
        'pending': int(max(len(tasks) - done_ok - done_bad, 0)),
        'session': cfg.session,
        'csv_path': str(cfg.csv_path),
        'output_root': str(cfg.output_root),
        'concurrent': cfg.concurrent,
        'expected_manifest_csv': str(expected_manifest),
        'recommended_concurrency_policy': recommended_concurrency_policy(cfg.concurrent),
    })


if __name__ == '__main__':
    main()
