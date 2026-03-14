from __future__ import annotations

import argparse
import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from time import sleep
from typing import Any

import pandas as pd


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_listlike(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(x) for x in v]
    if pd.isna(v):
        return []
    s = str(v).strip()
    if s in ('', '[]', 'nan', 'None'):
        return []
    try:
        x = ast.literal_eval(s)
        if isinstance(x, list):
            return [str(i) for i in x]
        return [str(x)]
    except Exception:
        return [s]


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding='utf-8')


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception:
        return pd.read_csv(path, engine='python', on_bad_lines='skip')


def _prepare_download_current(path: Path) -> pd.DataFrame:
    df = _safe_read_csv(path)
    if df.empty:
        return df
    if 'ticker' in df.columns:
        df['ticker'] = df['ticker'].astype(str).str.upper().str.strip()
    if 'date' in df.columns:
        df['date'] = df['date'].astype(str).str.strip()
    if 'session' not in df.columns:
        df['session'] = 'market'
    if 'task_key' not in df.columns and {'ticker', 'date', 'session'}.issubset(df.columns):
        df['task_key'] = df['ticker'] + '|' + df['date'] + '|' + df['session'].astype(str)
    if 'processed_at_utc' in df.columns:
        df['processed_at_utc'] = pd.to_datetime(df['processed_at_utc'], errors='coerce')
        df = df.sort_values('processed_at_utc').drop_duplicates('task_key', keep='last')
    return df


def build_once(run_id: str, run_dir: Path, expected_csv_arg: str) -> dict[str, Any]:
    events_csv = run_dir / 'trades_ticks_agent_events_current.csv'
    out_dir = run_dir / 'agent03_trades_ticks_outputs'
    out_dir.mkdir(parents=True, exist_ok=True)
    expected_vs_found_csv = run_dir / 'expected_vs_found_trades_ticks.csv'
    download_events_csv = run_dir / 'download_events_trades_ticks_current.csv'

    if not events_csv.exists():
        raise RuntimeError(f'No events found: {events_csv}')

    ev = _safe_read_csv(events_csv)
    if ev.empty:
        raise RuntimeError(f'No events found: {events_csv}')

    ev['ticker'] = ev['ticker'].astype(str).str.upper().str.strip()
    ev['date'] = pd.to_datetime(ev['date'], errors='coerce')
    ev['issues_list'] = ev.get('issues', pd.Series([None] * len(ev))).apply(parse_listlike)
    ev['warns_list'] = ev.get('warns', pd.Series([None] * len(ev))).apply(parse_listlike)
    ev['review_queue_flag'] = ev['severity'].astype(str).ne('PASS')
    ev['hard_fail_flag'] = ev['severity'].astype(str).eq('HARD_FAIL')
    ev['pass_ok_flag'] = ev['severity'].astype(str).isin(['PASS', 'SOFT_FAIL'])

    cov = (
        ev.groupby('ticker', dropna=False)
        .agg(
            files=('file', 'size'),
            present_ok_days=('pass_ok_flag', 'sum'),
            hard_fail_files=('hard_fail_flag', 'sum'),
            review_queue_files=('review_queue_flag', 'sum'),
        )
        .reset_index()
    )

    expected_tasks_total = None
    expected_tasks_found = None
    expected_tasks_missing = None
    downloaded_empty_tasks = None
    download_fail_tasks = None
    if expected_csv_arg:
        exp_path = Path(expected_csv_arg)
        if exp_path.exists():
            exp = _safe_read_csv(exp_path)
            if not exp.empty:
                exp['ticker'] = exp['ticker'].astype(str).str.upper().str.strip()
                expected_days = exp.groupby('ticker', dropna=False).size().rename('expected_days').reset_index()
                expected_tasks_total = int(len(exp))
                cov = cov.merge(expected_days, on='ticker', how='outer')

    if 'expected_days' not in cov.columns:
        cov['expected_days'] = cov['files']

    cov['files'] = pd.to_numeric(cov['files'], errors='coerce').fillna(0).astype(int)
    cov['present_ok_days'] = pd.to_numeric(cov['present_ok_days'], errors='coerce').fillna(0).astype(int)
    cov['hard_fail_files'] = pd.to_numeric(cov['hard_fail_files'], errors='coerce').fillna(0).astype(int)
    cov['review_queue_files'] = pd.to_numeric(cov['review_queue_files'], errors='coerce').fillna(0).astype(int)
    cov['expected_days'] = pd.to_numeric(cov['expected_days'], errors='coerce').fillna(cov['files']).astype(int)
    cov['missing_days_ok'] = (cov['expected_days'] - cov['present_ok_days']).clip(lower=0)
    cov['coverage_ratio_ok'] = cov['present_ok_days'] / cov['expected_days'].clip(lower=1)
    cov['observed_flag'] = cov['files'] > 0
    cov['ticker_review_status'] = cov.apply(
        lambda r: 'NOT_OBSERVED_YET' if not bool(r['observed_flag']) else ('REVIEW_QUEUE_PENDING' if r['review_queue_files'] > 0 else ('HARD_FAIL_PRESENT' if r['hard_fail_files'] > 0 else 'OBSERVED_ACCEPTED')),
        axis=1,
    )
    cov['backtest_ml_ready'] = (
        cov['observed_flag']
        & cov['hard_fail_files'].eq(0)
        & cov['review_queue_files'].eq(0)
    )
    cov = cov.sort_values(['observed_flag', 'review_queue_files', 'hard_fail_files', 'ticker'], ascending=[False, False, False, True]).reset_index(drop=True)

    observed_status_by_ticker = cov[cov['observed_flag']].copy().sort_values(['review_queue_files', 'hard_fail_files', 'files', 'ticker'], ascending=[False, False, False, True]).reset_index(drop=True)

    cause_rows = []
    for _, row in ev.iterrows():
        for cause in row['issues_list'] + row['warns_list']:
            cause_rows.append({'ticker': row['ticker'], 'cause': cause})
    causes = pd.DataFrame(cause_rows)
    if causes.empty:
        causes_by_ticker = pd.DataFrame(columns=['ticker', 'cause', 'count'])
    else:
        causes_by_ticker = causes.groupby(['ticker', 'cause'], dropna=False).size().reset_index(name='count').sort_values(['count', 'ticker'], ascending=[False, True])

    review_pending_files = int(cov['review_queue_files'].sum())
    hard_fail_files = int(cov['hard_fail_files'].sum())
    gate_status = 'REVIEW_QUEUE_OPEN' if review_pending_files > 0 else 'REVIEW_QUEUE_CLOSED'
    raw_dataset_status = 'RAW_ACCEPTED_REVIEW_PENDING' if review_pending_files > 0 else 'RAW_ACCEPTED_REVIEW_COMPLETE'

    dcur = _prepare_download_current(download_events_csv)
    if not dcur.empty and 'status' in dcur.columns:
        status = dcur['status'].astype(str)
        expected_tasks_found = int(status.eq('DOWNLOADED_OK').sum())
        downloaded_empty_tasks = int(status.eq('DOWNLOADED_EMPTY').sum())
        download_fail_tasks = int(status.eq('DOWNLOAD_FAIL').sum())
    if expected_tasks_total is not None:
        found_v = expected_tasks_found or 0
        empty_v = downloaded_empty_tasks or 0
        fail_v = download_fail_tasks or 0
        expected_tasks_missing = max(int(expected_tasks_total) - found_v - empty_v - fail_v, 0)

    summary = {
        'run_id': run_id,
        'updated_utc': utc_now_iso(),
        'tickers': int(cov['ticker'].nunique()),
        'tickers_observed': int(observed_status_by_ticker['ticker'].nunique()),
        'events_rows_dedup': int(len(ev)),
        'review_queue_pending_files': review_pending_files,
        'retry_pending_files': review_pending_files,
        'hard_fail_files': hard_fail_files,
        'mean_coverage_ok': float(cov['coverage_ratio_ok'].mean()) if len(cov) else 0.0,
        'gate_status': gate_status,
        'acceptance_policy': 'ACCEPT_ALL_RAW_REVIEW_LATER',
        'raw_dataset_status': raw_dataset_status,
        'review_queue_semantics': 'accept_all_raw_then_review_and_redlist',
        'expected_tasks_total': expected_tasks_total,
        'expected_tasks_found_files': expected_tasks_found,
        'expected_tasks_missing_files': expected_tasks_missing,
        'downloaded_empty_tasks': downloaded_empty_tasks,
        'download_fail_tasks': download_fail_tasks,
    }

    cov.to_csv(out_dir / 'coverage_by_ticker.csv', index=False)
    observed_status_by_ticker.to_csv(out_dir / 'observed_status_by_ticker.csv', index=False)
    if expected_vs_found_csv.exists():
        _safe_read_csv(expected_vs_found_csv).to_csv(out_dir / 'expected_vs_found_trades_ticks.csv', index=False)
    causes_by_ticker.to_csv(out_dir / 'causes_by_ticker.csv', index=False)
    write_json(out_dir / 'run_summary.json', summary)
    return summary


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--run-id', required=True)
    ap.add_argument('--run-dir', required=True)
    ap.add_argument('--expected-csv', default='')
    ap.add_argument('--interval-sec', type=int, default=15)
    ap.add_argument('--one-shot', action='store_true')
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    while True:
        summary = build_once(args.run_id, run_dir, args.expected_csv)
        print(json.dumps({
            'updated_utc': summary['updated_utc'],
            'processed': int(summary['events_rows_dedup']),
            'review_queue_pending': int(summary['review_queue_pending_files']),
            'retry_pending': int(summary['retry_pending_files']),
            'hard_fail': int(summary['hard_fail_files']),
            'expected_total': summary['expected_tasks_total'],
            'expected_found': summary['expected_tasks_found_files'],
            'expected_missing': summary['expected_tasks_missing_files'],
            'expected_empty': summary['downloaded_empty_tasks'],
            'download_fail_tasks': summary['download_fail_tasks'],
            'tickers_observed': summary['tickers_observed'],
            'gate': summary['gate_status'],
            'raw_dataset_status': summary['raw_dataset_status'],
            'acceptance_policy': summary['acceptance_policy'],
            'mean_coverage_ok': summary['mean_coverage_ok'],
        }))
        if args.one_shot:
            break
        sleep(args.interval_sec)


if __name__ == '__main__':
    main()
