from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path
import io

import pandas as pd
import requests

UA = 'Mozilla/5.0 AlexJ official-halts-research'
CURRENT_URL = 'https://www.nyse.com/api/trade-halts/current/download'
HIST_URL = 'https://www.nyse.com/api/trade-halts/historical/download'
STATIC_URL = 'https://www.nyse.com/api/trade-halts/static-data'


def fetch_csv(url: str, params: dict | None = None) -> pd.DataFrame:
    r = requests.get(url, params=params, headers={'User-Agent': UA}, timeout=60)
    r.raise_for_status()
    text = r.text
    return pd.read_csv(io.StringIO(text)) if text.strip() else pd.DataFrame()


def normalize(df: pd.DataFrame, source_url: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            'source','source_priority','ticker','issuer_name','listing_exchange','halt_date',
            'halt_start_et','resume_quote_et','resume_trade_et','halt_code','halt_type',
            'raw_reason','release_no','item_link','url_source','is_sec_suspension'
        ])
    out = df.copy()
    cols = {c.lower().strip(): c for c in out.columns}
    def gc(name: str):
        return out[cols[name]].copy() if name in cols else pd.Series([None] * len(out))
    halt_date = pd.to_datetime(gc('halt date'), errors='coerce')
    halt_time = gc('halt time').astype(str)
    resume_date = pd.to_datetime(gc('resume date'), errors='coerce')
    resume_time = gc('nyse resume time').astype(str)
    halt_start = pd.to_datetime(halt_date.dt.strftime('%Y-%m-%d') + ' ' + halt_time, errors='coerce')
    resume_trade = pd.to_datetime(resume_date.dt.strftime('%Y-%m-%d') + ' ' + resume_time, errors='coerce')
    norm = pd.DataFrame({
        'source': 'nyse',
        'source_priority': 1,
        'ticker': gc('symbol').astype(str).str.upper().str.strip().replace({'': pd.NA, 'nan': pd.NA}),
        'issuer_name': gc('name'),
        'listing_exchange': gc('exchange'),
        'halt_date': halt_date.dt.normalize(),
        'halt_start_et': halt_start,
        'resume_quote_et': pd.NaT,
        'resume_trade_et': resume_trade,
        'halt_code': pd.NA,
        'halt_type': gc('reason'),
        'raw_reason': gc('reason'),
        'release_no': pd.NA,
        'item_link': pd.NA,
        'url_source': source_url,
        'is_sec_suspension': False,
    })
    return norm


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--output-root', default=r'D:\Halts')
    ap.add_argument('--from-date', default=(date.today() - timedelta(days=365)).isoformat())
    ap.add_argument('--to-date', default=date.today().isoformat())
    args = ap.parse_args()

    root = Path(args.output_root)
    raw = root / 'raw' / 'nyse'
    proc = root / 'processed'
    raw.mkdir(parents=True, exist_ok=True)
    proc.mkdir(parents=True, exist_ok=True)

    current_resp = requests.get(CURRENT_URL, headers={'User-Agent': UA}, timeout=60)
    current_resp.raise_for_status()
    (raw / f'nyse_current_{date.today().isoformat()}.csv').write_text(current_resp.text, encoding='utf-8')
    current_df = pd.read_csv(io.StringIO(current_resp.text)) if current_resp.text.strip() else pd.DataFrame()

    hist_params = {
        'symbol': '', 'reason': '', 'sourceExchange': '',
        'haltDateFrom': args.from_date, 'haltDateTo': args.to_date,
    }
    hist_resp = requests.get(HIST_URL, params=hist_params, headers={'User-Agent': UA}, timeout=120)
    hist_resp.raise_for_status()
    (raw / f'nyse_historical_{args.from_date}_to_{args.to_date}.csv').write_text(hist_resp.text, encoding='utf-8')
    hist_df = pd.read_csv(io.StringIO(hist_resp.text)) if hist_resp.text.strip() else pd.DataFrame()

    static_resp = requests.get(STATIC_URL, headers={'User-Agent': UA}, timeout=60)
    static_resp.raise_for_status()
    (raw / 'nyse_static_data.json').write_text(static_resp.text, encoding='utf-8')

    current_norm = normalize(current_df, CURRENT_URL)
    hist_norm = normalize(hist_df, HIST_URL)
    master = pd.concat([hist_norm, current_norm], ignore_index=True).drop_duplicates(
        subset=['ticker','halt_date','halt_start_et','resume_trade_et','halt_type'], keep='last'
    )
    master.to_csv(proc / 'halts_master_nyse_1y.csv', index=False)
    master.to_parquet(proc / 'halts_master_nyse_1y.parquet', index=False)
    print({'rows_current': len(current_df), 'rows_historical': len(hist_df), 'rows_master': len(master), 'from_date': args.from_date, 'to_date': args.to_date})


if __name__ == '__main__':
    main()
