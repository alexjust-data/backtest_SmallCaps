from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

COLUMNS = [
    'source','source_priority','ticker','issuer_name','listing_exchange','halt_date',
    'halt_start_et','resume_quote_et','resume_trade_et','halt_code','halt_type',
    'raw_reason','release_no','item_link','url_source','is_sec_suspension'
]


def load_best(path: Path) -> pd.DataFrame:
    if path.with_suffix('.parquet').exists():
        return pd.read_parquet(path.with_suffix('.parquet'))
    if path.with_suffix('.csv').exists():
        return pd.read_csv(path.with_suffix('.csv'))
    return pd.DataFrame(columns=COLUMNS)


def normalize_cols(df: pd.DataFrame, source: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=COLUMNS)
    out = df.copy()
    for c in COLUMNS:
        if c not in out.columns:
            out[c] = pd.NA
    out['source'] = out['source'].fillna(source)
    out['halt_date'] = pd.to_datetime(out['halt_date'], errors='coerce')
    for c in ['halt_start_et','resume_quote_et','resume_trade_et']:
        out[c] = pd.to_datetime(out[c], errors='coerce')
    out['ticker'] = out['ticker'].astype('string').str.upper().str.strip()
    return out[COLUMNS]


def normalize_nasdaq(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=COLUMNS)
    out = pd.DataFrame(columns=COLUMNS)
    out['source'] = 'nasdaq'
    out['source_priority'] = pd.to_numeric(df.get('source_priority'), errors='coerce').fillna(2).astype('Int64')
    out['ticker'] = df.get('ticker', pd.Series([pd.NA]*len(df))).astype('string').str.upper().str.strip()
    out['issuer_name'] = df.get('title', pd.Series([pd.NA]*len(df)))
    out['listing_exchange'] = pd.NA
    out['halt_date'] = pd.to_datetime(df.get('halt_date'), errors='coerce')
    out['halt_start_et'] = pd.to_datetime(df.get('halt_start_et'), errors='coerce')
    out['resume_quote_et'] = pd.to_datetime(df.get('resume_quote_et'), errors='coerce')
    out['resume_trade_et'] = pd.to_datetime(df.get('resume_trade_et'), errors='coerce')
    out['halt_code'] = df.get('halt_code', pd.Series([pd.NA]*len(df)))
    out['halt_type'] = df.get('halt_type', pd.Series([pd.NA]*len(df)))
    out['raw_reason'] = df.get('raw_description_text', pd.Series([pd.NA]*len(df)))
    out['release_no'] = pd.NA
    out['item_link'] = df.get('item_link', pd.Series([pd.NA]*len(df)))
    out['url_source'] = df.get('url_source', pd.Series([pd.NA]*len(df)))
    out['is_sec_suspension'] = False
    return out[COLUMNS]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--output-root', default=r'D:\Halts')
    args = ap.parse_args()

    proc = Path(args.output_root) / 'processed'
    nasdaq = proc / 'halts_master_nasdaq_for_run_dates'
    nyse = proc / 'halts_master_nyse_1y'
    sec = proc / 'halts_master_sec'

    df_n = normalize_nasdaq(load_best(nasdaq))
    df_y = normalize_cols(load_best(nyse), 'nyse')
    df_s = normalize_cols(load_best(sec), 'sec')

    master = pd.concat([df_n, df_y, df_s], ignore_index=True)
    if not master.empty:
        master = master.drop_duplicates(subset=['source','ticker','issuer_name','halt_date','halt_start_et','release_no'], keep='last')
        master = master.sort_values(['halt_date','source','ticker','issuer_name'], ascending=[True, True, True, True])

    master.to_csv(proc / 'halts_master_multisource.csv', index=False)
    master.to_parquet(proc / 'halts_master_multisource.parquet', index=False)
    summary = pd.DataFrame({
        'source': ['nasdaq','nyse','sec','all'],
        'rows': [len(df_n), len(df_y), len(df_s), len(master)],
        'tickers_nonnull': [int(df_n['ticker'].notna().sum()), int(df_y['ticker'].notna().sum()), int(df_s['ticker'].notna().sum()), int(master['ticker'].notna().sum())],
    })
    summary.to_csv(proc / 'halts_master_multisource_summary.csv', index=False)
    print(summary.to_dict('records'))


if __name__ == '__main__':
    main()
