from __future__ import annotations

import argparse
import re
from pathlib import Path
import pandas as pd

COLUMNS = [
    'source','source_priority','ticker','issuer_name','listing_exchange','halt_date',
    'halt_start_et','resume_quote_et','resume_trade_et','halt_code','halt_type',
    'raw_reason','release_no','item_link','url_source','is_sec_suspension'
]


HALT_CODE_MAP = {
    'LUDP': 'LULD pause',
    'LUDS': 'LULD pause',
    'M': 'Volatility pause',
    'T1': 'News pending',
    'T2': 'News released',
    'T3': 'News and resumption pending',
    'T5': 'Single-stock trading pause',
    'H4': 'Non-compliance',
    'H9': 'Not current in filings',
    'D': 'Additional information requested',
    'SEC': 'SEC suspension',
}


HEADER_RE = re.compile(
    r'Issue Symbol\s+Issue Name\s+Mkt\s+Reason Code\s+Pause Threshold Price\s+Halt Date\s+Halt Time\s+Resumption Date\s+Resumption Quote Time\s+Resumption Trade Time',
    re.IGNORECASE,
)
DATE_RE = re.compile(r'\b\d{2}/\d{2}/\d{4}\b')
TIME_RE = re.compile(r'\b\d{2}:\d{2}:\d{2}\b')
MKT_CODE_TAIL_RE = re.compile(r'\b([A-Z])\s+([A-Z0-9]+)\s*$')


def _clean_text(x) -> str:
    if x is None or pd.isna(x):
        return ''
    return re.sub(r'\s+', ' ', str(x)).strip()


def _parse_nasdaq_desc(text: str) -> dict:
    s = _clean_text(text)
    if not s:
        return {}
    s = HEADER_RE.sub('', s).strip()
    dates = DATE_RE.findall(s)
    times = TIME_RE.findall(s)

    before_first_date = s
    if dates:
        before_first_date = s.split(dates[0], 1)[0].strip()

    mkt = None
    code = None
    m = MKT_CODE_TAIL_RE.search(before_first_date)
    if m:
        mkt = m.group(1)
        code = m.group(2)

    return {
        'parsed_halt_date': pd.to_datetime(dates[0], format='%m/%d/%Y', errors='coerce') if len(dates) >= 1 else pd.NaT,
        'parsed_resume_date': pd.to_datetime(dates[1], format='%m/%d/%Y', errors='coerce') if len(dates) >= 2 else pd.NaT,
        'parsed_halt_time': times[0] if len(times) >= 1 else pd.NA,
        'parsed_resume_quote_time': times[1] if len(times) >= 2 else pd.NA,
        'parsed_resume_trade_time': times[2] if len(times) >= 3 else pd.NA,
        'parsed_market': mkt,
        'parsed_halt_code': code,
    }


def _combine_et(date_series: pd.Series, time_series: pd.Series) -> pd.Series:
    date_str = pd.to_datetime(date_series, errors='coerce').dt.strftime('%Y-%m-%d')
    time_str = pd.Series(time_series, index=date_series.index, dtype='string').astype('string').str.extract(r'(\d{2}:\d{2}:\d{2})', expand=False)
    combo = pd.to_datetime(date_str.fillna('') + ' ' + time_str.fillna(''), errors='coerce')
    return combo


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
    out = pd.DataFrame(index=df.index, columns=COLUMNS)
    raw_ticker = df.get('ticker', pd.Series([pd.NA]*len(df))).astype('string').str.upper().str.strip()
    title_ticker = df.get('title', pd.Series([pd.NA]*len(df))).astype('string').str.upper().str.strip()
    parsed = df.get('raw_description_text', pd.Series([pd.NA]*len(df))).apply(_parse_nasdaq_desc).apply(pd.Series)
    raw_date = pd.to_datetime(df.get('halt_date', pd.Series([pd.NA]*len(df), index=df.index)), errors='coerce')
    fallback_date = pd.to_datetime(df.get('halt_date_text', pd.Series([pd.NA]*len(df), index=df.index)), errors='coerce')
    parsed_halt_date = pd.to_datetime(parsed.get('parsed_halt_date'), errors='coerce')
    parsed_resume_date = pd.to_datetime(parsed.get('parsed_resume_date'), errors='coerce')
    out['source'] = 'nasdaq'
    out['source_priority'] = pd.to_numeric(df.get('source_priority'), errors='coerce').fillna(2).astype('Int64')
    out['ticker'] = raw_ticker.fillna(title_ticker)
    out['issuer_name'] = pd.NA
    out['listing_exchange'] = pd.NA
    out['halt_date'] = raw_date.where(raw_date.notna(), fallback_date).where(raw_date.notna() | fallback_date.notna(), parsed_halt_date)
    parsed_halt_time = parsed.get('parsed_halt_time', pd.Series([pd.NA]*len(df), index=df.index))
    parsed_resume_quote = parsed.get('parsed_resume_quote_time', pd.Series([pd.NA]*len(df), index=df.index))
    parsed_resume_trade = parsed.get('parsed_resume_trade_time', pd.Series([pd.NA]*len(df), index=df.index))
    halt_start_raw = pd.to_datetime(df.get('halt_start_et'), errors='coerce')
    resume_quote_raw = pd.to_datetime(df.get('resume_quote_et'), errors='coerce')
    resume_trade_raw = pd.to_datetime(df.get('resume_trade_et'), errors='coerce')
    out['halt_start_et'] = halt_start_raw.where(halt_start_raw.notna(), _combine_et(out['halt_date'], parsed_halt_time))
    out['resume_quote_et'] = resume_quote_raw.where(resume_quote_raw.notna(), _combine_et(parsed_resume_date.where(parsed_resume_date.notna(), out['halt_date']), parsed_resume_quote))
    out['resume_trade_et'] = resume_trade_raw.where(resume_trade_raw.notna(), _combine_et(parsed_resume_date.where(parsed_resume_date.notna(), out['halt_date']), parsed_resume_trade))
    raw_code = df.get('halt_code', pd.Series([pd.NA]*len(df), index=df.index))
    parsed_code = parsed.get('parsed_halt_code', pd.Series([pd.NA]*len(df), index=df.index))
    out['halt_code'] = raw_code.where(raw_code.notna(), parsed_code)
    raw_type = df.get('halt_type', pd.Series([pd.NA]*len(df), index=df.index))
    out['halt_type'] = raw_type.where(raw_type.notna(), out['halt_code'].astype('string').map(HALT_CODE_MAP))
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
        master = master.drop_duplicates(subset=['source','ticker','issuer_name','halt_date','halt_start_et','resume_trade_et','halt_code','halt_type','release_no','item_link','url_source'], keep='last')
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
