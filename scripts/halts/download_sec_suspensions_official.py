from __future__ import annotations

import argparse
from pathlib import Path
import re
import io

import pandas as pd
import requests
from bs4 import BeautifulSoup

UA = 'Mozilla/5.0 AlexJ official-halts-research contact@example.com'
BASE = 'https://www.sec.gov'
LIST_URL = 'https://www.sec.gov/enforcement-litigation/trading-suspensions'
TICKER_PATTERNS = [
    re.compile(r'ticker\s+symbol(?:s)?\s*(?:of|is|are)?\s*[:\-]?\s*([A-Z]{1,6}(?:\s*,\s*[A-Z]{1,6})*)', re.I),
    re.compile(r'quoted\s+on\s+OTC\s+Link\s+(?:under|with)\s+the\s+symbol\s*[:\-]?\s*([A-Z]{1,6})', re.I),
    re.compile(r'symbol\s*[:\-]\s*([A-Z]{1,6})', re.I),
]


def norm_name(s: str) -> str:
    s = (s or '').upper().strip()
    s = re.sub(r'[^A-Z0-9 ]+', ' ', s)
    s = re.sub(r'\s+', ' ', s)
    return s


def load_overview_map(root: Path) -> dict[str, list[str]]:
    paths = list(root.glob('ticker=*/*.parquet'))
    rows = []
    for p in paths:
        try:
            df = pd.read_parquet(p, columns=['ticker','name'])
            rows.append(df[['ticker','name']].head(1))
        except Exception:
            continue
    if not rows:
        return {}
    all_df = pd.concat(rows, ignore_index=True)
    all_df['name_norm'] = all_df['name'].astype(str).map(norm_name)
    mp = {}
    for name_norm, grp in all_df.groupby('name_norm'):
        mp[name_norm] = sorted(grp['ticker'].astype(str).str.upper().unique().tolist())
    return mp


def extract_tickers_from_pdf(url: str) -> list[str]:
    try:
        import pypdf
    except Exception:
        return []
    try:
        r = requests.get(url, headers={'User-Agent': UA}, timeout=60)
        r.raise_for_status()
        reader = pypdf.PdfReader(io.BytesIO(r.content))
        txt = ''
        for page in reader.pages[:3]:
            txt += '\n' + (page.extract_text() or '')
        found = []
        for pat in TICKER_PATTERNS:
            for m in pat.findall(txt):
                if isinstance(m, tuple):
                    m = next((x for x in m if x), '')
                for tok in re.split(r'\s*,\s*', str(m).upper()):
                    tok = tok.strip()
                    if 1 <= len(tok) <= 6 and tok.isalpha():
                        found.append(tok)
        return sorted(set(found))
    except Exception:
        return []


def parse_page(page: int, overview_map: dict[str, list[str]]) -> tuple[pd.DataFrame, bool]:
    r = requests.get(LIST_URL, params={'page': page}, headers={'User-Agent': UA}, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'html.parser')
    table = soup.select_one('table.views-table tbody')
    if table is None:
        return pd.DataFrame(), False
    rows = []
    trs = table.select('tr.pr-list-page-row')
    for tr in trs:
        t = tr.select_one('time.datetime')
        publish_date = pd.to_datetime(t.get('datetime') if t else None, utc=True, errors='coerce')
        left = tr.select_one('div.release-view__respondents a')
        issuer_name = left.get_text(' ', strip=True) if left else None
        item_link = left.get('href') if left else None
        if item_link and item_link.startswith('/'):
            item_link = BASE + item_link
        release_no = None
        rel_val = tr.select_one('.view-table_subfield_release_number .view-table_subfield_value')
        if rel_val:
            release_no = rel_val.get_text(' ', strip=True)
        see_also_link = None
        see = tr.select_one('.view-table_subfield_see_also a')
        if see:
            see_also_link = see.get('href')
            if see_also_link and see_also_link.startswith('/'):
                see_also_link = BASE + see_also_link
        inferred_tickers = []
        if see_also_link:
            inferred_tickers = extract_tickers_from_pdf(see_also_link)
        if not inferred_tickers and issuer_name:
            inferred_tickers = overview_map.get(norm_name(issuer_name), [])
        rows.append({
            'source': 'sec',
            'source_priority': 1,
            'ticker': ','.join(inferred_tickers) if inferred_tickers else pd.NA,
            'issuer_name': issuer_name,
            'listing_exchange': pd.NA,
            'halt_date': publish_date.tz_convert(None).normalize() if pd.notna(publish_date) else pd.NaT,
            'halt_start_et': pd.NaT,
            'resume_quote_et': pd.NaT,
            'resume_trade_et': pd.NaT,
            'halt_code': 'SEC',
            'halt_type': 'SEC suspension',
            'raw_reason': 'SEC suspension',
            'release_no': release_no,
            'item_link': item_link,
            'url_source': f'{LIST_URL}?page={page}',
            'is_sec_suspension': True,
        })
    next_exists = any(a.get_text(' ', strip=True).lower() == 'next' for a in soup.find_all('a', href=True))
    return pd.DataFrame(rows), next_exists


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--output-root', default=r'D:\Halts')
    ap.add_argument('--max-pages', type=int, default=50)
    ap.add_argument('--overview-root', default=r'D:\reference\overview')
    args = ap.parse_args()

    root = Path(args.output_root)
    raw = root / 'raw' / 'sec'
    proc = root / 'processed'
    raw.mkdir(parents=True, exist_ok=True)
    proc.mkdir(parents=True, exist_ok=True)

    overview_map = load_overview_map(Path(args.overview_root))
    frames = []
    for page in range(args.max_pages):
        r = requests.get(LIST_URL, params={'page': page}, headers={'User-Agent': UA}, timeout=60)
        r.raise_for_status()
        (raw / f'sec_trading_suspensions_page_{page:02d}.html').write_text(r.text, encoding='utf-8')
        df, next_exists = parse_page(page, overview_map)
        if df.empty and page > 0:
            break
        frames.append(df)
        if not next_exists:
            break
    master = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not master.empty:
        master = master.drop_duplicates(subset=['release_no','issuer_name','halt_date'], keep='last')
    master.to_csv(proc / 'halts_master_sec.csv', index=False)
    master.to_parquet(proc / 'halts_master_sec.parquet', index=False)
    print({'pages_fetched': len(frames), 'rows_master': len(master), 'rows_with_inferred_ticker': int(master['ticker'].notna().sum()) if not master.empty else 0})


if __name__ == '__main__':
    main()
