from __future__ import annotations

import argparse
import json
import os
import random
import re
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd
import requests

BASE_URL = "https://api.polygon.io"
_THREAD_LOCAL = threading.local()
DATE_RE = re.compile(r"(?<!\d)(19\d{2}|20\d{2})[-_/]?(0[1-9]|1[0-2])[-_/]?(0[1-9]|[12]\d|3[01])(?!\d)")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_date(s: str) -> datetime.date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def pick_col(df: pd.DataFrame, names: Iterable[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for n in names:
        c = cols.get(n.lower())
        if c is not None:
            return c
    return None


def load_tickers(input_path: Path) -> List[str]:
    d = pd.read_parquet(input_path, columns=["ticker"])
    t = d["ticker"].astype("string").str.strip().dropna().str.upper()
    t = t[t != ""]
    return t.drop_duplicates().sort_values().tolist()


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def atomic_write_parquet(df: pd.DataFrame, final_path: Path) -> None:
    final_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = final_path.parent / f".tmp_{final_path.name}_{int(time.time() * 1000)}"
    df.to_parquet(tmp, index=False)
    os.replace(tmp, final_path)


def ticker_checkpoint_path(run_dir: Path, ticker: str) -> Path:
    return run_dir / "checkpoints" / f"{ticker}.json"


def resolve_out_file(cp_path: Path, p: str) -> Path:
    path = Path(p)
    if path.is_absolute():
        return path
    outdir = cp_path.parent.parent.parent
    return outdir / path


def load_checkpoint(cp_path: Path) -> Dict[str, Any] | None:
    if not cp_path.exists():
        return None
    try:
        return json.loads(cp_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def checkpoint_matches_run(c: Dict[str, Any], ticker: str, start: str, end: str, adjusted: bool) -> bool:
    required = ["ticker", "status", "start", "end", "out_files", "adjusted", "rows", "files_written"]
    if any(k not in c for k in required):
        return False
    if str(c["ticker"]).upper() != ticker.upper():
        return False
    if c["status"] != "ok":
        return False
    if str(c["start"]) != start or str(c["end"]) != end:
        return False
    if bool(c["adjusted"]) != bool(adjusted):
        return False
    return True


def validate_parquet_shape(path: Path) -> tuple[int, bool]:
    try:
        d = pd.read_parquet(path)
    except Exception:
        return 0, False
    required_cols = {"ticker", "date", "year", "o", "h", "l", "c", "v", "vw", "n", "t"}
    if not required_cols.issubset(set(d.columns)):
        return len(d), False
    if d.empty:
        return 0, True
    ok = d["ticker"].notna().all() and d["date"].notna().all() and d["year"].notna().all()
    return len(d), bool(ok)


def parquet_date_bounds(path: Path) -> tuple[str | None, str | None]:
    try:
        d = pd.read_parquet(path, columns=["date"])
    except Exception:
        return None, None
    if d.empty or "date" not in d.columns:
        return None, None
    s = d["date"].dropna().astype("string")
    if s.empty:
        return None, None
    return str(s.min()), str(s.max())


def is_existing_checkpoint_valid(cp_path: Path, ticker: str, start: str, end: str, adjusted: bool) -> bool:
    c = load_checkpoint(cp_path)
    if c is None:
        return False
    if not checkpoint_matches_run(c, ticker=ticker, start=start, end=end, adjusted=adjusted):
        return False

    expected_rows = int(c.get("rows", 0))
    out_files = c.get("out_files") or []
    if not isinstance(out_files, list):
        return False

    rows_sum = 0
    min_date: str | None = None
    max_date: str | None = None
    for p in out_files:
        fp = resolve_out_file(cp_path, str(p))
        if not fp.exists():
            return False
        n, ok = validate_parquet_shape(fp)
        if not ok:
            return False
        rows_sum += n
        pmin, pmax = parquet_date_bounds(fp)
        if pmin is not None and (min_date is None or pmin < min_date):
            min_date = pmin
        if pmax is not None and (max_date is None or pmax > max_date):
            max_date = pmax

    if rows_sum != expected_rows:
        return False
    if expected_rows > 0:
        if min_date is None or max_date is None:
            return False
        # PTI real no exige cobertura completa start..end por ticker:
        # basta con que lo observado caiga dentro de la ventana solicitada.
        if min_date < start or max_date > end:
            return False
    return True


def build_checkpoint(
    *,
    ticker: str,
    start: str,
    end: str,
    adjusted: bool,
    rows: int,
    files_written: int,
    pages: int,
    http_status: int,
    outdir: Path,
) -> Dict[str, Any]:
    out_files = sorted(str(p.relative_to(outdir)) for p in (outdir / f"ticker={ticker}").rglob("*.parquet"))
    return {
        "ticker": ticker,
        "status": "ok",
        "start": start,
        "end": end,
        "adjusted": adjusted,
        "rows": int(rows),
        "files_written": int(files_written),
        "pages": int(pages),
        "http_status": int(http_status),
        "updated_at_utc": utc_now(),
        "out_files": out_files,
    }


def summarize_ticker_outputs(outdir: Path, ticker: str) -> tuple[int, int]:
    base = outdir / f"ticker={ticker}"
    if not base.exists():
        return 0, 0
    files = sorted(base.rglob("*.parquet"))
    rows = 0
    for fp in files:
        n, ok = validate_parquet_shape(fp)
        if not ok:
            continue
        rows += int(n)
    return rows, len(files)


def upsert_ticker_year_partitions(d: pd.DataFrame, outdir: Path, ticker: str, *, prune_obsolete_years: bool) -> tuple[int, int]:
    base = outdir / f"ticker={ticker}"
    base.mkdir(parents=True, exist_ok=True)

    if d.empty:
        if prune_obsolete_years and base.exists():
            for child in base.iterdir():
                if not child.is_dir() or not child.name.startswith("year="):
                    continue
                shutil.rmtree(child, ignore_errors=True)
        return 0, 0

    expected_year_dirs: set[str] = set()
    nonce = f"{int(time.time() * 1000)}_{os.getpid()}_{ticker}"
    staging_root = outdir / "_staging_commit"
    staging_ticker = staging_root / f"ticker={ticker}__{nonce}"
    staging_ticker.mkdir(parents=True, exist_ok=True)

    files = 0
    for year, g in d.groupby("year"):
        y = int(year)
        yr_dir = staging_ticker / f"year={y}"
        expected_year_dirs.add(yr_dir.name)
        final_path = yr_dir / f"day_aggs_{ticker}_{y}.parquet"
        g2 = g.sort_values("date").drop_duplicates(subset=["ticker", "date"])
        atomic_write_parquet(g2, final_path)
        files += 1

    # Si no podamos, preservamos años no tocados dentro del staging.
    if (not prune_obsolete_years) and base.exists():
        for child in base.iterdir():
            if not child.is_dir() or not child.name.startswith("year="):
                continue
            if child.name in expected_year_dirs:
                continue
            dst = staging_ticker / child.name
            if dst.exists():
                shutil.rmtree(dst, ignore_errors=True)
            shutil.copytree(child, dst)

    backup_ticker = staging_root / f".bak_ticker={ticker}__{nonce}"
    try:
        if backup_ticker.exists():
            shutil.rmtree(backup_ticker, ignore_errors=True)
        if base.exists():
            os.replace(base, backup_ticker)
        os.replace(staging_ticker, base)
        if backup_ticker.exists():
            shutil.rmtree(backup_ticker, ignore_errors=True)
    except Exception:
        # rollback best-effort
        if base.exists():
            shutil.rmtree(base, ignore_errors=True)
        if backup_ticker.exists():
            os.replace(backup_ticker, base)
        raise
    finally:
        if staging_ticker.exists():
            shutil.rmtree(staging_ticker, ignore_errors=True)

    return len(d), files

def get_session(api_key: str) -> requests.Session:
    sess = getattr(_THREAD_LOCAL, "session", None)
    if sess is None:
        sess = requests.Session()
        sess.headers.update({"Authorization": f"Bearer {api_key}"})
        _THREAD_LOCAL.session = sess
    return sess


def parse_results(records: List[Dict[str, Any]], ticker: str) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=["ticker", "date", "year", "o", "h", "l", "c", "v", "vw", "n", "t"])

    d = pd.DataFrame.from_records(records)
    for c in ["o", "h", "l", "c", "v", "vw", "n", "t"]:
        if c not in d.columns:
            d[c] = pd.NA

    ts = pd.to_datetime(d["t"], unit="ms", utc=True, errors="coerce")
    d["date"] = ts.dt.strftime("%Y-%m-%d")
    d["year"] = ts.dt.year.astype("Int64")
    d["ticker"] = ticker

    out = d[["ticker", "date", "year", "o", "h", "l", "c", "v", "vw", "n", "t"]].copy()
    out = out.dropna(subset=["date", "year"])
    out["year"] = out["year"].astype(int)
    out = out.sort_values("date").drop_duplicates(subset=["ticker", "date"])
    return out


def read_retry_after(resp: requests.Response) -> float:
    ra = resp.headers.get("Retry-After")
    if not ra:
        return 0.0
    try:
        return float(ra)
    except Exception:
        try:
            dt = parsedate_to_datetime(ra)
            now = datetime.now(timezone.utc)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0.0, (dt - now).total_seconds())
        except Exception:
            return 0.0


def request_json_with_retry(
    sess: requests.Session,
    url: str,
    params: Dict[str, Any],
    timeout: int,
    max_retries: int,
    backoff_base: float,
    backoff_max: float,
) -> tuple[Dict[str, Any] | None, int, str]:
    last_status = 0
    for attempt in range(max_retries + 1):
        try:
            r = sess.get(url, params=params, timeout=timeout)
            last_status = r.status_code
            if r.status_code == 200:
                try:
                    return r.json(), 200, "ok"
                except ValueError as e:
                    return None, 200, f"json_decode_error:{e}"

            if r.status_code == 429 or r.status_code >= 500:
                if attempt < max_retries:
                    retry_after_wait = read_retry_after(r)
                    exp_wait = min(backoff_max, backoff_base * (2**attempt))
                    if retry_after_wait > 0:
                        base_wait = max(retry_after_wait, exp_wait)
                        wait = base_wait + random.uniform(0.0, min(2.0, max(0.1, base_wait * 0.05)))
                    else:
                        base_wait = exp_wait
                        wait = min(backoff_max, base_wait + random.uniform(0.0, max(0.1, base_wait * 0.25)))
                    time.sleep(wait)
                    continue
                return None, r.status_code, f"http_{r.status_code}_retries_exhausted"

            return None, r.status_code, f"http_{r.status_code}"

        except requests.RequestException as e:
            if attempt < max_retries:
                base_wait = min(backoff_max, backoff_base * (2**attempt))
                wait = min(backoff_max, base_wait + random.uniform(0.0, max(0.1, base_wait * 0.25)))
                time.sleep(wait)
                continue
            return None, last_status or -1, f"request_error:{e}"

    return None, last_status or -1, "unexpected_retry_exit"


def fetch_ohlcv_daily(
    sess: requests.Session,
    api_key: str,
    ticker: str,
    start: str,
    end: str,
    adjusted: bool,
    limit: int,
    timeout: int,
    max_pages: int,
    max_retries: int,
    backoff_base: float,
    backoff_max: float,
) -> tuple[pd.DataFrame, int, str, int]:
    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
    params = {
        "adjusted": str(adjusted).lower(),
        "sort": "asc",
        "limit": limit,
        "apiKey": api_key,
    }

    pages = 0
    all_records: List[Dict[str, Any]] = []

    while True:
        pages += 1
        payload, status, msg = request_json_with_retry(
            sess=sess,
            url=url,
            params=params,
            timeout=timeout,
            max_retries=max_retries,
            backoff_base=backoff_base,
            backoff_max=backoff_max,
        )
        if status != 200 or payload is None:
            return pd.DataFrame(), status, msg, pages

        res = payload.get("results") or []
        if res:
            all_records.extend(res)

        next_url = payload.get("next_url")
        if not next_url:
            break
        if pages >= max_pages:
            return pd.DataFrame(), 206, f"partial:max_pages={max_pages}", pages

        url = next_url
        params = {"apiKey": api_key}

    return parse_results(all_records, ticker), 200, "ok", pages


def process_ticker_rest(
    ticker: str,
    outdir: Path,
    run_dir: Path,
    api_key: str,
    start: str,
    end: str,
    adjusted: bool,
    limit: int,
    timeout: int,
    max_pages: int,
    max_retries: int,
    backoff_base: float,
    backoff_max: float,
    prune_obsolete_years: bool,
    resume: bool,
    resume_validate: bool,
) -> Dict[str, Any]:
    cp_path = ticker_checkpoint_path(run_dir, ticker)

    if resume and cp_path.exists():
        c = load_checkpoint(cp_path)
        if c is not None and checkpoint_matches_run(c, ticker=ticker, start=start, end=end, adjusted=adjusted):
            if (not resume_validate) or is_existing_checkpoint_valid(cp_path, ticker, start, end, adjusted):
                return {
                    "ticker": ticker,
                    "status": "resume-skip",
                    "http_status": 200,
                    "rows": int(c.get("rows", 0)),
                    "pages": 0,
                    "msg": "existing_checkpoint",
                }

    sess = get_session(api_key)
    d, http_status, msg, pages = fetch_ohlcv_daily(
        sess=sess,
        api_key=api_key,
        ticker=ticker,
        start=start,
        end=end,
        adjusted=adjusted,
        limit=limit,
        timeout=timeout,
        max_pages=max_pages,
        max_retries=max_retries,
        backoff_base=backoff_base,
        backoff_max=backoff_max,
    )

    if http_status != 200:
        return {
            "ticker": ticker,
            "status": "error",
            "http_status": http_status,
            "rows": 0,
            "pages": pages,
            "msg": msg,
        }

    rows, files_written = upsert_ticker_year_partitions(d, outdir, ticker, prune_obsolete_years=prune_obsolete_years)
    ckp = build_checkpoint(
        ticker=ticker,
        start=start,
        end=end,
        adjusted=adjusted,
        rows=rows,
        files_written=files_written,
        pages=pages,
        http_status=200,
        outdir=outdir,
    )
    write_json(cp_path, ckp)

    return {
        "ticker": ticker,
        "status": "ok",
        "http_status": 200,
        "rows": int(rows),
        "pages": pages,
        "msg": "ok",
    }

def parse_date_from_path(path: str) -> datetime.date | None:
    p = Path(path)
    candidates: List[str] = []
    if p.stem:
        candidates.append(p.stem)
    if p.name:
        candidates.append(p.name)
    candidates.extend(list(p.parts)[-3:])
    candidates.append(path)

    for text in candidates:
        m = DATE_RE.search(text)
        if not m:
            continue
        s = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        try:
            return parse_iso_date(s)
        except Exception:
            continue
    return None


def list_flatfiles_paths(root: str, storage_options: Dict[str, Any]) -> List[str]:
    is_s3 = root.startswith("s3://")
    has_glob = any(ch in root for ch in "*?[")

    if is_s3:
        try:
            import fsspec
        except Exception as e:
            raise RuntimeError(f"fsspec/s3fs no disponible para S3: {e}") from e

        fs, _, _ = fsspec.get_fs_token_paths(root, storage_options=storage_options)
        if has_glob:
            paths = fs.glob(root)
        else:
            base = root[len("s3://") :].rstrip("/")
            paths = fs.find(base)
        return [f"s3://{p}" if not str(p).startswith("s3://") else str(p) for p in paths]

    p = Path(root)
    if not p.exists():
        raise RuntimeError(f"Ruta flatfiles no existe: {root}")
    if p.is_file():
        return [str(p)]

    exts = {".parquet", ".csv", ".gz"}
    out: List[str] = []
    for f in p.rglob("*"):
        if f.is_file() and f.suffix.lower() in exts:
            out.append(str(f))
    return out


def read_any_table(path: str, storage_options: Dict[str, Any]) -> pd.DataFrame:
    lower = path.lower()
    if lower.endswith(".parquet"):
        return pd.read_parquet(path, engine="pyarrow", storage_options=storage_options or None)
    if lower.endswith(".csv") or lower.endswith(".csv.gz") or lower.endswith(".gz"):
        return pd.read_csv(path, low_memory=False, storage_options=storage_options or None)

    try:
        return pd.read_parquet(path, engine="pyarrow", storage_options=storage_options or None)
    except Exception:
        return pd.read_csv(path, low_memory=False, storage_options=storage_options or None)


def normalize_flatfile_df(df: pd.DataFrame, tickers_set: set[str], file_date: datetime.date | None) -> pd.DataFrame:
    tcol = pick_col(df, ["ticker", "sym", "symbol", "T"])
    if tcol is None:
        return pd.DataFrame(columns=["ticker", "date", "year", "o", "h", "l", "c", "v", "vw", "n", "t"])

    out = pd.DataFrame()
    out["ticker"] = df[tcol].astype("string").str.strip().str.upper()
    out = out[out["ticker"].isin(tickers_set)].copy()
    if out.empty:
        return pd.DataFrame(columns=["ticker", "date", "year", "o", "h", "l", "c", "v", "vw", "n", "t"])

    ocol = pick_col(df, ["o", "open"])
    hcol = pick_col(df, ["h", "high"])
    lcol = pick_col(df, ["l", "low"])
    ccol = pick_col(df, ["c", "close"])
    vcol = pick_col(df, ["v", "volume"])
    vwcol = pick_col(df, ["vw", "vwap"])
    ncol = pick_col(df, ["n", "transactions", "trade_count"])
    tscol = pick_col(df, ["t", "window_start", "timestamp", "ts"])
    dcol = pick_col(df, ["date", "session_date", "day"])

    base_idx = out.index

    def src_or_na(col: str | None) -> pd.Series:
        if col is None:
            return pd.Series(pd.NA, index=base_idx)
        return df.loc[base_idx, col]

    out["o"] = pd.to_numeric(src_or_na(ocol), errors="coerce")
    out["h"] = pd.to_numeric(src_or_na(hcol), errors="coerce")
    out["l"] = pd.to_numeric(src_or_na(lcol), errors="coerce")
    out["c"] = pd.to_numeric(src_or_na(ccol), errors="coerce")
    out["v"] = pd.to_numeric(src_or_na(vcol), errors="coerce")
    out["vw"] = pd.to_numeric(src_or_na(vwcol), errors="coerce")
    out["n"] = pd.to_numeric(src_or_na(ncol), errors="coerce")

    if dcol is not None:
        dates = pd.to_datetime(df.loc[base_idx, dcol], utc=True, errors="coerce")
    elif tscol is not None:
        raw = pd.to_numeric(df.loc[base_idx, tscol], errors="coerce")
        max_abs = raw.abs().max(skipna=True)
        if pd.notna(max_abs) and max_abs > 1e16:
            dates = pd.to_datetime(raw, unit="ns", utc=True, errors="coerce")
        elif pd.notna(max_abs) and max_abs > 1e13:
            dates = pd.to_datetime(raw, unit="us", utc=True, errors="coerce")
        else:
            dates = pd.to_datetime(raw, unit="ms", utc=True, errors="coerce")
    elif file_date is not None:
        dates = pd.Series(pd.Timestamp(file_date, tz="UTC"), index=base_idx)
    else:
        dates = pd.Series(pd.NaT, index=base_idx)

    out["date"] = pd.to_datetime(dates, utc=True, errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.dropna(subset=["date"])
    out["year"] = pd.to_datetime(out["date"], errors="coerce").dt.year.astype("Int64")
    out = out.dropna(subset=["year"])
    out["year"] = out["year"].astype(int)

    t_ms = pd.to_datetime(out["date"], utc=True, errors="coerce").astype("int64") // 10**6
    out["t"] = t_ms

    out = out[["ticker", "date", "year", "o", "h", "l", "c", "v", "vw", "n", "t"]]
    out = out.sort_values(["ticker", "date"]).drop_duplicates(subset=["ticker", "date"])
    return out


def run_flatfiles_pipeline(
    *,
    input_tickers: List[str],
    outdir: Path,
    run_dir: Path,
    start: str,
    end: str,
    adjusted: bool,
    flatfiles_root: str,
    flatfiles_storage_options: Dict[str, Any],
    prune_obsolete_years: bool,
    progress_path: Path,
    resume: bool,
    resume_validate: bool,
    flatfiles_max_read_errors: int,
    flatfiles_fail_on_no_rows: bool,
) -> Dict[str, Any]:
    audits: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    pending_tickers: List[str] = []
    for tk in input_tickers:
        cp_path = ticker_checkpoint_path(run_dir, tk)
        if resume and cp_path.exists():
            c = load_checkpoint(cp_path)
            if c is not None and checkpoint_matches_run(c, ticker=tk, start=start, end=end, adjusted=adjusted):
                if (not resume_validate) or is_existing_checkpoint_valid(cp_path, tk, start, end, adjusted):
                    audits.append(
                        {
                            "ticker": tk,
                            "status": "resume-skip",
                            "http_status": 200,
                            "rows": int(c.get("rows", 0)),
                            "pages": 0,
                            "msg": "existing_checkpoint",
                        }
                    )
                    continue
        pending_tickers.append(tk)

    if not pending_tickers:
        return {"audits": audits, "errors": errors, "rows_total": int(sum(int(a.get("rows", 0)) for a in audits)), "source_used": "flatfiles"}

    tickers_set = set(pending_tickers)
    start_d = parse_iso_date(start)
    end_d = parse_iso_date(end)

    paths = list_flatfiles_paths(flatfiles_root, flatfiles_storage_options)
    candidates: List[tuple[datetime.date, str]] = []
    for p in paths:
        d = parse_date_from_path(p)
        if d is None:
            continue
        if start_d <= d <= end_d:
            candidates.append((d, p))

    candidates.sort(key=lambda x: (x[0], x[1]))
    if not candidates:
        raise RuntimeError("No se encontraron flatfiles en rango de fechas solicitado")

    print(f"flatfiles_candidates={len(candidates)}")

    staging = outdir / "_staging_flatfiles"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    processed = 0
    files_errors = 0
    for d, p in candidates:
        try:
            raw = read_any_table(p, flatfiles_storage_options)
            ndf = normalize_flatfile_df(raw, tickers_set, d)
            if not ndf.empty:
                ndf.to_parquet(
                    staging,
                    engine="pyarrow",
                    index=False,
                    partition_cols=["ticker", "year"],
                    compression="zstd",
                )
                total_rows += len(ndf)
        except Exception as e:
            files_errors += 1
            print(f"warn flatfile_read_error path={p} err={e}")

        processed += 1
        if processed % 100 == 0 or processed == len(candidates):
            write_json(
                progress_path,
                {
                    "status": "running_flatfiles",
                    "start": start,
                    "end": end,
                    "processed_files": processed,
                    "total_files": len(candidates),
                    "rows_total": int(total_rows),
                    "file_errors": int(files_errors),
                    "updated_at_utc": utc_now(),
                    "outdir": str(outdir),
                },
            )

    if files_errors > int(flatfiles_max_read_errors):
        raise RuntimeError(f"Flatfiles con errores de lectura: {files_errors} ficheros fallidos")

    if total_rows == 0:
        raise RuntimeError("Flatfiles procesados pero 0 filas para el universo de tickers")

    done = 0
    for tk in pending_tickers:
        ticker_dir = staging / f"ticker={tk}"
        cp_path = ticker_checkpoint_path(run_dir, tk)

        if not ticker_dir.exists():
            if flatfiles_fail_on_no_rows:
                err = {
                    "ticker": tk,
                    "status": "error",
                    "http_status": 424,
                    "rows": 0,
                    "pages": 0,
                    "msg": "flatfiles_no_rows",
                }
                errors.append(err)
                audits.append(err)
                done += 1
                continue

            # Modo seguro: no certificar "OK" usando datos viejos si flatfiles no trae filas.
            # También limpiamos años locales para evitar contaminación silenciosa.
            base = outdir / f"ticker={tk}"
            if base.exists():
                for child in base.iterdir():
                    if child.is_dir() and child.name.startswith("year="):
                        shutil.rmtree(child, ignore_errors=True)

            err = {
                "ticker": tk,
                "status": "error",
                "http_status": 424,
                "rows": 0,
                "pages": 0,
                "msg": "flatfiles_no_rows_nonfatal",
            }
            errors.append(err)
            audits.append(err)
            done += 1
            continue

        try:
            year_frames: List[pd.DataFrame] = []
            files_written = 0
            for yd in sorted(ticker_dir.glob("year=*")):
                y = int(yd.name.split("=", 1)[1])
                dd = pd.read_parquet(yd)
                if dd.empty:
                    continue
                dd = dd.sort_values("date").drop_duplicates(subset=["ticker", "date"])
                final = outdir / f"ticker={tk}" / f"year={y}" / f"day_aggs_{tk}_{y}.parquet"
                atomic_write_parquet(dd, final)
                files_written += 1
                year_frames.append(dd)

            if prune_obsolete_years:
                expected = {f"year={int(df['year'].iloc[0])}" for df in year_frames if not df.empty}
                base = outdir / f"ticker={tk}"
                if base.exists():
                    for child in base.iterdir():
                        if child.is_dir() and child.name.startswith("year=") and child.name not in expected:
                            for p in child.rglob("*.parquet"):
                                p.unlink()
                            try:
                                child.rmdir()
                            except OSError:
                                pass

            rows = int(sum(len(df) for df in year_frames))
            ckp = build_checkpoint(
                ticker=tk,
                start=start,
                end=end,
                adjusted=adjusted,
                rows=rows,
                files_written=files_written,
                pages=0,
                http_status=200,
                outdir=outdir,
            )
            write_json(cp_path, ckp)
            audits.append({"ticker": tk, "status": "ok", "http_status": 200, "rows": rows, "pages": 0, "msg": "ok_flatfiles"})
        except Exception as e:
            err = {"ticker": tk, "status": "error", "http_status": -1, "rows": 0, "pages": 0, "msg": f"flatfiles_consolidation_error:{e}"}
            errors.append(err)
            audits.append(err)

        done += 1
        if done % 250 == 0 or done == len(pending_tickers):
            write_json(
                progress_path,
                {
                    "status": "running_flatfiles_finalize",
                    "start": start,
                    "end": end,
                    "done_tickers": done,
                    "total_tickers": len(pending_tickers),
                    "updated_at_utc": utc_now(),
                    "outdir": str(outdir),
                },
            )

    return {
        "audits": audits,
        "errors": errors,
        "rows_total": int(sum(int(a.get("rows", 0)) for a in audits)),
        "source_used": "flatfiles",
    }

def maybe_write_progress(
    progress_path: Path,
    payload: Dict[str, Any],
    force: bool,
    done: int,
    progress_every: int,
    now_ts: float,
    last_write_ts: float,
    progress_seconds: int,
) -> float:
    if force or (done % max(1, progress_every) == 0) or ((now_ts - last_write_ts) >= progress_seconds):
        write_json(progress_path, payload)
        return now_ts
    return last_write_ts


def run_rest_pipeline(args: argparse.Namespace, tickers: List[str], outdir: Path, run_dir: Path, progress_path: Path) -> Dict[str, Any]:
    total = len(tickers)
    start_ts = time.time()
    last_progress_write_ts = time.time()
    done = 0
    rows_total = 0
    errors: List[Dict[str, Any]] = []
    audits: List[Dict[str, Any]] = []

    for b0 in range(0, total, args.batch_size):
        batch = tickers[b0 : b0 + args.batch_size]
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {
                ex.submit(
                    process_ticker_rest,
                    ticker=tk,
                    outdir=outdir,
                    run_dir=run_dir,
                    api_key=args.api_key,
                    start=args.start,
                    end=args.end,
                    adjusted=args.adjusted,
                    limit=args.limit,
                    timeout=args.timeout,
                    max_pages=args.max_pages,
                    max_retries=args.max_retries,
                    backoff_base=args.backoff_base,
                    backoff_max=args.backoff_max,
                    prune_obsolete_years=args.prune_obsolete_years,
                    resume=args.resume,
                    resume_validate=args.resume_validate,
                ): tk
                for tk in batch
            }

            for fut in as_completed(futures):
                try:
                    res = fut.result()
                except Exception as e:
                    tk = futures[fut]
                    res = {"ticker": tk, "status": "error", "http_status": -1, "rows": 0, "pages": 0, "msg": f"worker_exception:{e}"}

                done += 1
                rows_total += int(res.get("rows", 0))
                if res["status"] == "error":
                    errors.append(res)

                audits.append(
                    {
                        "ticker": res["ticker"],
                        "status": res["status"],
                        "http_status": res["http_status"],
                        "rows": res["rows"],
                        "pages": res["pages"],
                        "msg": res["msg"],
                    }
                )

                elapsed = max(1e-9, time.time() - start_ts)
                rate = done / elapsed
                eta = int((total - done) / rate) if rate > 0 else -1
                if done % max(1, args.progress_every) == 0 or done == total:
                    print(f"ticker {done}/{total} ({100.0*done/total:.2f}%) last={res['ticker']} rows={res['rows']} status={res['http_status']} eta={eta}s")

                payload = {
                    "status": "running_rest",
                    "source": "rest",
                    "start": args.start,
                    "end": args.end,
                    "done_tickers": done,
                    "total_tickers": total,
                    "progress_pct": round(100.0 * done / total, 4),
                    "rows_total": rows_total,
                    "updated_at_utc": utc_now(),
                    "last_ticker": res["ticker"],
                    "last_http_status": res["http_status"],
                    "last_rows": res["rows"],
                    "last_msg": res["msg"],
                    "outdir": str(outdir),
                }
                now_ts = time.time()
                last_progress_write_ts = maybe_write_progress(
                    progress_path=progress_path,
                    payload=payload,
                    force=False,
                    done=done,
                    progress_every=args.progress_every,
                    now_ts=now_ts,
                    last_write_ts=last_progress_write_ts,
                    progress_seconds=args.progress_seconds,
                )

    return {"audits": audits, "errors": errors, "rows_total": rows_total, "source_used": "rest"}


def merge_flatfiles_with_rest_fallback(flat_res: Dict[str, Any], rest_res: Dict[str, Any]) -> Dict[str, Any]:
    flat_audits = list(flat_res.get("audits", []))
    flat_errors = list(flat_res.get("errors", []))
    rest_audits = list(rest_res.get("audits", []))
    rest_errors = list(rest_res.get("errors", []))

    rest_by_ticker: Dict[str, Dict[str, Any]] = {}
    for a in rest_audits:
        tk = str(a.get("ticker", "")).upper()
        if tk:
            rest_by_ticker[tk] = a

    merged_audits: List[Dict[str, Any]] = []
    for a in flat_audits:
        tk = str(a.get("ticker", "")).upper()
        if tk and tk in rest_by_ticker:
            merged_audits.append(rest_by_ticker.pop(tk))
        else:
            merged_audits.append(a)
    merged_audits.extend(rest_by_ticker.values())

    rest_error_tickers = {str(e.get("ticker", "")).upper() for e in rest_errors if e.get("ticker")}
    keep_flat_errors = [e for e in flat_errors if str(e.get("ticker", "")).upper() in rest_error_tickers]
    merged_errors = keep_flat_errors + rest_errors

    rows_total = int(sum(int(a.get("rows", 0)) for a in merged_audits))
    return {
        "audits": merged_audits,
        "errors": merged_errors,
        "rows_total": rows_total,
        "source_used": "flatfiles+rest_fallback",
    }


def parse_storage_options(raw: str) -> Dict[str, Any]:
    if not raw.strip():
        return {}
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as e:
        raise SystemExit(f"--flatfiles-storage-options no es JSON valido: {e}") from e
    if not isinstance(obj, dict):
        raise SystemExit("--flatfiles-storage-options debe ser un JSON object")
    return obj


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Parquet con columna ticker")
    ap.add_argument("--outdir", required=True, help="Output root (recomendado SSD local)")
    ap.add_argument("--start", default="2005-01-01")
    ap.add_argument("--end", default="2026-12-31")
    ap.add_argument("--adjusted", action=argparse.BooleanOptionalAction, default=True)

    ap.add_argument("--source", choices=["auto", "flatfiles", "rest"], default="auto")
    ap.add_argument("--flatfiles-root", default="s3://flatfiles/us_stocks_sip/day_aggs_v1", help="S3 o carpeta local de ficheros diarios (Polygon File Browser / Flat Files)")
    ap.add_argument("--flatfiles-storage-options", default="", help='JSON para fsspec/s3fs (ej: {"key":"...","secret":"..."})')
    ap.add_argument(
        "--flatfiles-adjusted-assumption",
        choices=["true", "false", "unknown"],
        default="unknown",
        help="Supuesto de ajuste por splits en flatfiles. Si no coincide con --adjusted, se evita usar flatfiles.",
    )
    ap.add_argument("--flatfiles-max-read-errors", type=int, default=0, help="Maximo de errores de lectura de ficheros flatfiles permitidos antes de fallar.")
    ap.add_argument("--flatfiles-fail-on-no-rows", action="store_true", help="Marca error por ticker si no aparecen filas en flatfiles para el rango.")

    ap.add_argument("--batch-size", type=int, default=200)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--limit", type=int, default=50000)
    ap.add_argument("--max-pages", type=int, default=1000)
    ap.add_argument("--timeout", type=int, default=40)
    ap.add_argument("--max-retries", type=int, default=6)
    ap.add_argument("--backoff-base", type=float, default=1.0)
    ap.add_argument("--backoff-max", type=float, default=30.0)

    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--resume-validate", action="store_true")
    ap.add_argument("--prune-obsolete-years", action="store_true", help="Borra year=* no presentes en la descarga actual (por defecto NO borra).")

    ap.add_argument("--max-rows", type=int, default=None, help="Limite de tickers para prueba")
    ap.add_argument("--progress-every", type=int, default=25)
    ap.add_argument("--progress-seconds", type=int, default=20)
    ap.add_argument("--api-key", default=os.getenv("POLYGON_API_KEY", ""))
    args = ap.parse_args()

    in_path = Path(args.input)
    outdir = Path(args.outdir)
    run_dir = outdir / "_run"
    progress_path = run_dir / "download_ohlcv_daily_v1.progress.json"
    errors_path = run_dir / "download_ohlcv_daily_v1.errors.csv"
    ticker_audit_path = run_dir / "download_ohlcv_daily_v1.ticker_audit.csv"

    tickers = load_tickers(in_path)
    if args.max_rows:
        tickers = tickers[: args.max_rows]

    total = len(tickers)
    if total == 0:
        raise SystemExit("No hay tickers en input")

    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "checkpoints").mkdir(parents=True, exist_ok=True)

    write_json(
        progress_path,
        {
            "status": "running",
            "source": args.source,
            "start": args.start,
            "end": args.end,
            "done_tickers": 0,
            "total_tickers": total,
            "progress_pct": 0.0,
            "rows_total": 0,
            "updated_at_utc": utc_now(),
            "outdir": str(outdir),
        },
    )

    result: Dict[str, Any] | None = None
    source_errors: List[str] = []

    if args.source in {"auto", "flatfiles"}:
        try:
            if args.flatfiles_adjusted_assumption != "unknown":
                flatfiles_adjusted = args.flatfiles_adjusted_assumption == "true"
                if bool(flatfiles_adjusted) != bool(args.adjusted):
                    raise RuntimeError(
                        f"flatfiles_adjusted_assumption={flatfiles_adjusted} no coincide con --adjusted={args.adjusted}"
                    )
            storage_options = parse_storage_options(args.flatfiles_storage_options)
            result = run_flatfiles_pipeline(
                input_tickers=tickers,
                outdir=outdir,
                run_dir=run_dir,
                start=args.start,
                end=args.end,
                adjusted=args.adjusted,
                flatfiles_root=args.flatfiles_root,
                flatfiles_storage_options=storage_options,
                prune_obsolete_years=args.prune_obsolete_years,
                progress_path=progress_path,
                resume=args.resume,
                resume_validate=args.resume_validate,
                flatfiles_max_read_errors=args.flatfiles_max_read_errors,
                flatfiles_fail_on_no_rows=args.flatfiles_fail_on_no_rows,
            )
            print("source_selected=flatfiles")

            if args.source == "auto":
                ff_errors = list(result.get("errors", []))
                failed_tickers = sorted(
                    {
                        str(e.get("ticker", "")).upper()
                        for e in ff_errors
                        if e.get("ticker")
                    }
                )
                if failed_tickers:
                    if not args.api_key:
                        source_errors.append(
                            f"rest_fallback_skipped:no_api_key failed_tickers={len(failed_tickers)}"
                        )
                        print(
                            f"warn auto fallback REST skipped (no api key), failed_tickers={len(failed_tickers)}"
                        )
                    else:
                        print(
                            f"auto_fallback_rest_for_failed_tickers={len(failed_tickers)}"
                        )
                        rest_fallback = run_rest_pipeline(
                            args=args,
                            tickers=failed_tickers,
                            outdir=outdir,
                            run_dir=run_dir,
                            progress_path=progress_path,
                        )
                        result = merge_flatfiles_with_rest_fallback(result, rest_fallback)
        except Exception as e:
            source_errors.append(f"flatfiles_error:{e}")
            if args.source == "flatfiles":
                raise
            print(f"warn flatfiles failed, fallback to rest: {e}")

    if result is None:
        if not args.api_key:
            raise SystemExit("Falta API key: usa --api-key o POLYGON_API_KEY. Necesaria para fallback REST cuando flatfiles no funciona.")
        result = run_rest_pipeline(args, tickers, outdir, run_dir, progress_path)
        print("source_selected=rest")

    audits = result.get("audits", [])
    errors = result.get("errors", [])
    rows_total = int(result.get("rows_total", 0))

    pd.DataFrame(audits).to_csv(ticker_audit_path, index=False)
    if errors:
        pd.DataFrame(errors).to_csv(errors_path, index=False)
    elif errors_path.exists():
        errors_path.unlink()

    final_status = "completed" if not errors else "completed_with_errors"
    write_json(
        progress_path,
        {
            "status": final_status,
            "source_used": result.get("source_used"),
            "start": args.start,
            "end": args.end,
            "done_tickers": len(audits),
            "total_tickers": total,
            "progress_pct": round(100.0 * len(audits) / max(1, total), 4),
            "rows_total": rows_total,
            "updated_at_utc": utc_now(),
            "errors": len(errors),
            "source_errors": source_errors,
            "errors_path": str(errors_path) if errors else None,
            "ticker_audit_path": str(ticker_audit_path),
            "outdir": str(outdir),
        },
    )

    print(f"done_tickers={len(audits)}/{total}")
    print(f"rows_total={rows_total:,}")
    print(f"progress={progress_path}")
    print(f"ticker_audit={ticker_audit_path}")
    if errors:
        print(f"errors={errors_path}")


if __name__ == "__main__":
    main()
