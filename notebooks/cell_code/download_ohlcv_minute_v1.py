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


def flatfiles_file_manifest_path(run_dir: Path, ticker: str, year: int, month: int) -> Path:
    return run_dir / "flatfiles_files" / f"ticker={ticker}" / f"year={year}" / f"month={month:02d}.json"


def flatfiles_month_manifest_path(run_dir: Path, year: int, month: int) -> Path:
    return run_dir / "flatfiles_months" / f"year={year}" / f"month={month:02d}.json"


def resolve_out_file(cp_path: Path, p: str) -> Path:
    path = Path(p)
    if path.is_absolute():
        return path
    for parent in cp_path.parents:
        if parent.name == "_run":
            return parent.parent / path
    return cp_path.parent / path


def load_checkpoint(cp_path: Path) -> Dict[str, Any] | None:
    if not cp_path.exists():
        return None
    try:
        return json.loads(cp_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def checkpoint_matches_run(
    c: Dict[str, Any],
    ticker: str,
    start: str,
    end: str,
    adjusted: bool,
    allow_partial: bool,
) -> bool:
    required = [
        "ticker",
        "status",
        "start",
        "end",
        "out_files",
        "adjusted",
        "rows",
        "files_written",
        "partial",
        "min_date",
        "max_date",
    ]
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
    if (not allow_partial) and bool(c.get("partial", False)):
        return False
    return True


def validate_parquet_shape(path: Path) -> tuple[int, bool]:
    try:
        d = pd.read_parquet(path)
    except Exception:
        return 0, False
    req = {"ticker", "ts_utc", "date", "year", "month", "o", "h", "l", "c", "v", "vw", "n", "t"}
    if not req.issubset(set(d.columns)):
        return len(d), False
    if d.empty:
        return 0, True
    ok = d["ticker"].notna().all() and d["t"].notna().all() and d["ts_utc"].notna().all()
    return len(d), bool(ok)


def validate_month_file(path: Path, ticker: str, year: int, month: int) -> tuple[int, bool, str | None, str | None]:
    try:
        d = pd.read_parquet(path)
    except Exception:
        return 0, False, None, None
    req = {"ticker", "ts_utc", "date", "year", "month", "o", "h", "l", "c", "v", "vw", "n", "t"}
    if not req.issubset(set(d.columns)):
        return len(d), False, None, None
    if d.empty:
        return 0, True, None, None
    if not d["ticker"].astype("string").str.upper().eq(ticker.upper()).all():
        return len(d), False, None, None
    if not pd.to_numeric(d["year"], errors="coerce").eq(int(year)).all():
        return len(d), False, None, None
    if not pd.to_numeric(d["month"], errors="coerce").eq(int(month)).all():
        return len(d), False, None, None
    s = d["date"].dropna().astype("string")
    if s.empty:
        return len(d), False, None, None
    return len(d), True, str(s.min()), str(s.max())


def build_flatfiles_file_manifest(
    *,
    ticker: str,
    year: int,
    month: int,
    start: str,
    end: str,
    adjusted: bool,
    rows: int,
    out_file: Path,
    outdir: Path,
    min_date: str | None,
    max_date: str | None,
) -> Dict[str, Any]:
    return {
        "ticker": ticker,
        "year": int(year),
        "month": int(month),
        "status": "complete",
        "source": "flatfiles",
        "start": start,
        "end": end,
        "adjusted": bool(adjusted),
        "rows": int(rows),
        "out_file": str(out_file.relative_to(outdir)),
        "min_date": min_date,
        "max_date": max_date,
        "updated_at_utc": utc_now(),
    }


def is_existing_flatfiles_file_valid(
    manifest_path: Path,
    *,
    ticker: str,
    year: int,
    month: int,
    start: str,
    end: str,
    adjusted: bool,
) -> bool:
    c = load_checkpoint(manifest_path)
    if c is None:
        return False
    required = ["ticker", "year", "month", "status", "source", "start", "end", "adjusted", "rows", "out_file"]
    if any(k not in c for k in required):
        return False
    if str(c["ticker"]).upper() != ticker.upper():
        return False
    if int(c["year"]) != int(year) or int(c["month"]) != int(month):
        return False
    if c["status"] != "complete" or c["source"] != "flatfiles":
        return False
    if str(c["start"]) != start or str(c["end"]) != end:
        return False
    if bool(c["adjusted"]) != bool(adjusted):
        return False
    out_file = resolve_out_file(manifest_path, str(c["out_file"]))
    rows, ok, min_date, max_date = validate_month_file(out_file, ticker=ticker, year=year, month=month)
    if not ok or rows != int(c["rows"]):
        return False
    if rows > 0:
        if min_date is None or max_date is None:
            return False
        if str(c.get("min_date")) != min_date or str(c.get("max_date")) != max_date:
            return False
        if min_date < start or max_date > end:
            return False
    return True


def is_existing_checkpoint_valid(
    cp_path: Path,
    ticker: str,
    start: str,
    end: str,
    adjusted: bool,
    allow_partial: bool,
) -> bool:
    c = load_checkpoint(cp_path)
    if c is None or not checkpoint_matches_run(
        c,
        ticker=ticker,
        start=start,
        end=end,
        adjusted=adjusted,
        allow_partial=allow_partial,
    ):
        return False
    expected_rows = int(c.get("rows", 0))
    out_files = c.get("out_files") or []
    if not isinstance(out_files, list):
        return False
    rows = 0
    min_date: str | None = None
    max_date: str | None = None
    for p in out_files:
        fp = resolve_out_file(cp_path, str(p))
        if not fp.exists():
            return False
        n, ok = validate_parquet_shape(fp)
        if not ok:
            return False
        rows += n
        try:
            dd = pd.read_parquet(fp, columns=["date"])
            if "date" in dd.columns and not dd.empty:
                s = dd["date"].dropna().astype("string")
                if not s.empty:
                    pmin = str(s.min())
                    pmax = str(s.max())
                    if min_date is None or pmin < min_date:
                        min_date = pmin
                    if max_date is None or pmax > max_date:
                        max_date = pmax
        except Exception:
            return False
    if rows != expected_rows:
        return False
    if rows > 0:
        if min_date is None or max_date is None:
            return False
        # PTI: debe estar dentro de la ventana solicitada
        if min_date < start or max_date > end:
            return False
        # Coherencia contra checkpoint
        if str(c.get("min_date")) != min_date or str(c.get("max_date")) != max_date:
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
    partial: bool,
    min_date: str | None,
    max_date: str | None,
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
        "partial": bool(partial),
        "min_date": min_date,
        "max_date": max_date,
        "updated_at_utc": utc_now(),
        "out_files": out_files,
    }


def get_session(api_key: str) -> requests.Session:
    sess = getattr(_THREAD_LOCAL, "session", None)
    if sess is None:
        sess = requests.Session()
        sess.headers.update({"Authorization": f"Bearer {api_key}"})
        _THREAD_LOCAL.session = sess
    return sess


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


def request_json_with_retry(sess: requests.Session, url: str, params: Dict[str, Any], timeout: int, max_retries: int, backoff_base: float, backoff_max: float) -> tuple[Dict[str, Any] | None, int, str]:
    last_status = 0
    for attempt in range(max_retries + 1):
        try:
            r = sess.get(url, params=params, timeout=timeout)
            last_status = r.status_code
            if r.status_code == 200:
                try:
                    return r.json(), 200, "ok"
                except ValueError as e:
                    if attempt < max_retries:
                        exp = min(backoff_max, backoff_base * (2**attempt))
                        wait = min(backoff_max, exp + random.uniform(0.0, max(0.1, exp * 0.25)))
                        time.sleep(wait)
                        continue
                    return None, 200, f"json_decode_error:{e}"
            if r.status_code == 429 or r.status_code >= 500:
                if attempt < max_retries:
                    ra = read_retry_after(r)
                    exp = min(backoff_max, backoff_base * (2**attempt))
                    if ra > 0:
                        wait = max(ra, exp) + random.uniform(0.0, 2.0)
                    else:
                        wait = min(backoff_max, exp + random.uniform(0.0, max(0.1, exp * 0.25)))
                    time.sleep(wait)
                    continue
                return None, r.status_code, f"http_{r.status_code}_retries_exhausted"
            return None, r.status_code, f"http_{r.status_code}"
        except requests.RequestException as e:
            if attempt < max_retries:
                exp = min(backoff_max, backoff_base * (2**attempt))
                wait = min(backoff_max, exp + random.uniform(0.0, max(0.1, exp * 0.25)))
                time.sleep(wait)
                continue
            return None, last_status or -1, f"request_error:{e}"
    return None, last_status or -1, "unexpected_retry_exit"


def parse_results(records: List[Dict[str, Any]], ticker: str) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=["ticker", "ts_utc", "date", "year", "month", "o", "h", "l", "c", "v", "vw", "n", "t"])
    d = pd.DataFrame.from_records(records)
    for c in ["o", "h", "l", "c", "v", "vw", "n", "t"]:
        if c not in d.columns:
            d[c] = pd.NA
    ts = pd.to_datetime(d["t"], unit="ms", utc=True, errors="coerce")
    d["ts_utc"] = ts.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    d["date"] = ts.dt.strftime("%Y-%m-%d")
    d["year"] = ts.dt.year.astype("Int64")
    d["month"] = ts.dt.month.astype("Int64")
    d["ticker"] = ticker
    out = d[["ticker", "ts_utc", "date", "year", "month", "o", "h", "l", "c", "v", "vw", "n", "t"]].copy()
    out = out.dropna(subset=["t", "ts_utc", "year", "month"]) 
    out["year"] = out["year"].astype(int)
    out["month"] = out["month"].astype(int)
    out = out.sort_values("t").drop_duplicates(subset=["ticker", "t"])
    return out


def fetch_ohlcv_minute(
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
    max_records_in_memory: int,
) -> tuple[pd.DataFrame, int, str, int]:
    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/minute/{start}/{end}"
    params = {"adjusted": str(adjusted).lower(), "sort": "asc", "limit": limit, "apiKey": api_key}
    pages = 0
    all_records: List[Dict[str, Any]] = []
    while True:
        pages += 1
        payload, status, msg = request_json_with_retry(sess, url, params, timeout, max_retries, backoff_base, backoff_max)
        if status != 200 or payload is None:
            return pd.DataFrame(), status, msg, pages
        res = payload.get("results") or []
        if res:
            all_records.extend(res)
            if len(all_records) > max_records_in_memory:
                d = parse_results(all_records, ticker)
                return d, 206, f"partial:memory_guard:max_records_in_memory={max_records_in_memory}", pages
        next_url = payload.get("next_url")
        if not next_url:
            break
        if pages >= max_pages:
            d = parse_results(all_records, ticker)
            return d, 206, f"partial:max_pages={max_pages}", pages
        url = next_url
        params = {"apiKey": api_key}
    return parse_results(all_records, ticker), 200, "ok", pages


def upsert_ticker_year_month_partitions(d: pd.DataFrame, outdir: Path, ticker: str, prune_obsolete_months: bool) -> tuple[int, int]:
    base = outdir / f"ticker={ticker}"
    base.mkdir(parents=True, exist_ok=True)
    if d.empty:
        if prune_obsolete_months and base.exists():
            for y in base.glob("year=*"):
                if y.is_dir():
                    for m in y.glob("month=*"):
                        if m.is_dir():
                            for p in m.rglob("*.parquet"):
                                p.unlink()
                            try:
                                m.rmdir()
                            except OSError:
                                pass
                    try:
                        y.rmdir()
                    except OSError:
                        pass
        return 0, 0

    expected = set()
    nonce = f"{int(time.time() * 1000)}_{os.getpid()}_{ticker}"
    staging_root = outdir / "_staging_commit_1m"
    staging_ticker = staging_root / f"ticker={ticker}__{nonce}"
    staging_ticker.mkdir(parents=True, exist_ok=True)

    files = 0
    for (year, month), g in d.groupby(["year", "month"]):
        year = int(year)
        month = int(month)
        ym = (f"year={year}", f"month={month:02d}")
        expected.add(ym)
        final = staging_ticker / ym[0] / ym[1] / f"minute_aggs_{ticker}_{year}_{month:02d}.parquet"
        atomic_write_parquet(g.sort_values("t").drop_duplicates(subset=["ticker", "t"]), final)
        files += 1

    # Si no podas, preserva meses antiguos fuera de expected dentro del staging.
    if (not prune_obsolete_months) and base.exists():
        for y in base.glob("year=*"):
            if not y.is_dir():
                continue
            for m in y.glob("month=*"):
                if not m.is_dir():
                    continue
                key = (y.name, m.name)
                if key in expected:
                    continue
                dst = staging_ticker / y.name / m.name
                dst.parent.mkdir(parents=True, exist_ok=True)
                if dst.exists():
                    shutil.rmtree(dst, ignore_errors=True)
                shutil.copytree(m, dst)

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
        if base.exists():
            shutil.rmtree(base, ignore_errors=True)
        if backup_ticker.exists():
            os.replace(backup_ticker, base)
        raise
    finally:
        if staging_ticker.exists():
            shutil.rmtree(staging_ticker, ignore_errors=True)

    if prune_obsolete_months and base.exists():
        for y in base.glob("year=*"):
            if not y.is_dir():
                continue
            for m in y.glob("month=*"):
                if m.is_dir() and not any(m.iterdir()):
                    try:
                        m.rmdir()
                    except OSError:
                        pass
            if not any(y.iterdir()):
                try:
                    y.rmdir()
                except OSError:
                    pass
    return len(d), files


def process_ticker(ticker: str, outdir: Path, run_dir: Path, api_key: str, start: str, end: str, adjusted: bool, limit: int, timeout: int, max_pages: int, max_retries: int, backoff_base: float, backoff_max: float, prune_obsolete_months: bool, allow_partial: bool, max_records_in_memory: int, resume: bool, resume_validate: bool) -> Dict[str, Any]:
    cp_path = ticker_checkpoint_path(run_dir, ticker)
    if resume and cp_path.exists():
        c = load_checkpoint(cp_path)
        if c is not None and checkpoint_matches_run(
            c,
            ticker=ticker,
            start=start,
            end=end,
            adjusted=adjusted,
            allow_partial=allow_partial,
        ):
            if resume_validate:
                if is_existing_checkpoint_valid(cp_path, ticker, start, end, adjusted, allow_partial=allow_partial):
                    return {"ticker": ticker, "status": "resume-skip-valid", "http_status": 200, "rows": int(c.get("rows", 0)), "pages": 0, "msg": "existing_checkpoint_valid"}
            else:
                return {"ticker": ticker, "status": "resume-skip", "http_status": 200, "rows": int(c.get("rows", 0)), "pages": 0, "msg": "existing_checkpoint"}

    sess = get_session(api_key)
    d, http_status, msg, pages = fetch_ohlcv_minute(
        sess,
        api_key,
        ticker,
        start,
        end,
        adjusted,
        limit,
        timeout,
        max_pages,
        max_retries,
        backoff_base,
        backoff_max,
        max_records_in_memory=max_records_in_memory,
    )
    if http_status not in (200, 206):
        return {"ticker": ticker, "status": "error", "http_status": http_status, "rows": 0, "pages": pages, "msg": msg}
    if http_status == 206 and not allow_partial:
        return {"ticker": ticker, "status": "error", "http_status": 206, "rows": int(len(d)), "pages": pages, "msg": msg}

    rows, files_written = upsert_ticker_year_month_partitions(d, outdir, ticker, prune_obsolete_months)
    min_date = None
    max_date = None
    if rows > 0 and not d.empty:
        min_date = str(d["date"].min())
        max_date = str(d["date"].max())
    ckp = build_checkpoint(
        ticker=ticker,
        start=start,
        end=end,
        adjusted=adjusted,
        rows=rows,
        files_written=files_written,
        pages=pages,
        http_status=http_status,
        outdir=outdir,
        partial=(http_status == 206),
        min_date=min_date,
        max_date=max_date,
    )
    write_json(cp_path, ckp)
    return {"ticker": ticker, "status": "ok", "http_status": http_status, "rows": int(rows), "pages": pages, "msg": "ok_partial" if http_status == 206 else "ok"}


def parse_date_from_path(path: str) -> datetime.date | None:
    p = Path(path)
    candidates = [p.stem, p.name, *list(p.parts)[-3:], path]
    for text in candidates:
        if not text:
            continue
        m = DATE_RE.search(text)
        if not m:
            continue
        try:
            return parse_iso_date(f"{m.group(1)}-{m.group(2)}-{m.group(3)}")
        except Exception:
            continue
    return None


def list_flatfiles_paths(root: str, storage_options: Dict[str, Any]) -> List[str]:
    if root.startswith("s3://"):
        try:
            import fsspec
        except Exception as e:
            raise RuntimeError(f"fsspec/s3fs no disponible para S3: {e}") from e
        fs, _, _ = fsspec.get_fs_token_paths(root, storage_options=storage_options)
        base = root[len("s3://") :].rstrip("/")
        paths = fs.find(base)
        return [f"s3://{p}" if not str(p).startswith("s3://") else str(p) for p in paths]

    p = Path(root)
    if not p.exists():
        raise RuntimeError(f"Ruta flatfiles no existe: {root}")
    if p.is_file():
        return [str(p)]
    return [str(f) for f in p.rglob("*") if f.is_file() and f.suffix.lower() in {".parquet", ".csv", ".gz"}]


def read_any_table(path: str, storage_options: Dict[str, Any]) -> pd.DataFrame:
    lower = path.lower()
    if lower.endswith(".parquet"):
        return pd.read_parquet(path, engine="pyarrow", storage_options=storage_options or None)
    return pd.read_csv(path, low_memory=False, storage_options=storage_options or None)


def normalize_flatfile_df(df: pd.DataFrame, tickers_set: set[str], file_date: datetime.date | None) -> pd.DataFrame:
    tcol = pick_col(df, ["ticker", "sym", "symbol", "T"])
    if tcol is None:
        return pd.DataFrame(columns=["ticker", "ts_utc", "date", "year", "month", "o", "h", "l", "c", "v", "vw", "n", "t"])

    out = pd.DataFrame()
    out["ticker"] = df[tcol].astype("string").str.strip().str.upper()
    out = out[out["ticker"].isin(tickers_set)].copy()
    if out.empty:
        return pd.DataFrame(columns=["ticker", "ts_utc", "date", "year", "month", "o", "h", "l", "c", "v", "vw", "n", "t"])

    base_idx = out.index
    tscol = pick_col(df, ["t", "window_start", "timestamp", "ts"])
    dcol = pick_col(df, ["date", "session_date", "day"])

    if dcol is not None:
        dt = pd.to_datetime(df.loc[base_idx, dcol], utc=True, errors="coerce")
    elif tscol is not None:
        raw = pd.to_numeric(df.loc[base_idx, tscol], errors="coerce")
        max_abs = raw.abs().max(skipna=True)
        if pd.notna(max_abs) and max_abs > 1e16:
            dt = pd.to_datetime(raw, unit="ns", utc=True, errors="coerce")
        elif pd.notna(max_abs) and max_abs > 1e13:
            dt = pd.to_datetime(raw, unit="us", utc=True, errors="coerce")
        else:
            dt = pd.to_datetime(raw, unit="ms", utc=True, errors="coerce")
    elif file_date is not None:
        dt = pd.Series(pd.Timestamp(file_date, tz="UTC"), index=base_idx)
    else:
        dt = pd.Series(pd.NaT, index=base_idx)

    out["ts_utc"] = pd.to_datetime(dt, utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    out["date"] = pd.to_datetime(dt, utc=True, errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.dropna(subset=["ts_utc", "date"])
    out["year"] = pd.to_datetime(out["date"], errors="coerce").dt.year.astype("Int64")
    out["month"] = pd.to_datetime(out["date"], errors="coerce").dt.month.astype("Int64")
    out = out.dropna(subset=["year", "month"])
    out["year"] = out["year"].astype(int)
    out["month"] = out["month"].astype(int)
    out["t"] = pd.to_datetime(out["ts_utc"], utc=True, errors="coerce").astype("int64") // 10**6

    for dst, srcs in [("o", ["o", "open"]), ("h", ["h", "high"]), ("l", ["l", "low"]), ("c", ["c", "close"]), ("v", ["v", "volume"]), ("vw", ["vw", "vwap"]), ("n", ["n", "transactions", "trade_count"])]:
        c = pick_col(df, srcs)
        out[dst] = pd.to_numeric(df.loc[base_idx, c], errors="coerce") if c else pd.NA

    out = out[["ticker", "ts_utc", "date", "year", "month", "o", "h", "l", "c", "v", "vw", "n", "t"]]
    return out.sort_values(["ticker", "t"]).drop_duplicates(subset=["ticker", "t"])


def run_flatfiles_pipeline(args: argparse.Namespace, tickers: List[str], outdir: Path, run_dir: Path, progress_path: Path) -> Dict[str, Any]:
    start_d = parse_iso_date(args.start)
    end_d = parse_iso_date(args.end)
    tickers_set = set(tickers)
    paths = list_flatfiles_paths(args.flatfiles_root, parse_storage_options(args.flatfiles_storage_options))
    candidates = [(d, p) for p in paths for d in [parse_date_from_path(p)] if d is not None and start_d <= d <= end_d]
    candidates.sort(key=lambda x: (x[0], x[1]))
    if not candidates:
        raise RuntimeError("No se encontraron flatfiles en rango")

    staging = outdir / "_staging_flatfiles_1m"
    staging.mkdir(parents=True, exist_ok=True)
    (run_dir / "flatfiles_files").mkdir(parents=True, exist_ok=True)
    (run_dir / "flatfiles_months").mkdir(parents=True, exist_ok=True)

    month_candidates: Dict[tuple[int, int], List[tuple[datetime.date, str]]] = {}
    for d, p in candidates:
        month_candidates.setdefault((d.year, d.month), []).append((d, p))

    files_errors = 0
    tickers_with_rows: set[str] = set()
    completed_files = 0
    total_months = len(month_candidates)
    done_months = 0

    for (year, month) in sorted(month_candidates):
        month_manifest = flatfiles_month_manifest_path(run_dir, year, month)
        print(f"flatfiles month {year}-{month:02d}: source_files={len(month_candidates[(year, month)])}")
        prior = load_checkpoint(month_manifest)
        if args.resume and prior is not None and prior.get("status") == "complete":
            month_tickers = [str(t).upper() for t in (prior.get("tickers") or [])]
            if all(
                is_existing_flatfiles_file_valid(
                    flatfiles_file_manifest_path(run_dir, tk, year, month),
                    ticker=tk,
                    year=year,
                    month=month,
                    start=args.start,
                    end=args.end,
                    adjusted=args.adjusted,
                )
                for tk in month_tickers
                ):
                tickers_with_rows.update(month_tickers)
                completed_files += len(month_tickers)
                done_months += 1
                print(f"flatfiles month {year}-{month:02d}: resume-skip tickers={len(month_tickers)}")
                write_json(
                    progress_path,
                    {
                        "status": "running",
                        "source": args.source,
                        "source_used": "flatfiles",
                        "start": args.start,
                        "end": args.end,
                        "done_months": done_months,
                        "total_months": total_months,
                        "completed_files": completed_files,
                        "updated_at_utc": utc_now(),
                        "outdir": str(outdir),
                    },
                )
                continue

        month_tickers: set[str] = set()
        month_stage_roots: set[Path] = set()
        for d, p in month_candidates[(year, month)]:
            try:
                ndf = normalize_flatfile_df(read_any_table(p, parse_storage_options(args.flatfiles_storage_options)), tickers_set, d)
                if not ndf.empty:
                    month_tickers.update(ndf["ticker"].dropna().astype("string").str.upper().tolist())
                    ndf = ndf[["ticker", "ts_utc", "date", "year", "month", "o", "h", "l", "c", "v", "vw", "n", "t"]]
                    for tk, g in ndf.groupby("ticker"):
                        tk = str(tk).upper()
                        tdir = staging / f"ticker={tk}" / f"year={year}" / f"month={month:02d}"
                        if tdir not in month_stage_roots:
                            if tdir.exists():
                                shutil.rmtree(tdir, ignore_errors=True)
                            tdir.mkdir(parents=True, exist_ok=True)
                            month_stage_roots.add(tdir)
                        shard = tdir / f"{int(time.time() * 1000)}_{random.randint(0, 10**9):09d}.parquet"
                        g.to_parquet(shard, engine="pyarrow", index=False, compression="zstd")
            except Exception:
                files_errors += 1
                if files_errors > args.flatfiles_max_read_errors:
                    raise RuntimeError(f"Flatfiles con errores de lectura: {files_errors}")

        completed_this_month: List[str] = []
        for tk in sorted(month_tickers):
            tdir = staging / f"ticker={tk}" / f"year={year}" / f"month={month:02d}"
            if not tdir.exists():
                continue
            dd = pd.read_parquet(tdir)
            if dd.empty:
                continue
            if "ticker" not in dd.columns:
                dd["ticker"] = tk
            if "year" not in dd.columns:
                dd["year"] = int(year)
            if "month" not in dd.columns:
                dd["month"] = int(month)
            dd = dd.sort_values("t").drop_duplicates(subset=["ticker", "t"])
            final = outdir / f"ticker={tk}" / f"year={year}" / f"month={month:02d}" / f"minute_aggs_{tk}_{year}_{month:02d}.parquet"
            atomic_write_parquet(dd, final)
            min_date = str(dd["date"].min()) if "date" in dd.columns and not dd.empty else None
            max_date = str(dd["date"].max()) if "date" in dd.columns and not dd.empty else None
            write_json(
                flatfiles_file_manifest_path(run_dir, tk, year, month),
                build_flatfiles_file_manifest(
                    ticker=tk,
                    year=year,
                    month=month,
                    start=args.start,
                    end=args.end,
                    adjusted=args.adjusted,
                    rows=len(dd),
                    out_file=final,
                    outdir=outdir,
                    min_date=min_date,
                    max_date=max_date,
                ),
            )
            completed_this_month.append(tk)
            tickers_with_rows.add(tk)
            completed_files += 1

        print(
            f"flatfiles month {year}-{month:02d}: staged_tickers={len(month_tickers)} "
            f"written_files={len(completed_this_month)}"
        )
        write_json(
            month_manifest,
            {
                "status": "complete",
                "source": "flatfiles",
                "year": int(year),
                "month": int(month),
                "tickers": completed_this_month,
                "source_files": len(month_candidates[(year, month)]),
                "updated_at_utc": utc_now(),
            },
        )
        done_months += 1
        write_json(
            progress_path,
            {
                "status": "running",
                "source": args.source,
                "source_used": "flatfiles",
                "start": args.start,
                "end": args.end,
                "done_months": done_months,
                "total_months": total_months,
                "completed_files": completed_files,
                "updated_at_utc": utc_now(),
                "outdir": str(outdir),
            },
        )

    audits: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    for tk in tickers:
        base = outdir / f"ticker={tk}"
        if tk not in tickers_with_rows or not base.exists():
            err = {"ticker": tk, "status": "error", "http_status": 424, "rows": 0, "pages": 0, "msg": "flatfiles_no_rows"}
            errors.append(err)
            audits.append(err)
            continue
        try:
            frames: List[pd.DataFrame] = []
            files_written = 0
            for yd in sorted(base.glob("year=*")):
                for md in sorted(yd.glob("month=*")):
                    dd = pd.read_parquet(md)
                    if dd.empty:
                        continue
                    files_written += 1
                    frames.append(dd)
            rows = int(sum(len(f) for f in frames))
            min_date = str(min(str(f["date"].min()) for f in frames if not f.empty)) if rows > 0 else None
            max_date = str(max(str(f["date"].max()) for f in frames if not f.empty)) if rows > 0 else None
            write_json(
                ticker_checkpoint_path(run_dir, tk),
                build_checkpoint(
                    ticker=tk,
                    start=args.start,
                    end=args.end,
                    adjusted=args.adjusted,
                    rows=rows,
                    files_written=files_written,
                    pages=0,
                    http_status=200,
                    outdir=outdir,
                    partial=False,
                    min_date=min_date,
                    max_date=max_date,
                ),
            )
            audits.append({"ticker": tk, "status": "ok", "http_status": 200, "rows": rows, "pages": 0, "msg": "ok_flatfiles"})
        except Exception as e:
            err = {"ticker": tk, "status": "error", "http_status": -1, "rows": 0, "pages": 0, "msg": f"flatfiles_consolidation_error:{e}"}
            errors.append(err)
            audits.append(err)

    return {"audits": audits, "errors": errors, "rows_total": int(sum(int(a.get("rows", 0)) for a in audits)), "source_used": "flatfiles"}


def run_rest_pipeline(args: argparse.Namespace, tickers: List[str], outdir: Path, run_dir: Path, progress_path: Path) -> Dict[str, Any]:
    total = len(tickers)
    start_ts = time.time()
    done = 0
    rows_total = 0
    errors: List[Dict[str, Any]] = []
    audits: List[Dict[str, Any]] = []
    for b0 in range(0, total, args.batch_size):
        batch = tickers[b0:b0 + args.batch_size]
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {
                ex.submit(
                    process_ticker,
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
                    prune_obsolete_months=args.prune_obsolete_months,
                    allow_partial=args.allow_partial,
                    max_records_in_memory=args.max_records_in_memory,
                    resume=args.resume,
                    resume_validate=args.resume_validate,
                ): tk for tk in batch
            }
            for fut in as_completed(futures):
                try:
                    res = fut.result()
                except Exception as e:
                    tk = futures[fut]
                    res = {"ticker": tk, "status": "error", "http_status": -1, "rows": 0, "pages": 0, "msg": f"worker_exception:{e}"}
                done += 1
                if res["status"] == "error":
                    errors.append(res)
                else:
                    rows_total += int(res.get("rows", 0))
                audits.append(res)
                if done % max(1, args.progress_every) == 0 or done == total:
                    elapsed = max(1e-9, time.time() - start_ts)
                    eta = int((total - done) / (done / elapsed)) if done > 0 else -1
                    print(f"ticker {done}/{total} ({100.0*done/total:.2f}%) last={res['ticker']} rows={res['rows']} status={res['http_status']} eta={eta}s")
    return {"audits": audits, "errors": errors, "rows_total": rows_total, "source_used": "rest"}


def parse_storage_options(raw: str) -> Dict[str, Any]:
    if not raw.strip():
        return {}
    text = raw.strip()
    if text.startswith("@"):
        p = Path(text[1:])
        if not p.exists():
            raise SystemExit(f"--flatfiles-storage-options file no existe: {p}")
        text = p.read_text(encoding="utf-8-sig")
    else:
        p = Path(text)
        if p.exists() and p.is_file():
            text = p.read_text(encoding="utf-8-sig")
    obj = json.loads(text)
    if not isinstance(obj, dict):
        raise SystemExit("--flatfiles-storage-options debe ser JSON object")
    return obj


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--start", default="2005-01-01")
    ap.add_argument("--end", default="2026-12-31")
    ap.add_argument("--adjusted", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--source", choices=["auto", "flatfiles", "rest"], default="auto")
    ap.add_argument("--flatfiles-root", default="s3://flatfiles/us_stocks_sip/minute_aggs_v1")
    ap.add_argument("--flatfiles-storage-options", default="")
    ap.add_argument("--flatfiles-max-read-errors", type=int, default=0)
    ap.add_argument("--batch-size", type=int, default=50)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--limit", type=int, default=50000)
    ap.add_argument("--max-pages", type=int, default=5000)
    ap.add_argument("--timeout", type=int, default=40)
    ap.add_argument("--max-retries", type=int, default=6)
    ap.add_argument("--backoff-base", type=float, default=1.0)
    ap.add_argument("--backoff-max", type=float, default=30.0)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--resume-validate", action="store_true")
    ap.add_argument("--prune-obsolete-months", action="store_true")
    ap.add_argument("--allow-partial", action="store_true", help="Permite guardar y certificar respuestas 206 parciales.")
    ap.add_argument("--max-records-in-memory", type=int, default=2_000_000, help="Guardia de memoria por ticker; si se supera, marca error y evita OOM.")
    ap.add_argument("--max-rows", type=int, default=None)
    ap.add_argument("--progress-every", type=int, default=10)
    ap.add_argument("--progress-seconds", type=int, default=15)
    ap.add_argument("--api-key", default=os.getenv("POLYGON_API_KEY", ""))
    args = ap.parse_args()

    outdir = Path(args.outdir)
    run_dir = outdir / "_run"
    progress_path = run_dir / "download_ohlcv_minute_v1.progress.json"
    errors_path = run_dir / "download_ohlcv_minute_v1.errors.csv"
    audit_path = run_dir / "download_ohlcv_minute_v1.ticker_audit.csv"

    tickers = load_tickers(Path(args.input))
    if args.max_rows:
        tickers = tickers[: args.max_rows]
    total = len(tickers)
    if total == 0:
        raise SystemExit("No hay tickers en input")

    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "checkpoints").mkdir(parents=True, exist_ok=True)
    write_json(progress_path, {"status": "running", "source": args.source, "start": args.start, "end": args.end, "done_tickers": 0, "total_tickers": total, "progress_pct": 0.0, "rows_total": 0, "updated_at_utc": utc_now(), "outdir": str(outdir)})

    result: Dict[str, Any] | None = None
    source_errors: List[str] = []

    if args.source in {"auto", "flatfiles"}:
        try:
            result = run_flatfiles_pipeline(args, tickers, outdir, run_dir, progress_path)
            print("source_selected=flatfiles")
        except Exception as e:
            source_errors.append(f"flatfiles_error:{e}")
            if args.source == "flatfiles":
                raise
            print(f"warn flatfiles failed, fallback to rest: {e}")

    if result is None:
        if not args.api_key:
            raise SystemExit("Falta API key: usa --api-key o POLYGON_API_KEY. Necesaria para fallback REST.")
        result = run_rest_pipeline(args, tickers, outdir, run_dir, progress_path)
        print("source_selected=rest")

    audits = result.get("audits", [])
    errors = result.get("errors", [])
    rows_total = int(result.get("rows_total", 0))
    pd.DataFrame(audits).to_csv(audit_path, index=False)
    if errors:
        pd.DataFrame(errors).to_csv(errors_path, index=False)
    elif errors_path.exists():
        errors_path.unlink()

    final_status = "completed" if not errors else "completed_with_errors"
    write_json(progress_path, {"status": final_status, "source_used": result.get("source_used"), "start": args.start, "end": args.end, "done_tickers": len(audits), "total_tickers": total, "progress_pct": round(100.0 * len(audits) / max(1, total), 4), "rows_total": rows_total, "updated_at_utc": utc_now(), "errors": len(errors), "source_errors": source_errors, "errors_path": str(errors_path) if errors else None, "ticker_audit_path": str(audit_path), "outdir": str(outdir)})
    print(f"done_tickers={len(audits)}/{total}")
    print(f"rows_total={rows_total:,}")
    print(f"progress={progress_path}")
    print(f"ticker_audit={audit_path}")
    if errors:
        print(f"errors={errors_path}")


if __name__ == "__main__":
    main()
