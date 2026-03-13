#!/usr/bin/env python3
"""
Agent01 downloader robusto para quotes Polygon.

Compatibilidad de CLI con script legacy:
  --csv --output --concurrent --api-key --resume

Mejoras clave:
- Escritura atomica (.tmp -> rename)
- Estado explicito por tarea: DOWNLOADED_OK / DOWNLOADED_EMPTY / DOWNLOAD_PARTIAL / DOWNLOAD_FAIL
- Resume por clave de tarea (ticker+date), no por indice de CSV
- Eventos history/current + live status para flujo 01-02-03
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from zoneinfo import ZoneInfo


TERMINAL_OK = {"DOWNLOADED_OK", "EMPTY_CONFIRMED"}
TERMINAL_BAD = {"DOWNLOAD_PARTIAL", "DOWNLOAD_FAIL"}
RESUME_SKIP_STATUSES = {"DOWNLOADED_OK", "EMPTY_CONFIRMED"}

EVENT_COLUMNS = [
    "task_key",
    "ticker",
    "date",
    "file",
    "status",
    "rows",
    "pages",
    "http_status",
    "attempt",
    "error",
    "empty_rechecks",
    "file_size_bytes",
    "file_sha256",
    "processed_at_utc",
    "run_id",
]

FLUSH_EVERY = 100

QUOTES_ARROW_SCHEMA = pa.schema([
    pa.field("ask_exchange", pa.int64()),
    pa.field("ask_price", pa.float64()),
    pa.field("ask_size", pa.int64()),
    pa.field("bid_exchange", pa.int64()),
    pa.field("bid_price", pa.float64()),
    pa.field("bid_size", pa.int64()),
    pa.field("conditions", pa.large_list(pa.int64())),
    pa.field("indicators", pa.large_list(pa.int64())),
    pa.field("participant_timestamp", pa.int64()),
    pa.field("sequence_number", pa.int64()),
    pa.field("timestamp", pa.int64()),
    pa.field("tape", pa.int64()),
    pa.field("trf_timestamp", pa.int64()),
    pa.field("year", pa.int32()),
    pa.field("month", pa.int32()),
    pa.field("day", pa.int32()),
])


@dataclass
class DownloadConfig:
    csv_path: Path
    output_root: Path
    run_dir: Path
    run_id: str
    api_key: str
    concurrent: int
    resume: bool
    max_pages: int
    request_timeout_sec: int
    max_retries_per_page: int
    retry_backoff_sec: float
    day_partition: str  # DD | YYYY-MM-DD
    market_tz: str
    market_tz_offset_fallback: str
    session_start_local: str
    session_end_local: str
    hash_files: bool
    max_empty_rechecks: int
    task_batch_size: int


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def task_key(ticker: str, date: str) -> str:
    return f"{ticker}|{date}"


def expected_file(output_root: Path, ticker: str, date: str, day_partition: str) -> Path:
    y, m, d = date.split("-")
    day_value = d if day_partition == "DD" else date
    return output_root / ticker / f"year={y}" / f"month={m}" / f"day={day_value}" / "quotes.parquet"


def atomic_write_parquet(df: pl.DataFrame, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    if tmp.exists():
        tmp.unlink()
    df.write_parquet(tmp, compression="zstd", compression_level=1, statistics=False)
    os.replace(tmp, target)


def compute_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv_tasks(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    if "ticker" not in df.columns or "date" not in df.columns:
        raise ValueError("CSV debe contener columnas: ticker,date")
    out = df[["ticker", "date"]].copy()
    out["ticker"] = out["ticker"].astype(str).str.strip()
    out["date"] = out["date"].astype(str).str.strip()
    out = out.drop_duplicates(subset=["ticker", "date"]).sort_values(["ticker", "date"]).reset_index(drop=True)
    out["task_key"] = out.apply(lambda r: task_key(r["ticker"], r["date"]), axis=1)
    return out


def load_current(events_current_csv: Path) -> pd.DataFrame:
    if not events_current_csv.exists():
        return pd.DataFrame(columns=EVENT_COLUMNS)
    try:
        df = pd.read_csv(events_current_csv)
    except Exception:
        return pd.DataFrame(columns=EVENT_COLUMNS)
    for c in EVENT_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    return df[EVENT_COLUMNS].copy()


def save_history_append(events_history_csv: Path, new_events: pd.DataFrame) -> None:
    events_history_csv.parent.mkdir(parents=True, exist_ok=True)
    write_header = not events_history_csv.exists()
    new_events.to_csv(events_history_csv, mode="a", header=write_header, index=False, encoding="utf-8")


def save_current_merge(events_current_csv: Path, old_current: pd.DataFrame, new_events: pd.DataFrame) -> pd.DataFrame:
    cur = pd.concat([old_current, new_events], ignore_index=True)
    cur = cur.sort_values("processed_at_utc").drop_duplicates(subset=["task_key"], keep="last")
    cur.to_csv(events_current_csv, index=False, encoding="utf-8")
    return cur


def save_live_status(live_status_json: Path, cfg: DownloadConfig, tasks_total: int, cur: pd.DataFrame) -> None:
    status_counts = cur["status"].value_counts(dropna=False).to_dict() if len(cur) else {}
    done_ok = int(cur["status"].isin(list(TERMINAL_OK)).sum()) if len(cur) else 0
    done_bad = int(cur["status"].isin(list(TERMINAL_BAD)).sum()) if len(cur) else 0
    pending = max(0, tasks_total - done_ok)

    payload = {
        "updated_utc": utc_now(),
        "run_id": cfg.run_id,
        "output_root": str(cfg.output_root),
        "tasks_total": int(tasks_total),
        "done_ok": done_ok,
        "done_bad": done_bad,
        "pending": int(pending),
        "status_counts_current": status_counts,
        "concurrent": cfg.concurrent,
        "max_pages": cfg.max_pages,
        "session_start_local": cfg.session_start_local,
        "session_end_local": cfg.session_end_local,
    }
    live_status_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def validate_written_file(path: Path) -> Tuple[bool, int, str]:
    if not path.exists():
        return False, 0, "missing_after_write"
    try:
        t = pl.read_parquet(path)
        return True, int(t.height), "ok"
    except Exception as e:
        return False, 0, f"unreadable_after_write:{e}"


def build_quotes_frame(results: List[Dict[str, Any]]) -> pl.DataFrame:
    df = pl.DataFrame(results)
    if "sip_timestamp" in df.columns and "timestamp" not in df.columns:
        df = df.rename({"sip_timestamp": "timestamp"})
    if "timestamp" in df.columns:
        df = df.sort("timestamp")
    return df


def _to_int(value: Any) -> Optional[int]:
    if value is None or value is pd.NA:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _to_float(value: Any) -> Optional[float]:
    if value is None or value is pd.NA:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _to_int_list(value: Any) -> List[int]:
    if value is None or value is pd.NA:
        return []
    if isinstance(value, list):
        out: List[int] = []
        for x in value:
            xi = _to_int(x)
            if xi is not None:
                out.append(xi)
        return out
    return []


def normalize_quote_rows(results: List[Dict[str, Any]], date_str: str) -> List[Dict[str, Any]]:
    y, m, d = date_str.split("-")
    y_i, m_i, d_i = int(y), int(m), int(d)
    rows: List[Dict[str, Any]] = []
    for q in results:
        rows.append({
            "ask_exchange": _to_int(q.get("ask_exchange")),
            "ask_price": _to_float(q.get("ask_price")),
            "ask_size": _to_int(q.get("ask_size")),
            "bid_exchange": _to_int(q.get("bid_exchange")),
            "bid_price": _to_float(q.get("bid_price")),
            "bid_size": _to_int(q.get("bid_size")),
            "conditions": _to_int_list(q.get("conditions")),
            "indicators": _to_int_list(q.get("indicators")),
            "participant_timestamp": _to_int(q.get("participant_timestamp")),
            "sequence_number": _to_int(q.get("sequence_number")),
            "timestamp": _to_int(q.get("sip_timestamp", q.get("timestamp"))),
            "tape": _to_int(q.get("tape")),
            "trf_timestamp": _to_int(q.get("trf_timestamp")),
            "year": y_i,
            "month": m_i,
            "day": d_i,
        })
    return rows


def open_incremental_writer(target: Path) -> Tuple[Path, pq.ParquetWriter]:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    if tmp.exists():
        tmp.unlink()
    writer = pq.ParquetWriter(
        where=str(tmp),
        schema=QUOTES_ARROW_SCHEMA,
        compression="zstd",
        compression_level=1,
        use_dictionary=False,
        write_statistics=False,
    )
    return tmp, writer


def append_quote_page(writer: pq.ParquetWriter, results: List[Dict[str, Any]], date_str: str) -> int:
    rows = normalize_quote_rows(results, date_str)
    if not rows:
        return 0
    table = pa.Table.from_pylist(rows, schema=QUOTES_ARROW_SCHEMA)
    writer.write_table(table)
    return len(rows)


def inspect_existing_good_file(path: Path) -> Tuple[bool, int]:
    ok, rows, _ = validate_written_file(path)
    return bool(ok and rows > 0), int(rows)


class QuotesDownloader:
    def __init__(self, cfg: DownloadConfig):
        self.cfg = cfg
        self.base_url = "https://api.polygon.io/v3/quotes"
        self.sem = asyncio.Semaphore(cfg.concurrent)
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=self.cfg.request_timeout_sec)
        connector = aiohttp.TCPConnector(limit=max(200, self.cfg.concurrent * 4), limit_per_host=max(100, self.cfg.concurrent * 2))
        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def _get_json_with_retries(self, url: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[int], Optional[str]]:
        assert self.session is not None
        last_error = None
        last_status = None

        for attempt in range(1, self.cfg.max_retries_per_page + 1):
            try:
                async with self.session.get(url, params=params) as resp:
                    last_status = int(resp.status)
                    if resp.status == 429:
                        retry_after = resp.headers.get("Retry-After")
                        sleep_sec = float(retry_after) if retry_after else self.cfg.retry_backoff_sec * attempt
                        await asyncio.sleep(sleep_sec)
                        last_error = "rate_limited"
                        continue
                    if 500 <= resp.status < 600:
                        await asyncio.sleep(self.cfg.retry_backoff_sec * attempt)
                        last_error = f"server_{resp.status}"
                        continue
                    if resp.status != 200:
                        txt = await resp.text()
                        return None, last_status, f"http_{resp.status}:{txt[:300]}"

                    data = await resp.json()
                    return data, last_status, None
            except Exception as e:
                last_error = str(e)
                await asyncio.sleep(self.cfg.retry_backoff_sec * attempt)

        return None, last_status, f"retries_exhausted:{last_error}"

    def _market_session_bounds(self, date_str: str) -> Tuple[str, str]:
        # DST-aware by default. Fallback to fixed offset if timezone fails.
        try:
            d = datetime.fromisoformat(date_str).date()
            tz = ZoneInfo(self.cfg.market_tz)
            start_h, start_m, start_s = (int(x) for x in self.cfg.session_start_local.split(":"))
            end_h, end_m, end_s = (int(x) for x in self.cfg.session_end_local.split(":"))
            start_local = datetime(d.year, d.month, d.day, start_h, start_m, start_s, tzinfo=tz)
            end_local = datetime(d.year, d.month, d.day, end_h, end_m, end_s, tzinfo=tz)
            return start_local.isoformat(), end_local.isoformat()
        except Exception:
            return (
                f"{date_str}T{self.cfg.session_start_local}{self.cfg.market_tz_offset_fallback}",
                f"{date_str}T{self.cfg.session_end_local}{self.cfg.market_tz_offset_fallback}",
            )

    async def download_task(self, ticker: str, date: str, prev_empty_rechecks: int = 0) -> Dict[str, Any]:
        fpath = expected_file(self.cfg.output_root, ticker, date, self.cfg.day_partition)
        ts_gte, ts_lt = self._market_session_bounds(date)
        event: Dict[str, Any] = {
            "task_key": task_key(ticker, date),
            "ticker": ticker,
            "date": date,
            "file": str(fpath),
            "status": "DOWNLOAD_FAIL",
            "rows": 0,
            "pages": 0,
            "http_status": pd.NA,
            "attempt": 1,
            "error": pd.NA,
            "empty_rechecks": int(prev_empty_rechecks),
            "file_size_bytes": pd.NA,
            "file_sha256": pd.NA,
            "processed_at_utc": utc_now(),
            "run_id": self.cfg.run_id,
        }

        if self.cfg.resume and fpath.exists():
            good_existing, existing_rows = inspect_existing_good_file(fpath)
            if good_existing:
                event["status"] = "DOWNLOADED_OK"
                event["rows"] = existing_rows
                event["pages"] = pd.NA
                event["error"] = "resume_existing_file"
                event["processed_at_utc"] = utc_now()
                try:
                    st = fpath.stat()
                    event["file_size_bytes"] = int(st.st_size)
                except Exception:
                    event["file_size_bytes"] = pd.NA
                return event

        url = f"{self.base_url}/{ticker}"
        params: Dict[str, Any] = {
            "timestamp.gte": ts_gte,
            "timestamp.lt": ts_lt,
            "limit": 50000,
            "sort": "timestamp",
            "order": "asc",
            "apiKey": self.cfg.api_key,
        }

        pages = 0
        next_url: Optional[str] = None
        last_status = None
        tmp_path: Optional[Path] = None
        writer: Optional[pq.ParquetWriter] = None
        rows_written_stream = 0
        saw_any_results = False

        async with self.sem:
            data, status, err = await self._get_json_with_retries(url, params=params)
            last_status = status
            if data is None:
                event["status"] = "DOWNLOAD_FAIL"
                event["http_status"] = last_status if last_status is not None else pd.NA
                event["error"] = err
                event["processed_at_utc"] = utc_now()
                return event

            results = data.get("results") or []
            if results:
                saw_any_results = True
                tmp_path, writer = open_incremental_writer(fpath)
                rows_written_stream += append_quote_page(writer, results, date)
            pages = 1
            next_url = data.get("next_url")

            partial_due_to_cap = False

            while next_url:
                if self.cfg.max_pages > 0 and pages >= self.cfg.max_pages:
                    partial_due_to_cap = True
                    break

                if "apiKey=" not in next_url:
                    next_url = next_url + ("&" if "?" in next_url else "?") + f"apiKey={self.cfg.api_key}"

                page_data, page_status, page_err = await self._get_json_with_retries(next_url, params=None)
                last_status = page_status
                if page_data is None:
                    event["status"] = "DOWNLOAD_PARTIAL" if saw_any_results else "DOWNLOAD_FAIL"
                    event["error"] = page_err
                    event["http_status"] = last_status if last_status is not None else pd.NA
                    break

                page_results = page_data.get("results") or []
                if page_results and writer is not None:
                    saw_any_results = True
                    rows_written_stream += append_quote_page(writer, page_results, date)
                pages += 1
                next_url = page_data.get("next_url")

            if partial_due_to_cap:
                event["status"] = "DOWNLOAD_PARTIAL"
                event["error"] = f"max_pages_reached:{self.cfg.max_pages}"
                event["http_status"] = last_status if last_status is not None else pd.NA

        # Persist only terminal-good payloads to the watched quotes root.
        # Agent02 validates files directly from disk, so publishing partial
        # payloads here would create false PASS/coverage while the day is incomplete.
        try:
            event["pages"] = pages
            event["http_status"] = last_status if last_status is not None else pd.NA

            if event["status"] == "DOWNLOAD_PARTIAL":
                event["rows"] = int(rows_written_stream)
            elif not saw_any_results:
                next_empty = int(prev_empty_rechecks) + 1
                event["rows"] = 0
                event["empty_rechecks"] = next_empty
                if next_empty >= self.cfg.max_empty_rechecks:
                    event["status"] = "EMPTY_CONFIRMED"
                else:
                    event["status"] = "DOWNLOADED_EMPTY"
                event["error"] = "polygon_empty_results"
            else:
                if writer is None:
                    raise RuntimeError("writer_missing_for_non_empty_payload")
                writer.close()
                writer = None
                os.replace(tmp_path, fpath)

                ok, rows_written, vmsg = validate_written_file(fpath)
                event["rows"] = rows_written

                if not ok:
                    event["status"] = "DOWNLOAD_FAIL"
                    event["error"] = vmsg
                else:
                    event["status"] = "DOWNLOADED_OK"
                    event["error"] = pd.NA
                    event["empty_rechecks"] = 0

                    try:
                        st = fpath.stat()
                        event["file_size_bytes"] = int(st.st_size)
                    except Exception:
                        event["file_size_bytes"] = pd.NA

                    if self.cfg.hash_files:
                        try:
                            event["file_sha256"] = compute_sha256(fpath)
                        except Exception as e:
                            event["file_sha256"] = pd.NA
                            if pd.isna(event["error"]):
                                event["error"] = f"hash_error:{e}"

        except Exception as e:
            event["status"] = "DOWNLOAD_FAIL"
            event["error"] = f"write_error:{e}"
            event["rows"] = 0
            event["pages"] = pages
        finally:
            try:
                if writer is not None:
                    writer.close()
            except Exception:
                pass
            try:
                if event["status"] != "DOWNLOADED_OK" and tmp_path is not None and tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass

        event["processed_at_utc"] = utc_now()
        return event


async def run(cfg: DownloadConfig) -> int:
    events_history_csv = cfg.run_dir / "download_events_history.csv"
    events_current_csv = cfg.run_dir / "download_events_current.csv"
    state_json = cfg.run_dir / "download_state.json"
    live_status_json = cfg.run_dir / "download_live_status.json"

    cfg.run_dir.mkdir(parents=True, exist_ok=True)
    cfg.output_root.mkdir(parents=True, exist_ok=True)

    tasks = read_csv_tasks(cfg.csv_path)
    current = load_current(events_current_csv)

    already_ok = set()
    empty_rechecks_map: Dict[str, int] = {}
    if cfg.resume and len(current):
        already_ok = set(current[current["status"].isin(list(RESUME_SKIP_STATUSES))]["task_key"].astype(str).tolist())
        if "task_key" in current.columns:
            tmp = current.copy()
            if "empty_rechecks" not in tmp.columns:
                tmp["empty_rechecks"] = 0
            tmp["empty_rechecks"] = pd.to_numeric(tmp["empty_rechecks"], errors="coerce").fillna(0).astype(int)
            empty_rechecks_map = dict(zip(tmp["task_key"].astype(str), tmp["empty_rechecks"].astype(int)))

    todo_df = tasks[~tasks["task_key"].isin(already_ok)].copy()

    print(f"tasks_total={len(tasks)}")
    print(f"tasks_already_ok={len(already_ok)}")
    print(f"tasks_to_process={len(todo_df)}")

    if len(todo_df) == 0:
        save_live_status(live_status_json, cfg, len(tasks), current)
        return 0

    merged_current = current.copy()
    pending_batch: List[Dict[str, Any]] = []

    def flush_batch() -> None:
        nonlocal merged_current, pending_batch
        if not pending_batch:
            return
        new_events = pd.DataFrame(pending_batch)
        for c in EVENT_COLUMNS:
            if c not in new_events.columns:
                new_events[c] = pd.NA
        new_events = new_events[EVENT_COLUMNS]
        save_history_append(events_history_csv, new_events)
        merged_current = save_current_merge(events_current_csv, merged_current, new_events)

        retry_current = merged_current[merged_current["status"].isin(["DOWNLOAD_FAIL", "DOWNLOAD_PARTIAL"])].copy()
        retry_csv = cfg.run_dir / "download_retry_queue_current.csv"
        retry_csv.write_text("", encoding="utf-8") if retry_current.empty else retry_current.to_csv(retry_csv, index=False, encoding="utf-8")

        sev = merged_current["status"].value_counts(dropna=False).to_dict()
        state = {
            "run_id": cfg.run_id,
            "updated_utc": utc_now(),
            "tasks_total": int(len(tasks)),
            "tasks_current_rows": int(len(merged_current)),
            "status_counts_current": sev,
            "retry_pending": int(len(retry_current)),
            "output_root": str(cfg.output_root),
            "csv_path": str(cfg.csv_path),
        }
        state_json.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
        save_live_status(live_status_json, cfg, len(tasks), merged_current)
        pending_batch = []

    async with QuotesDownloader(cfg) as dl:
        for start in range(0, len(todo_df), cfg.task_batch_size):
            chunk = todo_df.iloc[start:start + cfg.task_batch_size]
            coros = [
                dl.download_task(
                    r.ticker,
                    r.date,
                    prev_empty_rechecks=int(empty_rechecks_map.get(r.task_key, 0)),
                )
                for r in chunk.itertuples(index=False)
            ]
            for fut in asyncio.as_completed(coros):
                ev = await fut
                pending_batch.append(ev)
                done_count = len(merged_current) - len(current) + len(pending_batch)
                if done_count % FLUSH_EVERY == 0:
                    flush_batch()
                    print(f"processed={done_count}/{len(todo_df)}")

            flush_batch()
            done_count = len(merged_current) - len(current)
            print(f"batch_done={done_count}/{len(todo_df)}")

    flush_batch()

    retry_current = merged_current[merged_current["status"].isin(["DOWNLOAD_FAIL", "DOWNLOAD_PARTIAL"])].copy()
    final_state = {
        "run_id": cfg.run_id,
        "updated_utc": utc_now(),
        "tasks_total": int(len(tasks)),
        "tasks_current_rows": int(len(merged_current)),
        "status_counts_current": merged_current["status"].value_counts(dropna=False).to_dict(),
        "retry_pending": int(len(retry_current)),
        "output_root": str(cfg.output_root),
        "csv_path": str(cfg.csv_path),
    }

    print(json.dumps(final_state, ensure_ascii=False))
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Robust download quotes for Agent01")
    p.add_argument("--csv", required=True, help="CSV con columnas ticker,date")
    p.add_argument("--output", required=True, help="Carpeta root de quotes")
    p.add_argument("--concurrent", type=int, default=80, help="Conexiones simultaneas")
    p.add_argument("--api-key", default=None, help="Polygon API key; fallback POLYGON_API_KEY")
    p.add_argument("--resume", action="store_true", help="Skip tareas ya en estado terminal OK")

    p.add_argument("--run-id", default=None, help="Run id; default timestamp")
    p.add_argument("--run-dir", default=None, help="Directorio de corrida; default carpeta del CSV")
    p.add_argument("--max-pages", type=int, default=0, help="0=sin limite; >0 marca PARTIAL al alcanzar")
    p.add_argument("--request-timeout-sec", type=int, default=30)
    p.add_argument("--max-retries-per-page", type=int, default=5)
    p.add_argument("--retry-backoff-sec", type=float, default=1.5)
    p.add_argument("--day-partition", choices=["DD", "YYYY-MM-DD"], default="DD")
    p.add_argument("--market-tz", default="America/New_York", help="Timezone de mercado (DST-aware)")
    p.add_argument("--market-tz-offset", default="-05:00", help="Fallback offset fijo si timezone falla")
    p.add_argument("--session-start", default="04:00:00", help="Inicio sesion local ET")
    p.add_argument("--session-end", default="20:00:00", help="Fin sesion local ET")
    p.add_argument("--hash-files", action="store_true", help="Calcular sha256 por archivo descargado")
    p.add_argument("--max-empty-rechecks", type=int, default=2, help="Rondas maximas de recheck antes de EMPTY_CONFIRMED")
    p.add_argument("--task-batch-size", type=int, default=5000, help="Numero de tareas cargadas simultaneamente en memoria")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    api_key = args.api_key or os.getenv("POLYGON_API_KEY")
    if not api_key:
        print("ERROR: falta POLYGON_API_KEY", file=sys.stderr)
        return 2

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"ERROR: no existe CSV {csv_path}", file=sys.stderr)
        return 2

    run_id = args.run_id or datetime.now().strftime("%Y%m%d_%H%M%S_quotes")
    run_dir = Path(args.run_dir) if args.run_dir else csv_path.resolve().parent

    cfg = DownloadConfig(
        csv_path=csv_path,
        output_root=Path(args.output),
        run_dir=run_dir,
        run_id=run_id,
        api_key=api_key,
        concurrent=max(1, int(args.concurrent)),
        resume=bool(args.resume),
        max_pages=max(0, int(args.max_pages)),
        request_timeout_sec=max(5, int(args.request_timeout_sec)),
        max_retries_per_page=max(1, int(args.max_retries_per_page)),
        retry_backoff_sec=max(0.1, float(args.retry_backoff_sec)),
        day_partition=args.day_partition,
        market_tz=str(args.market_tz),
        market_tz_offset_fallback=str(args.market_tz_offset),
        session_start_local=str(args.session_start),
        session_end_local=str(args.session_end),
        hash_files=bool(args.hash_files),
        max_empty_rechecks=max(1, int(args.max_empty_rechecks)),
        task_batch_size=max(1, int(args.task_batch_size)),
    )

    print(f"run_id={cfg.run_id}")
    print(f"run_dir={cfg.run_dir}")
    print(f"output_root={cfg.output_root}")
    print(f"csv={cfg.csv_path}")

    try:
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        return asyncio.run(run(cfg))
    except KeyboardInterrupt:
        print("Interrupted by user", file=sys.stderr)
        return 130
    except Exception:
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
