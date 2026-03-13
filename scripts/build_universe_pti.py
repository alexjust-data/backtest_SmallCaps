#!/usr/bin/env python
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import requests

BASE_URL = "https://api.polygon.io"
EXCHANGES = ["XNAS", "XNYS", "XASE", "ARCX"]
EXCHANGE_PRIORITY = {ex: i for i, ex in enumerate(EXCHANGES)}


def month_ends(start: dt.date, end: dt.date) -> List[dt.date]:
    out: List[dt.date] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        if m == 12:
            next_first = dt.date(y + 1, 1, 1)
        else:
            next_first = dt.date(y, m + 1, 1)
        last_day = next_first - dt.timedelta(days=1)

        if last_day < start:
            pass
        elif last_day > end:
            out.append(end)
        else:
            out.append(last_day)

        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1

    return sorted(set(out))


def date_cuts(start: dt.date, end: dt.date, frequency: str) -> List[dt.date]:
    if frequency == "month_end":
        return month_ends(start, end)
    if frequency == "weekly":
        # Friday cuts to reduce calls while improving capture vs monthly.
        out: List[dt.date] = []
        cur = start
        while cur <= end:
            if cur.weekday() == 4:
                out.append(cur)
            cur += dt.timedelta(days=1)
        if out and out[-1] != end:
            out.append(end)
        elif not out:
            out = [end]
        return sorted(set(out))
    if frequency == "daily":
        out = []
        cur = start
        while cur <= end:
            out.append(cur)
            cur += dt.timedelta(days=1)
        return out
    raise ValueError(f"Unsupported frequency: {frequency}")


def get_with_retry(
    session: requests.Session,
    url: str,
    params: Optional[Dict] = None,
    retries: int = 7,
) -> requests.Response:
    backoff = 1.6
    last_err: Optional[Exception] = None
    for i in range(retries):
        try:
            r = session.get(url, params=params, timeout=60)
            if r.status_code in (429, 500, 502, 503, 504):
                retry_after = r.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except ValueError:
                        sleep_s = backoff**i
                else:
                    sleep_s = backoff**i
                time.sleep(max(0.0, sleep_s))
                continue
            r.raise_for_status()
            return r
        except Exception as exc:
            last_err = exc
            time.sleep(backoff**i)
    raise RuntimeError(f"Failed after retries: {url}") from last_err


def fetch_page(
    session: requests.Session,
    api_key: str,
    date_str: str,
    exchange: str,
    active_filter: str = "all",
) -> List[Dict]:
    results: List[Dict] = []
    url = f"{BASE_URL}/v3/reference/tickers"
    params: Dict[str, object] = {
        "market": "stocks",
        "locale": "us",
        "type": "CS",
        "exchange": exchange,
        # importante: no forzar active=true si quieres activos+inactivos históricos en cada corte
        "date": date_str,
        "limit": 1000,
        "sort": "ticker",
        "order": "asc",
        "apiKey": api_key,
    }
    if active_filter == "true":
        params["active"] = "true"
    elif active_filter == "false":
        params["active"] = "false"

    while True:
        r = get_with_retry(session, url, params=params)
        j = r.json()
        results.extend(j.get("results", []))

        next_url = j.get("next_url")
        if not next_url:
            break

        url = next_url
        params = {"apiKey": api_key}

    return results


def normalize_rows(rows: List[Dict], snap_date: dt.date) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    keep = [
        "ticker",
        "name",
        "market",
        "locale",
        "primary_exchange",
        "type",
        "active",
        "currency_name",
        "cik",
        "composite_figi",
        "share_class_figi",
        "list_date",
        "delisted_utc",
        "last_updated_utc",
    ]
    for c in keep:
        if c not in df.columns:
            df[c] = None

    df = df[keep].copy()
    df["snapshot_date"] = pd.to_datetime(str(snap_date)).date()
    df["snapshot_year"] = snap_date.year
    df["snapshot_month"] = snap_date.month

    # Entity key robusta (preferir FIGI)
    df["entity_id"] = df["composite_figi"].fillna(df["share_class_figi"])
    df["entity_id"] = df["entity_id"].fillna(
        df["ticker"].astype(str) + "|" + df["primary_exchange"].fillna("UNK").astype(str)
    )
    df["exchange_priority"] = df["primary_exchange"].map(EXCHANGE_PRIORITY).fillna(999).astype(int)
    df["has_composite_figi"] = df["composite_figi"].notna().astype(int)
    df["has_share_class_figi"] = df["share_class_figi"].notna().astype(int)
    df["has_list_date"] = df["list_date"].notna().astype(int)
    df["active"] = df["active"].fillna(False).astype(bool)
    return df


def quality_sort(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(
        by=[
            "snapshot_date",
            "entity_id",
            "has_composite_figi",
            "has_share_class_figi",
            "active",
            "has_list_date",
            "exchange_priority",
        ],
        ascending=[True, True, False, False, False, False, True],
    )


def validate_checkpoint_file(fp: Path, expected_date: str) -> tuple[bool, Optional[str]]:
    try:
        pf = pq.ParquetFile(fp)
        if pf.metadata is None or pf.metadata.num_rows <= 0:
            return False, "empty_or_no_metadata"
        cols = set(pf.schema.names)
        if "snapshot_date" not in cols or "entity_id" not in cols:
            return False, "missing_required_columns"
        t = pf.read(columns=["snapshot_date", "entity_id"])
        d = t.to_pandas()
        if d.empty:
            return False, "empty_payload"
        if d["entity_id"].isna().all():
            return False, "all_entity_id_null"
        got_dates = pd.to_datetime(d["snapshot_date"], errors="coerce").dt.date.astype(str).unique().tolist()
        if len(got_dates) != 1 or got_dates[0] != expected_date:
            return False, f"snapshot_date_mismatch:{got_dates}"
        return True, None
    except Exception as exc:
        return False, f"unreadable:{exc}"


def atomic_write_parquet(df: pd.DataFrame, dst: Path) -> None:
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), tmp)
    os.replace(tmp, dst)


def atomic_write_json(payload: Dict[str, object], dst: Path) -> None:
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(tmp, dst)


def qa_row_from_checkpoint(date_str: str, ckpt_fp: Path) -> Dict[str, object]:
    t = pq.read_table(ckpt_fp, columns=["entity_id"])
    d = t.to_pandas()
    rows = int(len(d))
    entities = int(d["entity_id"].nunique()) if rows > 0 else 0
    return {
        "snapshot_date": date_str,
        "rows_raw": rows,
        "rows_dedup": rows,
        "entities": entities,
        "dupes_by_snapshot_entity": 0,
        "null_entity_id": int(d["entity_id"].isna().sum()) if rows > 0 else 0,
        "null_composite_figi": None,
        "null_share_class_figi": None,
        "source": "checkpoint_fallback",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build point-in-time universe panel from Polygon reference tickers")
    parser.add_argument("--start", default="2005-01-01")
    parser.add_argument("--end", default=dt.date.today().isoformat())
    parser.add_argument("--outdir", default=r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti")
    parser.add_argument("--sleep-between-calls", type=float, default=0.0)
    parser.add_argument(
        "--frequency",
        choices=["month_end", "weekly", "daily"],
        default="weekly",
        help="Date cut frequency. daily minimizes missed short-lived listings.",
    )
    parser.add_argument(
        "--active-filter",
        choices=["all", "true", "false"],
        default="all",
        help="Filter by active flag at each date cut. Use 'all' for activos+inactivos.",
    )
    parser.add_argument("--append-panel", action="store_true", help="Append/merge with existing panel directory.")
    parser.add_argument(
        "--max-cuts",
        type=int,
        default=0,
        help="Process only first N cuts (smoke test / quick validation). 0 = all cuts.",
    )
    parser.add_argument(
        "--progress-file",
        default="build_universe_pti.progress.json",
        help="Progress JSON file written inside outdir while running.",
    )
    parser.add_argument(
        "--checkpoint-mode",
        choices=["on", "off"],
        default="on",
        help="If 'on', write per-snapshot parquet checkpoints during run for crash recovery.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from existing per-snapshot checkpoints (requires --checkpoint-mode on).",
    )
    args = parser.parse_args()

    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        raise ValueError("Set POLYGON_API_KEY env var")

    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    panel_parts: List[pd.DataFrame] = []
    qa_rows: List[Dict[str, object]] = []

    cuts = date_cuts(start, end, args.frequency)
    if args.max_cuts and args.max_cuts > 0:
        cuts = cuts[: args.max_cuts]
    requested_cuts = {d.isoformat() for d in cuts}
    successful_cuts: set[str] = set()
    total_cuts = len(cuts)
    if total_cuts == 0:
        raise RuntimeError("No date cuts to process")
    started_at = time.time()
    progress_path = outdir / args.progress_file
    partial_qa_path = outdir / "qa_coverage_by_cut.partial.csv"
    checkpoint_dir = outdir / "tickers_panel_pti_checkpoints"
    checkpoint_mode_on = args.checkpoint_mode == "on"

    if checkpoint_mode_on:
        if checkpoint_dir.exists() and not args.resume:
            shutil.rmtree(checkpoint_dir)
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
    if partial_qa_path.exists() and not args.resume:
        partial_qa_path.unlink()

    checkpoint_dates_present: set[str] = set()
    checkpoint_validation_issues: Dict[str, str] = {}
    if checkpoint_mode_on and checkpoint_dir.exists():
        for fp in checkpoint_dir.glob("snapshot_date=*.parquet"):
            date_key = fp.stem.replace("snapshot_date=", "")
            ok, reason = validate_checkpoint_file(fp, date_key)
            if ok:
                checkpoint_dates_present.add(date_key)
            else:
                checkpoint_validation_issues[date_key] = reason or "invalid_checkpoint"
                try:
                    fp.unlink()
                except Exception:
                    pass
                qa_sidecar = checkpoint_dir / f"snapshot_date={date_key}.qa.json"
                if qa_sidecar.exists():
                    try:
                        qa_sidecar.unlink()
                    except Exception:
                        pass

    def write_progress(
        *,
        idx: int,
        snapshot_date: str,
        status: str,
        rows_dedup: int = 0,
        entities: int = 0,
        error: Optional[str] = None,
    ) -> None:
        elapsed = time.time() - started_at
        done = max(0, idx)
        avg = elapsed / done if done > 0 else 0.0
        remaining = max(0, total_cuts - done)
        eta_sec = int(avg * remaining) if avg > 0 else None
        payload = {
            "status": status,
            "frequency": args.frequency,
            "active_filter": args.active_filter,
            "snapshot_index": done,
            "snapshot_total": total_cuts,
            "snapshot_date": snapshot_date,
            "progress_pct": round(100.0 * done / total_cuts, 2),
            "rows_dedup_last_snapshot": int(rows_dedup),
            "entities_last_snapshot": int(entities),
            "successful_cuts": len(successful_cuts),
            "elapsed_sec": int(elapsed),
            "eta_sec": eta_sec,
            "error": error,
            "updated_at_utc": dt.datetime.now(dt.UTC).isoformat(),
            "checkpoint_mode": args.checkpoint_mode,
            "checkpoint_dir": str(checkpoint_dir) if checkpoint_mode_on else None,
            "checkpoint_validation_issues": checkpoint_validation_issues,
        }
        progress_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with requests.Session() as session:
        for i, snap in enumerate(cuts, start=1):
            date_str = snap.isoformat()

            if checkpoint_mode_on and args.resume and date_str in checkpoint_dates_present:
                successful_cuts.add(date_str)
                qa_sidecar = checkpoint_dir / f"snapshot_date={date_str}.qa.json"
                if qa_sidecar.exists():
                    try:
                        qa_rows.append(json.loads(qa_sidecar.read_text(encoding="utf-8")))
                    except Exception:
                        qa_rows.append(qa_row_from_checkpoint(date_str, checkpoint_dir / f"snapshot_date={date_str}.parquet"))
                else:
                    qa_rows.append(qa_row_from_checkpoint(date_str, checkpoint_dir / f"snapshot_date={date_str}.parquet"))
                pct = 100.0 * i / total_cuts
                elapsed = time.time() - started_at
                avg = elapsed / i
                eta = int(avg * (total_cuts - i))
                print(
                    f"snapshot {date_str}: resume-skip (checkpoint exists) "
                    f"| progress={i}/{total_cuts} ({pct:.2f}%) elapsed={int(elapsed)}s eta={eta}s"
                )
                write_progress(idx=i, snapshot_date=date_str, status="running")
                continue

            snap_frames: List[pd.DataFrame] = []

            for ex in EXCHANGES:
                rows = fetch_page(
                    session,
                    api_key=api_key,
                    date_str=date_str,
                    exchange=ex,
                    active_filter=args.active_filter,
                )
                df_ex = normalize_rows(rows, snap_date=snap)
                if not df_ex.empty:
                    snap_frames.append(df_ex)
                if args.sleep_between_calls > 0:
                    time.sleep(args.sleep_between_calls)

            snap_df = (
                pd.concat([x for x in snap_frames if not x.empty], ignore_index=True)
                if snap_frames
                else pd.DataFrame()
            )
            if snap_df.empty:
                pct = 100.0 * i / total_cuts
                elapsed = int(time.time() - started_at)
                print(
                    f"snapshot {date_str}: empty | progress={i}/{total_cuts} ({pct:.2f}%) elapsed={elapsed}s"
                )
                write_progress(idx=i, snapshot_date=date_str, status="running", rows_dedup=0, entities=0)
                continue

            successful_cuts.add(date_str)
            raw_rows = int(len(snap_df))
            dupes_entity = int(snap_df.duplicated(subset=["snapshot_date", "entity_id"]).sum())
            null_entity_id = int(snap_df["entity_id"].isna().sum())
            null_composite_figi = int(snap_df["composite_figi"].isna().sum())
            null_share_class_figi = int(snap_df["share_class_figi"].isna().sum())

            # Dedup deterministica por calidad de registro dentro del corte.
            snap_df = snap_df.sort_values(
                by=[
                    "snapshot_date",
                    "entity_id",
                    "has_composite_figi",
                    "has_share_class_figi",
                    "active",
                    "has_list_date",
                    "exchange_priority",
                ],
                ascending=[True, True, False, False, False, False, True],
            )
            snap_df = snap_df.drop_duplicates(subset=["snapshot_date", "entity_id"], keep="first")
            dedup_rows = int(len(snap_df))
            entities_n = int(snap_df["entity_id"].nunique())

            qa_rows.append(
                {
                    "snapshot_date": date_str,
                    "rows_raw": raw_rows,
                    "rows_dedup": dedup_rows,
                    "entities": entities_n,
                    "dupes_by_snapshot_entity": dupes_entity,
                    "null_entity_id": null_entity_id,
                    "null_composite_figi": null_composite_figi,
                    "null_share_class_figi": null_share_class_figi,
                }
            )
            pd.DataFrame([qa_rows[-1]]).to_csv(
                partial_qa_path,
                mode="a",
                header=not partial_qa_path.exists(),
                index=False,
            )

            if checkpoint_mode_on:
                ckpt_path = checkpoint_dir / f"snapshot_date={date_str}.parquet"
                atomic_write_parquet(snap_df, ckpt_path)
                qa_sidecar = checkpoint_dir / f"snapshot_date={date_str}.qa.json"
                atomic_write_json(qa_rows[-1], qa_sidecar)

            if not checkpoint_mode_on:
                panel_parts.append(snap_df)
            pct = 100.0 * i / total_cuts
            elapsed = time.time() - started_at
            avg = elapsed / i
            eta = int(avg * (total_cuts - i))
            print(
                f"snapshot {date_str}: rows={dedup_rows:,} entities={entities_n:,} "
                f"| progress={i}/{total_cuts} ({pct:.2f}%) elapsed={int(elapsed)}s eta={eta}s"
            )
            write_progress(
                idx=i,
                snapshot_date=date_str,
                status="running",
                rows_dedup=dedup_rows,
                entities=entities_n,
            )

    panel_dir = outdir / "tickers_panel_pti"
    existing_rows = 0
    if checkpoint_mode_on:
        ckpt_files = list(checkpoint_dir.glob("snapshot_date=*.parquet"))
        if not ckpt_files:
            write_progress(
                idx=len(successful_cuts),
                snapshot_date=cuts[-1].isoformat(),
                status="failed",
                error="No data fetched",
            )
            raise RuntimeError("No data fetched")
        # Construye/actualiza panel final por particion tocada sin cargar todo el panel previo a memoria.
        touched_partitions = sorted(
            {(int(pd.to_datetime(d).year), int(pd.to_datetime(d).month)) for d in requested_cuts}
        )
        if panel_dir.exists() and args.append_panel:
            try:
                existing_rows = int(ds.dataset(str(panel_dir), format="parquet").count_rows())
            except Exception:
                existing_rows = 0
            for yy, mm in touched_partitions:
                part_dir = panel_dir / f"snapshot_year={yy}" / f"snapshot_month={mm}"
                if part_dir.exists():
                    shutil.rmtree(part_dir)
        elif panel_dir.exists():
            shutil.rmtree(panel_dir)
        panel_dir.mkdir(parents=True, exist_ok=True)
        for fp in sorted(ckpt_files):
            date_key = fp.stem.replace("snapshot_date=", "")
            if date_key not in requested_cuts:
                continue
            t = pq.read_table(fp)
            ds.write_dataset(
                t,
                base_dir=str(panel_dir),
                format="parquet",
                partitioning=["snapshot_year", "snapshot_month"],
                existing_data_behavior="overwrite_or_ignore",
                # Ensure each daily cut writes to a unique file inside year/month partition.
                basename_template=f"snapshot_date={date_key}-{{i}}.parquet",
            )
        panel = ds.dataset(str(panel_dir), format="parquet").to_table().to_pandas()
        panel["snapshot_date"] = pd.to_datetime(panel["snapshot_date"]).dt.date
    else:
        if not panel_parts:
            write_progress(
                idx=len(successful_cuts),
                snapshot_date=cuts[-1].isoformat(),
                status="failed",
                error="No data fetched",
            )
            raise RuntimeError("No data fetched")
        panel = pd.concat(panel_parts, ignore_index=True)
    panel["cut_frequency"] = args.frequency
    panel = quality_sort(panel)
    panel = panel.drop_duplicates(subset=["snapshot_date", "entity_id"], keep="first")

    # Merge robusto si se append: recarga panel existente y vuelve a deduplicar.
    if (not checkpoint_mode_on) and args.append_panel and panel_dir.exists():
        try:
            existing = ds.dataset(str(panel_dir), format="parquet").to_table().to_pandas()
            if not existing.empty:
                existing_rows = int(len(existing))
                if "cut_frequency" not in existing.columns:
                    existing["cut_frequency"] = "unknown"
                panel = pd.concat([existing, panel], ignore_index=True)
                panel = quality_sort(panel)
                panel = panel.drop_duplicates(subset=["snapshot_date", "entity_id"], keep="first")
        except Exception as exc:
            raise RuntimeError(f"append-panel failed reading existing panel: {panel_dir}") from exc

    # Escribir panel particionado (point-in-time)
    # Siempre reescribir directorio completo tras deduplicar en memoria,
    # para evitar ficheros parquet obsoletos al usar append/merge.
    if (not checkpoint_mode_on) and panel_dir.exists():
        shutil.rmtree(panel_dir)

    if not checkpoint_mode_on:
        panel_table = pa.Table.from_pandas(panel, preserve_index=False)
        ds.write_dataset(
            panel_table,
            base_dir=str(panel_dir),
            format="parquet",
            partitioning=["snapshot_year", "snapshot_month"],
            existing_data_behavior="overwrite_or_ignore",
        )

    # Entidades: first_seen / last_seen
    g = panel.groupby("entity_id", as_index=False).agg(
        first_seen_date=("snapshot_date", "min"),
        last_seen_date=("snapshot_date", "max"),
    )

    last_snap = panel["snapshot_date"].max()
    latest = (
        panel.sort_values(["entity_id", "snapshot_date"])
        .groupby("entity_id", as_index=False)
        .tail(1)
    )
    entities = latest.merge(g, on="entity_id", how="left")
    entities["latest_active"] = entities["active"].fillna(False).astype(bool)
    entities["status_confidence"] = "high"
    # Estado operativo unico para consumo downstream.
    entities["status"] = entities["last_seen_date"].apply(
        lambda d: "active" if d == last_snap else "inactive"
    )
    entities.loc[~entities["latest_active"], "status"] = "inactive"
    missing_cuts = sorted(requested_cuts - successful_cuts)
    if missing_cuts:
        entities.loc[entities["last_seen_date"] == last_snap, "status_confidence"] = "medium"
        entities.loc[entities["last_seen_date"] < last_snap, "status_confidence"] = "low"

    all_path = outdir / "tickers_all.parquet"
    active_path = outdir / "tickers_active.parquet"
    inactive_path = outdir / "tickers_inactive.parquet"

    entities.to_parquet(all_path, index=False)
    entities[entities["status"] == "active"].to_parquet(active_path, index=False)
    entities[entities["status"] == "inactive"].to_parquet(inactive_path, index=False)
    qa_path = outdir / "qa_coverage_by_cut.csv"
    qa_current = pd.DataFrame(qa_rows)
    if partial_qa_path.exists():
        try:
            qa_partial = pd.read_csv(partial_qa_path)
            qa_current = pd.concat([qa_partial, qa_current], ignore_index=True)
        except Exception:
            pass
    if not qa_current.empty:
        qa_current = qa_current.sort_values("snapshot_date").drop_duplicates(subset=["snapshot_date"], keep="last")
    existing_qa_rows = 0
    if args.append_panel and qa_path.exists():
        try:
            qa_existing = pd.read_csv(qa_path)
            existing_qa_rows = int(len(qa_existing))
            qa_all = pd.concat([qa_existing, qa_current], ignore_index=True)
            qa_all = qa_all.sort_values("snapshot_date")
            qa_all = qa_all.drop_duplicates(subset=["snapshot_date"], keep="last")
        except Exception as exc:
            raise RuntimeError(f"append-panel failed reading existing QA file: {qa_path}") from exc
    else:
        qa_all = qa_current.sort_values("snapshot_date")
    qa_all.to_csv(qa_path, index=False)

    meta = {
        "start": args.start,
        "end": args.end,
        "outdir": str(outdir),
        "panel_rows": int(len(panel)),
        "existing_panel_rows_before_append": existing_rows,
        "existing_qa_rows_before_append": existing_qa_rows,
        "entities": int(len(entities)),
        "active": int((entities["status"] == "active").sum()),
        "inactive": int((entities["status"] == "inactive").sum()),
        "last_snapshot": str(last_snap),
        "last_cut_partial": bool(end != cuts[-1] if cuts else False),
        "frequency": args.frequency,
        "requested_cuts": len(requested_cuts),
        "successful_cuts": len(successful_cuts),
        "missing_cuts": missing_cuts,
        "qa_coverage_by_cut": str(qa_path),
        "checkpoint_validation_issues": checkpoint_validation_issues,
    }
    (outdir / "build_universe_pti.meta.json").write_text(pd.Series(meta).to_json(indent=2), encoding="utf-8")
    write_progress(
        idx=total_cuts,
        snapshot_date=cuts[-1].isoformat(),
        status="completed",
        rows_dedup=int(len(panel)),
        entities=int(len(entities)),
    )

    print(f"panel rows: {len(panel):,}")
    print(f"entities : {len(entities):,}")
    print(f"active   : {(entities['status'] == 'active').sum():,}")
    print(f"inactive : {(entities['status'] == 'inactive').sum():,}")
    print(f"cuts     : requested={len(requested_cuts):,} successful={len(successful_cuts):,} missing={len(missing_cuts):,}")
    print(f"outdir   : {outdir}")
    print(f"all      : {all_path}")
    print(f"active   : {active_path}")
    print(f"inactive : {inactive_path}")
    print(f"qa       : {qa_path}")


if __name__ == "__main__":
    main()
