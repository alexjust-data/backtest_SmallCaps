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
import requests


BASE_URL = "https://api.polygon.io"
EXCHANGES = ["XNAS", "XNYS", "XASE", "ARCX"]


def get_with_retry(session: requests.Session, url: str, params: Dict[str, object], retries: int = 7) -> requests.Response:
    backoff = 1.8
    last_err: Optional[Exception] = None
    for i in range(retries):
        try:
            r = session.get(url, params=params, timeout=45)
            if r.status_code in (429, 500, 502, 503, 504):
                retry_after = r.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after and retry_after.isdigit() else backoff**i
                time.sleep(max(0.0, sleep_s))
                continue
            return r
        except Exception as exc:
            last_err = exc
            time.sleep(backoff**i)
    raise RuntimeError(f"request_failed: {url}") from last_err


def fetch_ticker_overview_asof(
    session: requests.Session,
    api_key: str,
    ticker: str,
    date_str: str,
) -> Dict[str, object]:
    url = f"{BASE_URL}/v3/reference/tickers/{ticker}"
    params: Dict[str, object] = {"date": date_str, "apiKey": api_key}
    r = get_with_retry(session, url, params=params)
    out: Dict[str, object] = {
        "http_status": int(r.status_code),
        "request_ticker": ticker,
        "request_date": date_str,
        "asof_found": False,
        "asof_error": None,
    }
    if r.status_code != 200:
        out["asof_error"] = f"http_{r.status_code}"
        return out
    j = r.json()
    res = j.get("results") if isinstance(j, dict) else None
    if not isinstance(res, dict):
        return out
    out["asof_found"] = True
    # Campos útiles para universo híbrido enriquecido
    out["asof_active"] = res.get("active")
    out["asof_name"] = res.get("name")
    out["asof_market_cap"] = res.get("market_cap")
    out["asof_weighted_shares_outstanding"] = res.get("weighted_shares_outstanding")
    out["asof_share_class_shares_outstanding"] = res.get("share_class_shares_outstanding")
    out["asof_total_employees"] = res.get("total_employees")
    out["asof_description"] = res.get("description")
    out["asof_homepage_url"] = res.get("homepage_url")
    out["asof_phone_number"] = res.get("phone_number")
    out["asof_sic_code"] = res.get("sic_code")
    out["asof_sic_description"] = res.get("sic_description")
    out["asof_ticker_root"] = res.get("ticker_root")
    out["asof_ticker_suffix"] = res.get("ticker_suffix")
    out["asof_round_lot"] = res.get("round_lot")
    out["asof_list_date"] = res.get("list_date")
    out["asof_delisted_utc"] = res.get("delisted_utc")
    out["asof_cik"] = res.get("cik")
    out["asof_composite_figi"] = res.get("composite_figi")
    out["asof_share_class_figi"] = res.get("share_class_figi")
    out["asof_last_updated_utc"] = res.get("last_updated_utc")
    out["asof_primary_exchange"] = res.get("primary_exchange")
    out["asof_type"] = res.get("type")
    addr = res.get("address") if isinstance(res.get("address"), dict) else {}
    out["asof_address_address1"] = addr.get("address1")
    out["asof_address_city"] = addr.get("city")
    out["asof_address_state"] = addr.get("state")
    out["asof_address_postal_code"] = addr.get("postal_code")
    branding = res.get("branding") if isinstance(res.get("branding"), dict) else {}
    out["asof_branding_logo_url"] = branding.get("logo_url")
    out["asof_branding_icon_url"] = branding.get("icon_url")
    return out


def fetch_ticker_events(
    session: requests.Session,
    api_key: str,
    ticker_or_id: str,
    limit: int = 1000,
) -> Dict[str, object]:
    # Experimental endpoint documented by Polygon:
    # /vX/reference/tickers/{id}/events
    url = f"{BASE_URL}/vX/reference/tickers/{ticker_or_id}/events"
    params: Dict[str, object] = {"limit": limit, "apiKey": api_key}
    r = get_with_retry(session, url, params=params)
    out: Dict[str, object] = {
        "events_http_status": int(r.status_code),
        "events_found": False,
        "events_count": 0,
        "events_error": None,
        "event_type": None,
        "event_execution_date": None,
        "event_ticker": None,
        "event_name": None,
        "event_composite_figi": None,
        "event_share_class_figi": None,
        "event_cik": None,
        "event_updated_utc": None,
        "event_created_utc": None,
        "raw_event_json": None,
    }
    if r.status_code != 200:
        out["events_error"] = f"http_{r.status_code}"
        return out
    j = r.json()
    results = j.get("results", []) if isinstance(j, dict) else []
    if not isinstance(results, list) or len(results) == 0:
        return out
    out["events_found"] = True
    out["events_count"] = len(results)
    # Keep raw payload to preserve heterogeneous event schema
    out["raw_event_json"] = json.dumps(results, ensure_ascii=False)

    # Pick latest event (by execution_date/date if available, else last item)
    def event_date(e: Dict[str, object]) -> str:
        if not isinstance(e, dict):
            return ""
        return str(e.get("execution_date") or e.get("date") or "")

    latest = sorted([e for e in results if isinstance(e, dict)], key=event_date)[-1]
    out["event_type"] = latest.get("event_type")
    out["event_execution_date"] = latest.get("execution_date") or latest.get("date")
    out["event_ticker"] = latest.get("ticker")
    out["event_name"] = latest.get("name")
    out["event_composite_figi"] = latest.get("composite_figi")
    out["event_share_class_figi"] = latest.get("share_class_figi")
    out["event_cik"] = latest.get("cik")
    out["event_updated_utc"] = latest.get("updated_utc")
    out["event_created_utc"] = latest.get("created_utc")
    return out


def fetch_inactive_catalog(api_key: str) -> pd.DataFrame:
    """
    Pull inactive catalog directly from /v3/reference/tickers with active=false.
    This is the strongest source for delisted_utc in many universes.
    """
    rows: List[Dict[str, object]] = []
    with requests.Session() as session:
        for ex in EXCHANGES:
            url = f"{BASE_URL}/v3/reference/tickers"
            params: Dict[str, object] = {
                "market": "stocks",
                "locale": "us",
                "type": "CS",
                "exchange": ex,
                "active": "false",
                "limit": 1000,
                "sort": "ticker",
                "order": "asc",
                "apiKey": api_key,
            }
            while True:
                r = get_with_retry(session, url, params=params)
                if r.status_code != 200:
                    break
                j = r.json()
                results = j.get("results", []) if isinstance(j, dict) else []
                for x in results:
                    if not isinstance(x, dict):
                        continue
                    rows.append(
                        {
                            "ticker": x.get("ticker"),
                            "primary_exchange": x.get("primary_exchange"),
                            "inactive_cat_delisted_utc": x.get("delisted_utc"),
                            "inactive_cat_cik": x.get("cik"),
                            "inactive_cat_composite_figi": x.get("composite_figi"),
                            "inactive_cat_share_class_figi": x.get("share_class_figi"),
                            "inactive_cat_last_updated_utc": x.get("last_updated_utc"),
                            "inactive_cat_active": x.get("active"),
                        }
                    )
                next_url = j.get("next_url") if isinstance(j, dict) else None
                if not next_url:
                    break
                url = next_url
                params = {"apiKey": api_key}

    if not rows:
        return pd.DataFrame(
            columns=[
                "ticker",
                "primary_exchange",
                "inactive_cat_delisted_utc",
                "inactive_cat_cik",
                "inactive_cat_composite_figi",
                "inactive_cat_share_class_figi",
                "inactive_cat_last_updated_utc",
                "inactive_cat_active",
            ]
        )

    cat = pd.DataFrame(rows)
    # Dedup by ticker+exchange preferring rows with delisted_utc and latest update.
    cat["has_delisted"] = cat["inactive_cat_delisted_utc"].notna().astype(int)
    cat["upd"] = pd.to_datetime(cat["inactive_cat_last_updated_utc"], errors="coerce", utc=True)
    cat = cat.sort_values(["ticker", "primary_exchange", "has_delisted", "upd"], ascending=[True, True, False, False])
    cat = cat.drop_duplicates(subset=["ticker", "primary_exchange"], keep="first")
    cat = cat.drop(columns=["has_delisted", "upd"], errors="ignore")
    return cat


def choose_with_source(asof_val: object, fallback_val: object) -> tuple[object, str]:
    asof_ok = pd.notna(asof_val)
    fb_ok = pd.notna(fallback_val)
    if asof_ok:
        return asof_val, "asof"
    if fb_ok:
        return fallback_val, "fallback"
    return None, "missing"


def choose_with_source_3(asof_val: object, events_val: object, fallback_val: object) -> tuple[object, str]:
    asof_ok = pd.notna(asof_val)
    ev_ok = pd.notna(events_val)
    fb_ok = pd.notna(fallback_val)
    if asof_ok:
        return asof_val, "asof"
    if ev_ok:
        return events_val, "events"
    if fb_ok:
        return fallback_val, "fallback"
    return None, "missing"


def choose_with_source_4(
    asof_val: object,
    inactive_cat_val: object,
    events_val: object,
    fallback_val: object,
) -> tuple[object, str]:
    asof_ok = pd.notna(asof_val)
    cat_ok = pd.notna(inactive_cat_val)
    ev_ok = pd.notna(events_val)
    fb_ok = pd.notna(fallback_val)
    if asof_ok:
        return asof_val, "asof"
    if cat_ok:
        return inactive_cat_val, "inactive_catalog"
    if ev_ok:
        return events_val, "events"
    if fb_ok:
        return fallback_val, "fallback"
    return None, "missing"


def main() -> None:
    ap = argparse.ArgumentParser(description="Build Universo Híbrido Enriquecido from tickers_2005_2026 + Polygon as-of overview.")
    ap.add_argument(
        "--input",
        default=r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026.parquet",
    )
    ap.add_argument(
        "--outdir",
        default=r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\hybrid_enriched",
    )
    ap.add_argument("--batch-size", type=int, default=200)
    ap.add_argument("--sleep-between-calls", type=float, default=0.0)
    ap.add_argument("--max-rows", type=int, default=0, help="0=all")
    ap.add_argument("--events-limit", type=int, default=1000)
    ap.add_argument("--disable-events", action="store_true")
    ap.add_argument("--disable-inactive-catalog", action="store_true")
    ap.add_argument("--inactive-catalog-refresh", action="store_true")
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        raise ValueError("Set POLYGON_API_KEY env var")

    inp = Path(args.input)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    ckpt_dir = outdir / "checkpoints"
    progress_path = outdir / "build_universe_hybrid_enriched.progress.json"
    enriched_path = outdir / "universe_hybrid_enriched.parquet"
    qa_path = outdir / "universe_hybrid_enriched.qa.csv"
    inactive_catalog_path = outdir / "inactive_catalog.parquet"

    # Avoid mixing old sample checkpoints with new runs when not resuming.
    if not args.resume:
        if ckpt_dir.exists():
            shutil.rmtree(ckpt_dir)
        if enriched_path.exists():
            enriched_path.unlink()
        if qa_path.exists():
            qa_path.unlink()
    ckpt_dir.mkdir(parents=True, exist_ok=True)

    # Build/load inactive catalog from v3/reference/tickers?active=false
    if args.disable_inactive_catalog:
        inactive_catalog = pd.DataFrame(columns=["ticker", "primary_exchange", "inactive_cat_delisted_utc"])
    else:
        if args.inactive_catalog_refresh or (not inactive_catalog_path.exists()):
            inactive_catalog = fetch_inactive_catalog(api_key=api_key)
            inactive_catalog.to_parquet(inactive_catalog_path, index=False)
        else:
            inactive_catalog = pd.read_parquet(inactive_catalog_path)

    base = pd.read_parquet(inp).copy()
    base["snapshot_date"] = pd.to_datetime(base["snapshot_date"], errors="coerce").dt.date
    base = base.sort_values(["snapshot_date", "ticker", "entity_id"], kind="stable").reset_index(drop=True)
    if args.max_rows and args.max_rows > 0:
        base = base.head(args.max_rows).copy()
    base["row_id"] = base.index.astype(int)

    total = len(base)
    if total == 0:
        raise RuntimeError("No rows in input.")

    n_batches = (total + args.batch_size - 1) // args.batch_size
    started = time.time()

    with requests.Session() as session:
        for b in range(n_batches):
            s = b * args.batch_size
            e = min((b + 1) * args.batch_size, total)
            ckpt_file = ckpt_dir / f"batch_{s:07d}_{e-1:07d}.parquet"
            if args.resume and ckpt_file.exists():
                continue

            chunk = base.iloc[s:e]
            rows: List[Dict[str, object]] = []
            for _, r in chunk.iterrows():
                ticker = str(r["ticker"])
                snap = r["snapshot_date"]
                date_str = snap.isoformat() if pd.notna(snap) else None
                out = {"row_id": int(r["row_id"])}
                if not date_str:
                    out.update({"http_status": None, "asof_found": False, "asof_error": "invalid_snapshot_date"})
                else:
                    try:
                        out.update(fetch_ticker_overview_asof(session, api_key, ticker=ticker, date_str=date_str))
                        if not args.disable_events:
                            out.update(fetch_ticker_events(session, api_key, ticker_or_id=ticker, limit=args.events_limit))
                    except Exception as exc:
                        out.update({"http_status": None, "asof_found": False, "asof_error": str(exc)})
                rows.append(out)
                if args.sleep_between_calls > 0:
                    time.sleep(args.sleep_between_calls)

            pd.DataFrame(rows).to_parquet(ckpt_file, index=False)
            elapsed = int(time.time() - started)
            done = e
            avg = elapsed / done if done > 0 else 0.0
            eta = int(avg * (total - done)) if avg > 0 else None
            progress = {
                "status": "running",
                "rows_done": done,
                "rows_total": total,
                "progress_pct": round(100.0 * done / total, 2),
                "batch_index": b + 1,
                "batches_total": n_batches,
                "elapsed_sec": elapsed,
                "eta_sec": eta,
                "updated_at_utc": dt.datetime.now(dt.UTC).isoformat(),
            }
            progress_path.write_text(json.dumps(progress, indent=2), encoding="utf-8")
            print(f"batch {b+1}/{n_batches} rows={s}-{e-1} done={done}/{total} ({progress['progress_pct']:.2f}%) eta={eta}s")

    # Merge checkpoints
    parts = sorted(ckpt_dir.glob("batch_*.parquet"))
    if not parts:
        raise RuntimeError("No checkpoint batch files found.")
    frames = []
    for p in parts:
        f = pd.read_parquet(p)
        if len(f) > 0:
            frames.append(f)
    if not frames:
        raise RuntimeError("Checkpoint files exist but all are empty.")
    asof = pd.concat(frames, ignore_index=True)
    asof = asof.sort_values("row_id").drop_duplicates("row_id", keep="last")

    out = base.merge(asof, on="row_id", how="left")
    if "request_date" in out.columns and "request_snapshot_date" not in out.columns:
        out["request_snapshot_date"] = out["request_date"]
    if len(inactive_catalog) > 0:
        out = out.merge(inactive_catalog, on=["ticker", "primary_exchange"], how="left")

    # Campo final + source.
    # Requested traceability:
    # - request_ticker, request_snapshot_date, http_status, asof_found already in asof payload
    # - source_* values: asof/events/fallback/missing
    merge_specs = {
        "final_cik": ("asof_cik", "cik"),
        "final_composite_figi": ("asof_composite_figi", "composite_figi"),
        "final_share_class_figi": ("asof_share_class_figi", "share_class_figi"),
        "final_list_date": ("asof_list_date", "list_date"),
        "final_delisted_utc": ("asof_delisted_utc", "delisted_utc"),
        "final_primary_exchange": ("asof_primary_exchange", "primary_exchange"),
        "final_type": ("asof_type", "type"),
        "final_last_updated_utc": ("asof_last_updated_utc", "last_updated_utc"),
        "final_market_cap": ("asof_market_cap", None),
        "final_weighted_shares_outstanding": ("asof_weighted_shares_outstanding", None),
        "final_share_class_shares_outstanding": ("asof_share_class_shares_outstanding", None),
        "final_total_employees": ("asof_total_employees", None),
        "final_description": ("asof_description", None),
        "final_homepage_url": ("asof_homepage_url", None),
        "final_phone_number": ("asof_phone_number", None),
        "final_sic_code": ("asof_sic_code", None),
        "final_sic_description": ("asof_sic_description", None),
        "final_ticker_root": ("asof_ticker_root", None),
        "final_ticker_suffix": ("asof_ticker_suffix", None),
        "final_round_lot": ("asof_round_lot", None),
        "final_address_address1": ("asof_address_address1", None),
        "final_address_city": ("asof_address_city", None),
        "final_address_state": ("asof_address_state", None),
        "final_address_postal_code": ("asof_address_postal_code", None),
        "final_branding_logo_url": ("asof_branding_logo_url", None),
        "final_branding_icon_url": ("asof_branding_icon_url", None),
    }

    for final_col, (asof_col, fb_col) in merge_specs.items():
        vals = []
        srcs = []
        if fb_col is None:
            out[final_col] = out[asof_col]
            out[f"source_{final_col}"] = out[asof_col].notna().map({True: "asof", False: "missing"})
            continue
        # Use events as secondary source for identity/lifecycle fields when available
        event_col_map = {
            "final_cik": "event_cik",
            "final_composite_figi": "event_composite_figi",
            "final_share_class_figi": "event_share_class_figi",
            "final_delisted_utc": "event_execution_date",
        }
        inactive_col_map = {
            "final_cik": "inactive_cat_cik",
            "final_composite_figi": "inactive_cat_composite_figi",
            "final_share_class_figi": "inactive_cat_share_class_figi",
            "final_delisted_utc": "inactive_cat_delisted_utc",
            "final_last_updated_utc": "inactive_cat_last_updated_utc",
        }
        ev_col = event_col_map.get(final_col)
        cat_col = inactive_col_map.get(final_col)
        ev_series = out[ev_col] if ev_col in out.columns else pd.Series([None] * len(out))
        cat_series = out[cat_col] if cat_col in out.columns else pd.Series([None] * len(out))

        for a, catv, ev, f in zip(out[asof_col], cat_series, ev_series, out[fb_col]):
            v, s = choose_with_source_4(a, catv, ev, f)
            vals.append(v)
            srcs.append(s)
        out[final_col] = vals
        out[f"source_{final_col}"] = srcs

    # If delisted is still missing and entity is inactive, infer from last_seen_date.
    if "final_delisted_utc" in out.columns and "status" in out.columns and "last_seen_date" in out.columns:
        m_infer = out["final_delisted_utc"].isna() & out["status"].eq("inactive") & out["last_seen_date"].notna()
        out.loc[m_infer, "final_delisted_utc"] = out.loc[m_infer, "last_seen_date"].astype(str)
        src_col = "source_final_delisted_utc"
        if src_col in out.columns:
            out.loc[m_infer & out[src_col].isin(["missing", "fallback"]), src_col] = "inferred_last_seen"

    # QA summary
    qa = []
    for c in [
        "final_market_cap",
        "final_weighted_shares_outstanding",
        "final_share_class_shares_outstanding",
        "final_total_employees",
        "final_sic_code",
        "final_description",
        "final_homepage_url",
        "final_phone_number",
        "final_list_date",
        "final_delisted_utc",
        "final_cik",
        "final_composite_figi",
        "final_share_class_figi",
        "final_ticker_root",
        "final_ticker_suffix",
        "final_round_lot",
        "final_address_address1",
        "final_address_city",
        "final_address_state",
        "final_address_postal_code",
        "final_branding_logo_url",
        "final_branding_icon_url",
    ]:
        nn = int(out[c].notna().sum())
        qa.append({"field": c, "non_null": nn, "rows": len(out), "coverage_pct": round(100.0 * nn / len(out), 2)})
    qa_df = pd.DataFrame(qa).sort_values("coverage_pct", ascending=False)

    cov_fields = [r["field"] for r in qa]
    by_status_rows: List[Dict[str, object]] = []
    if "status" in out.columns:
        for st, g in out.groupby("status", dropna=False):
            row: Dict[str, object] = {"status": st, "rows": int(len(g))}
            for c in cov_fields:
                row[c] = round(100.0 * float(g[c].notna().mean()), 2) if len(g) else 0.0
            by_status_rows.append(row)
    qa_by_status_df = pd.DataFrame(by_status_rows)

    by_ex_rows: List[Dict[str, object]] = []
    ex_col = "final_primary_exchange" if "final_primary_exchange" in out.columns else "primary_exchange"
    if ex_col in out.columns:
        for ex, g in out.groupby(ex_col, dropna=False):
            row: Dict[str, object] = {"exchange": ex, "rows": int(len(g))}
            for c in cov_fields:
                row[c] = round(100.0 * float(g[c].notna().mean()), 2) if len(g) else 0.0
            by_ex_rows.append(row)
    qa_by_exchange_df = pd.DataFrame(by_ex_rows)

    # Save outputs
    qa_status_path = outdir / "universe_hybrid_enriched.qa.by_status.csv"
    qa_exchange_path = outdir / "universe_hybrid_enriched.qa.by_exchange.csv"
    out.to_parquet(enriched_path, index=False)
    qa_df.to_csv(qa_path, index=False)
    if len(qa_by_status_df):
        qa_by_status_df.to_csv(qa_status_path, index=False)
    if len(qa_by_exchange_df):
        qa_by_exchange_df.to_csv(qa_exchange_path, index=False)

    progress = {
        "status": "completed",
        "rows_done": total,
        "rows_total": total,
        "progress_pct": 100.0,
        "batches_total": n_batches,
        "updated_at_utc": dt.datetime.now(dt.UTC).isoformat(),
        "output_enriched": str(enriched_path),
        "output_qa": str(qa_path),
        "output_qa_by_status": str(qa_status_path),
        "output_qa_by_exchange": str(qa_exchange_path),
    }
    progress_path.write_text(json.dumps(progress, indent=2), encoding="utf-8")

    print(f"rows={len(out):,}")
    print(f"saved={enriched_path}")
    print(f"qa={qa_path}")
    print("coverage:")
    print(qa_df.to_string(index=False))


if __name__ == "__main__":
    main()
