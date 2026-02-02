from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple
from collections import Counter

import polars as pl

from src.core.settings import settings


@dataclass
class Anomaly:
    severity: str   # FAIL / WARN / INFO
    dataset: str
    key: str
    message: str
    details: Dict[str, Any]


def ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def to_scalar(value: Any) -> Any:
    if hasattr(value, "item"):
        return value.item()
    return value


def load_manifest_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def local_cache_path(key: str) -> str:
    # Mirror R2 key structure under local cache dir
    return os.path.join(settings.DATA_CACHE_DIR, key)


def read_parquet_cached(key: str, columns: List[str] | None = None) -> pl.DataFrame:
    """
    Reads parquet from local cache. For now we assume you already sync/download.
    Next step: we can add a fetch-on-miss from R2 (controlled + cached).
    """
    path = local_cache_path(key)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Missing local cache file: {path}. "
            f"Sync/download this parquet to {settings.DATA_CACHE_DIR} preserving key paths."
        )
    return pl.read_parquet(path, columns=columns)


def detect_ts_column(df: pl.DataFrame) -> str:
    # Prefer provider canonical names first
    for c in ["timestamp", "participant_timestamp", "ts", "datetime", "time", "t"]:
        if c in df.columns:
            return c
    raise ValueError(f"No timestamp column found. Columns: {df.columns}")


def normalize_quotes(df: pl.DataFrame) -> pl.DataFrame:
    """
    Map provider-specific column names to canonical names used by checks:
    - bid_price -> bid
    - ask_price -> ask
    Keeps original columns too (we don't drop anything here).
    """
    rename_map: Dict[str, str] = {}
    if "bid" not in df.columns and "bid_price" in df.columns:
        rename_map["bid_price"] = "bid"
    if "ask" not in df.columns and "ask_price" in df.columns:
        rename_map["ask_price"] = "ask"
    if rename_map:
        df = df.rename(rename_map)
    return df


def normalize_ohlcv(df: pl.DataFrame) -> pl.DataFrame:
    """
    Placeholder for OHLCV normalization if needed later (e.g., o/h/l/c/v).
    Currently no-op.
    """
    return df


def check_ohlcv(df: pl.DataFrame, key: str) -> List[Anomaly]:
    anoms: List[Anomaly] = []
    df = normalize_ohlcv(df)
    ts_col = detect_ts_column(df)

    required = ["open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        anoms.append(Anomaly("FAIL", "ohlcv_intraday_1m", key, "Missing required columns", {"missing": missing}))
        return anoms

    # Basic domain checks
    # prices positive
    for c in ["open", "high", "low", "close"]:
        bad = df.filter(pl.col(c) <= 0).height
        if bad:
            anoms.append(Anomaly("FAIL", "ohlcv_intraday_1m", key, f"Non-positive prices in {c}", {"rows": bad}))

    # volume non-negative
    bad_vol = df.filter(pl.col("volume") < 0).height
    if bad_vol:
        anoms.append(Anomaly("FAIL", "ohlcv_intraday_1m", key, "Negative volume", {"rows": bad_vol}))

    # OHLC invariants
    bad_ohlc = df.filter(
        (pl.col("low") > pl.col("high")) |
        (pl.col("open") < pl.col("low")) | (pl.col("open") > pl.col("high")) |
        (pl.col("close") < pl.col("low")) | (pl.col("close") > pl.col("high"))
    ).height
    if bad_ohlc:
        anoms.append(Anomaly("FAIL", "ohlcv_intraday_1m", key, "OHLC invariants violated", {"rows": bad_ohlc}))

    # Duplicate timestamps
    dup = int(to_scalar(df.select(pl.col(ts_col)).is_duplicated().sum()))
    if dup and dup > 0:
        anoms.append(Anomaly("FAIL", "ohlcv_intraday_1m", key, "Duplicate timestamps", {"duplicates": int(dup)}))

    return anoms


def check_quotes(df: pl.DataFrame, key: str, dataset: str) -> List[Anomaly]:
    anoms: List[Anomaly] = []
    df = normalize_quotes(df)
    ts_col = detect_ts_column(df)

    required = ["bid", "ask"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        anoms.append(
            Anomaly(
                "FAIL",
                dataset,
                key,
                "Missing required columns",
                {
                    "missing": missing,
                    "columns": df.columns,
                    "dtypes": [str(t) for t in df.dtypes],
                },
            )
        )
        return anoms

    bad_bid = df.filter(pl.col("bid") <= 0).height
    bad_ask = df.filter(pl.col("ask") <= 0).height
    if bad_bid:
        anoms.append(Anomaly("FAIL", dataset, key, "Non-positive bid", {"rows": bad_bid}))
    if bad_ask:
        anoms.append(Anomaly("FAIL", dataset, key, "Non-positive ask", {"rows": bad_ask}))

    crossed = df.filter(pl.col("bid") > pl.col("ask")).height
    if crossed:
        # classify as WARN initially; later we set thresholds
        anoms.append(Anomaly("WARN", dataset, key, "Crossed market (bid > ask) rows", {"rows": crossed}))

    # Duplicate timestamps (not always fatal in quotes, but should be measured)
    dup = int(to_scalar(df.select(pl.col(ts_col)).is_duplicated().sum()))
    if dup and dup > 0:
        anoms.append(Anomaly("WARN", dataset, key, "Duplicate quote timestamps", {"duplicates": int(dup)}))

    return anoms


def severity_rank(sev: str) -> int:
    return {"FAIL": 3, "WARN": 2, "INFO": 1}.get(sev, 0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True, help="Path to manifest .json (slice) produced by build_manifest.py")
    ap.add_argument("--out_dir", required=True, help="Output directory for validation artifacts")
    args = ap.parse_args()

    ensure_dir(os.path.join(args.out_dir, "x"))  # create folder

    manifest = load_manifest_json(args.manifest)
    objs: List[Dict[str, Any]] = manifest["objects"]

    anoms: List[Anomaly] = []
    stats: Dict[str, Any] = {
        "manifest": args.manifest,
        "checked_files": 0,
        "ohlcv_files": 0,
        "quotes_files": 0,
        "fail": 0,
        "warn": 0,
        "info": 0,
    }

    for o in objs:
        dataset = o["dataset"]
        key = o["key"]

        # Only handle the datasets we know now
        if dataset == "ohlcv_intraday_1m":
            stats["ohlcv_files"] += 1
            df = read_parquet_cached(key)  # later: add scan + lazy
            anoms.extend(check_ohlcv(df, key))

        elif dataset in ("quotes_p95", "quotes_test"):
            stats["quotes_files"] += 1
            df = read_parquet_cached(key)
            anoms.extend(check_quotes(df, key, dataset))

        stats["checked_files"] += 1

    # Count severities
    for a in anoms:
        if a.severity == "FAIL":
            stats["fail"] += 1
        elif a.severity == "WARN":
            stats["warn"] += 1
        else:
            stats["info"] += 1

    overall = "PASS"
    if stats["fail"] > 0:
        overall = "FAIL"
    elif stats["warn"] > 0:
        overall = "WARN"
    stats["overall"] = overall

    # Write anomalies as JSONL
    anomalies_path = os.path.join(args.out_dir, "anomalies.jsonl")
    with open(anomalies_path, "w", encoding="utf-8") as f:
        for a in anoms:
            f.write(
                json.dumps(
                    {
                        "severity": a.severity,
                        "dataset": a.dataset,
                        "key": a.key,
                        "message": a.message,
                        "details": a.details,
                    }
                )
                + "\n"
            )

    # Write summary metrics
    metrics_path = os.path.join(args.out_dir, "quality_metrics.json")
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, sort_keys=True)

    msg_counter = Counter((a.severity, a.message) for a in anoms)
    print("\nTop anomalies:")
    for (sev, msg), n in msg_counter.most_common(10):
        print(f"  {sev:4} {n:6} {msg}")

    print(f"\n✅ Validation complete: {overall}")
    print(f"   metrics:   {metrics_path}")
    print(f"   anomalies: {anomalies_path}")


if __name__ == "__main__":
    main()
