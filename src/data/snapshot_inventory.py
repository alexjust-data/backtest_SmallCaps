from __future__ import annotations

"""
Snapshot Inventory (01) — audit-ready inventory of expected objects vs local cache.

This module is intentionally "low-cost": it does NOT validate the content of parquets.
It only checks existence, file sizes (and optionally a lightweight schema sniff), and writes
reproducible artifacts under runs/.

Artifacts written:
- run_metadata.json
- manifest_snapshot.json (optional)
- inventory.parquet
- schema_observed.parquet (optional)
- missing_objects.json
- anomalies.jsonl
- summary.json
"""

import argparse
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import polars as pl


# =============================================================================
# Helpers
# =============================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, obj: Any) -> None:
    safe_mkdir(path.parent)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    safe_mkdir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def local_cache_path(cache_dir: Path, key: str) -> Path:
    return cache_dir / key


def coerce_int_or_none(x: Any) -> Optional[int]:
    """
    Best-effort conversion to int.
    Accepts int/float/numeric strings. Returns None if not convertible.
    """
    if x is None or isinstance(x, bool):
        return None
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        if x != x:  # NaN
            return None
        return int(x)
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        s = s.replace(",", "").replace("_", "")
        try:
            f = float(s)
            if f != f:
                return None
            return int(f)
        except Exception:
            return None
    return None


# =============================================================================
# Manifest loading / normalization
# =============================================================================

def normalize_manifest_objects(objs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for o in objs:
        if "key" not in o:
            if "Key" in o:
                o = dict(o)
                o["key"] = o["Key"]
            else:
                raise ValueError(f"Manifest object missing 'key': {o}")

        if "size" not in o:
            if "Size" in o:
                o = dict(o)
                o["size"] = o["Size"]
            elif "expected_size_bytes" in o:
                o = dict(o)
                o["size"] = o["expected_size_bytes"]

        if "etag" not in o and "ETag" in o:
            o = dict(o)
            o["etag"] = o["ETag"]

        out.append(o)
    return out


def load_manifest(path: Path) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    if not path.exists():
        raise FileNotFoundError(f"Manifest not found: {path}")

    if path.suffix.lower() == ".jsonl":
        header: Dict[str, Any] = {"manifest_path": str(path)}
        objects: List[Dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                t = rec.get("type")
                if t == "header":
                    header.update({k: v for k, v in rec.items() if k != "type"})
                elif t == "object":
                    objects.append(rec.get("object", rec))
                elif t == "footer":
                    header["footer"] = {k: v for k, v in rec.items() if k != "type"}
                else:
                    objects.append(rec)
        return header, normalize_manifest_objects(objects)

    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return {"manifest_path": str(path)}, normalize_manifest_objects(data)

    if isinstance(data, dict):
        if "objects" in data and isinstance(data["objects"], list):
            header = {k: v for k, v in data.items() if k != "objects"}
            header["manifest_path"] = str(path)
            return header, normalize_manifest_objects(data["objects"])
        if "items" in data and isinstance(data["items"], list):
            header = {k: v for k, v in data.items() if k != "items"}
            header["manifest_path"] = str(path)
            return header, normalize_manifest_objects(data["items"])

    raise ValueError(f"Unsupported manifest format in {path}")


# =============================================================================
# Snapshot inventory
# =============================================================================

@dataclass(frozen=True)
class SnapshotConfig:
    slice_id: str
    manifest_path: Path
    cache_dir: Path
    runs_dir: Path
    run_id: str
    schema_sniff: bool = True
    schema_sniff_n_rows: int = 1
    save_manifest_snapshot: bool = True


def _stat_file(path: Path) -> Tuple[bool, Optional[int], Optional[str]]:
    if not path.exists():
        return False, None, None
    st = path.stat()
    return (
        True,
        int(st.st_size),
        datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat(),
    )


def _sniff_schema(path: Path, n_rows: int = 1) -> Dict[str, Any]:
    try:
        df = pl.read_parquet(path, n_rows=n_rows)
        return {
            "ok": True,
            "columns": df.columns,
            "dtypes": [str(dt) for dt in df.dtypes],
            "rows_read": df.height,
        }
    except Exception as e:
        return {"ok": False, "error": repr(e)}


def run_snapshot_inventory(cfg: SnapshotConfig) -> Dict[str, Any]:
    out_dir = (
        cfg.runs_dir
        / "data_quality"
        / "snapshot_inventory"
        / cfg.slice_id
        / cfg.run_id
    )
    safe_mkdir(out_dir)

    header, objects = load_manifest(cfg.manifest_path)
    manifest_sha256 = sha256_file(cfg.manifest_path)

    if cfg.save_manifest_snapshot:
        write_json(
            out_dir / "manifest_snapshot.json",
            {
                "header": header,
                "objects_preview_first_5": objects[:5],
                "n_objects": len(objects),
                "manifest_path": str(cfg.manifest_path),
                "manifest_sha256": manifest_sha256,
            },
        )

    write_json(
        out_dir / "run_metadata.json",
        {
            "slice_id": cfg.slice_id,
            "run_id": cfg.run_id,
            "created_at_utc": utc_now_iso(),
            "manifest_path": str(cfg.manifest_path),
            "manifest_sha256": manifest_sha256,
            "cache_dir": str(cfg.cache_dir.resolve()),
            "runs_dir": str(cfg.runs_dir.resolve()),
            "schema_sniff": cfg.schema_sniff,
            "schema_sniff_n_rows": cfg.schema_sniff_n_rows,
            "polars_version": pl.__version__,
            "python": os.sys.version,
        },
    )

    anomalies: List[Dict[str, Any]] = []
    inv_rows: List[Dict[str, Any]] = []
    schema_rows: List[Dict[str, Any]] = []

    for o in objects:
        key = o["key"]

        expected_size_raw = o.get("size")
        expected_size = coerce_int_or_none(expected_size_raw)

        if expected_size_raw is not None and expected_size is None:
            anomalies.append(
                {
                    "ts_utc": utc_now_iso(),
                    "level": "WARN",
                    "code": "EXPECTED_SIZE_NOT_NUMERIC",
                    "key": key,
                    "expected_size_raw": repr(expected_size_raw),
                }
            )

        p = local_cache_path(cfg.cache_dir, key)
        exists, local_size, local_mtime = _stat_file(p)

        size_ok: Optional[bool] = None
        if exists and expected_size is not None:
            size_ok = expected_size == int(local_size or -1)

        status = "OK"
        if not exists:
            status = "MISSING"
            anomalies.append(
                {
                    "ts_utc": utc_now_iso(),
                    "level": "ERROR",
                    "code": "MISSING_OBJECT",
                    "key": key,
                }
            )
        elif size_ok is False:
            status = "SIZE_MISMATCH"
            anomalies.append(
                {
                    "ts_utc": utc_now_iso(),
                    "level": "ERROR",
                    "code": "SIZE_MISMATCH",
                    "key": key,
                    "expected_size": expected_size,
                    "local_size": int(local_size or -1),
                }
            )

        row = {
            "key": key,
            "local_path": str(p),
            "exists": exists,
            "local_size": local_size,
            "local_mtime_utc": local_mtime,
            "expected_size": expected_size,
            "expected_etag": o.get("etag"),
            "expected_last_modified": o.get("last_modified")
            or o.get("LastModified"),
            "size_ok": size_ok,
            "status": status,
        }

        for k in ["dataset", "symbol", "year", "month", "day", "date", "era", "freq"]:
            if k in o:
                row[k] = o.get(k)

        inv_rows.append(row)

        if cfg.schema_sniff and exists and str(p).lower().endswith(".parquet"):
            sniff = _sniff_schema(p, n_rows=cfg.schema_sniff_n_rows)
            schema_rows.append(
                {
                    "key": key,
                    "local_path": str(p),
                    "sniff_ok": sniff.get("ok", False),
                    "columns": sniff.get("columns"),
                    "dtypes": sniff.get("dtypes"),
                    "rows_read": sniff.get("rows_read"),
                    "error": sniff.get("error"),
                }
            )

    pl.DataFrame(inv_rows).write_parquet(out_dir / "inventory.parquet")

    if cfg.schema_sniff and schema_rows:
        schema_rows_json = []
        for r in schema_rows:
            rr = dict(r)
            cols = rr.pop("columns", [])
            dts = rr.pop("dtypes", [])
            rr["columns_json"] = json.dumps(cols, ensure_ascii=False)
            rr["dtypes_json"] = json.dumps(dts, ensure_ascii=False)
            schema_rows_json.append(rr)

        pl.DataFrame(schema_rows_json).write_parquet(
            out_dir / "schema_observed.parquet"
        )

    missing = [r["key"] for r in inv_rows if r["status"] == "MISSING"]
    write_json(
        out_dir / "missing_objects.json",
        {"missing": missing, "n_missing": len(missing)},
    )

    n_expected = len(inv_rows)
    n_found = sum(1 for r in inv_rows if r["exists"])
    n_missing = len(missing)
    n_size_mismatch = sum(1 for r in inv_rows if r["status"] == "SIZE_MISMATCH")
    coverage = n_found / n_expected if n_expected else 0.0

    overall = "PASS"
    if n_missing > 0 or n_size_mismatch > 0:
        overall = "FAIL"
    elif coverage < 1.0:
        overall = "WARN"

    summary = {
        "slice_id": cfg.slice_id,
        "run_id": cfg.run_id,
        "created_at_utc": utc_now_iso(),
        "n_expected": n_expected,
        "n_found": n_found,
        "n_missing": n_missing,
        "n_size_mismatch": n_size_mismatch,
        "coverage": coverage,
        "overall_status": overall,
        "out_dir": str(out_dir),
        "manifest_sha256": manifest_sha256,
    }

    write_json(out_dir / "summary.json", summary)
    write_jsonl(out_dir / "anomalies.jsonl", anomalies)

    return summary


# =============================================================================
# CLI
# =============================================================================

def main() -> None:
    p = argparse.ArgumentParser(
        description="Snapshot inventory vs local cache (audit-ready)."
    )
    p.add_argument("--slice-id", required=True)
    p.add_argument("--manifest", required=True)
    p.add_argument("--cache-dir", required=True)
    p.add_argument("--runs-dir", default="./runs")
    p.add_argument("--run-id", default=None)
    p.add_argument("--no-schema-sniff", action="store_true")
    p.add_argument("--schema-sniff-n-rows", type=int, default=1)
    p.add_argument("--no-manifest-snapshot", action="store_true")
    args = p.parse_args()

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    cfg = SnapshotConfig(
        slice_id=args.slice_id,
        manifest_path=Path(args.manifest),
        cache_dir=Path(args.cache_dir),
        runs_dir=Path(args.runs_dir),
        run_id=run_id,
        schema_sniff=not args.no_schema_sniff,
        schema_sniff_n_rows=args.schema_sniff_n_rows,
        save_manifest_snapshot=not args.no_manifest_snapshot,
    )

    summary = run_snapshot_inventory(cfg)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
