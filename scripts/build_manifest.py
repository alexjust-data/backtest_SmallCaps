from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional

from src.core.settings import settings
from src.data.r2_client import get_r2_client
from src.data.catalog import parse_r2_key


def iter_objects(prefix: str) -> Iterator[Dict[str, Any]]:
    s3 = get_r2_client()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=settings.R2_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield {
                "key": obj["Key"],
                "size": int(obj["Size"]),
                "last_modified": obj["LastModified"].astimezone(timezone.utc).isoformat(),
                "etag": obj.get("ETag", "").strip('"'),
            }


def ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def write_json(path: str, payload: Dict[str, Any]) -> None:
    ensure_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", action="append", required=True)
    ap.add_argument("--out", required=True, help="Output manifest (.json or .jsonl)")
    ap.add_argument("--limit", type=int, default=0, help="Stop after N listed objects (0 = no limit)")
    ap.add_argument(
        "--mode",
        choices=["json", "jsonl"],
        default=None,
        help="Force output mode. Default inferred from --out extension.",
    )
    args = ap.parse_args()

    created_at = datetime.now(timezone.utc).isoformat()
    mode = args.mode
    if mode is None:
        mode = "jsonl" if args.out.endswith(".jsonl") else "json"

    listed_total = 0
    recognized = 0
    unrecognized = 0

    header = {
        "created_at": created_at,
        "bucket": settings.R2_BUCKET,
        "endpoint": settings.R2_ENDPOINT,
        "prefixes": args.prefix,
        "format": mode,
    }

    ensure_dir(args.out)

    if mode == "json":
        # JSON mode is fine for small slices. For huge inventories, use jsonl.
        objects = []
        for p in args.prefix:
            for o in iter_objects(p):
                listed_total += 1
                pk = parse_r2_key(o["key"])
                if pk is None:
                    unrecognized += 1
                else:
                    recognized += 1
                    objects.append(
                        {
                            **o,
                            "dataset": pk.dataset,
                            "symbol": pk.symbol,
                            "year": pk.year,
                            "month": pk.month,
                            "day": pk.day,
                            "era": pk.era,
                        }
                    )

                if args.limit and listed_total >= args.limit:
                    break
            if args.limit and listed_total >= args.limit:
                break

        manifest = {
            **header,
            "counts": {
                "listed_objects_total": listed_total,
                "recognized_objects": recognized,
                "unrecognized_objects": unrecognized,
            },
            "objects": objects,
        }
        write_json(args.out, manifest)
        print(f"✅ Manifest written: {args.out}")
        print(json.dumps(manifest["counts"], indent=2))
        return

    # JSONL mode: write header + one record per line
    counts_path = args.out.replace(".jsonl", "_counts.json")
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(json.dumps({"type": "header", **header}) + "\n")

        for p in args.prefix:
            for o in iter_objects(p):
                listed_total += 1
                pk = parse_r2_key(o["key"])
                if pk is None:
                    unrecognized += 1
                    rec: Optional[Dict[str, Any]] = None
                else:
                    recognized += 1
                    rec = {
                        **o,
                        "dataset": pk.dataset,
                        "symbol": pk.symbol,
                        "year": pk.year,
                        "month": pk.month,
                        "day": pk.day,
                        "era": pk.era,
                    }

                if rec is not None:
                    f.write(json.dumps({"type": "object", **rec}) + "\n")

                if args.limit and listed_total >= args.limit:
                    break
            if args.limit and listed_total >= args.limit:
                break

        counts = {
            "listed_objects_total": listed_total,
            "recognized_objects": recognized,
            "unrecognized_objects": unrecognized,
        }

        # write footer line for convenience
        f.write(json.dumps({"type": "footer", "counts": counts}) + "\n")

    write_json(counts_path, {"counts": counts, **header})
    print(f"✅ Manifest written: {args.out}")
    print(f"✅ Counts written: {counts_path}")
    print(json.dumps(counts, indent=2))


if __name__ == "__main__":
    main()
