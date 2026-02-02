from __future__ import annotations

import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from src.core.settings import settings
from src.data.r2_client import get_r2_client


@dataclass(frozen=True)
class ObjRef:
    key: str
    size: int
    etag: str


def load_manifest(path: str) -> List[ObjRef]:
    """
    Supports manifest .json produced by build_manifest.py (json mode).
    For jsonl mode, we can extend later.
    """
    with open(path, "r", encoding="utf-8") as f:
        m = json.load(f)

    objs = m.get("objects", [])
    out: List[ObjRef] = []
    for o in objs:
        out.append(
            ObjRef(
                key=o["key"],
                size=int(o.get("size", 0)),
                etag=str(o.get("etag", "")),
            )
        )
    return out


def local_path_for_key(key: str) -> str:
    return os.path.join(settings.DATA_CACHE_DIR, key)


def ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def file_ok(path: str, expected_size: int) -> bool:
    if not os.path.exists(path):
        return False
    if expected_size and os.path.getsize(path) != expected_size:
        return False
    return True


def download_one(obj: ObjRef, force: bool = False) -> Tuple[str, str]:
    """
    Returns (key, status) where status in {"downloaded","skipped","re-downloaded"}
    """
    s3 = get_r2_client()
    dst = local_path_for_key(obj.key)

    if not force and file_ok(dst, obj.size):
        return (obj.key, "skipped")

    ensure_parent_dir(dst)
    # Atomic-ish download: write to tmp then rename
    tmp = dst + ".part"

    s3.download_file(settings.R2_BUCKET, obj.key, tmp)
    os.replace(tmp, dst)

    # Recheck size (very important)
    if obj.size and os.path.getsize(dst) != obj.size:
        raise RuntimeError(f"Size mismatch after download: {obj.key} expected {obj.size} got {os.path.getsize(dst)}")

    return (obj.key, "downloaded" if not os.path.exists(dst) else "re-downloaded")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True, help="Path to manifest .json")
    ap.add_argument(
        "--max_workers", type=int, default=settings.MAX_WORKERS, help="Parallel downloads"
    )
    ap.add_argument("--force", action="store_true", help="Re-download even if file exists with same size")
    ap.add_argument("--limit", type=int, default=0, help="Download only first N objects (0 = no limit)")
    args = ap.parse_args()

    objs = load_manifest(args.manifest)
    if args.limit:
        objs = objs[: args.limit]

    total = len(objs)
    print(f"Syncing {total} objects into {settings.DATA_CACHE_DIR}")

    downloaded = 0
    skipped = 0

    # Thread pool works fine for network IO
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futs = [ex.submit(download_one, o, args.force) for o in objs]
        for fut in as_completed(futs):
            key, status = fut.result()
            if status == "skipped":
                skipped += 1
            else:
                downloaded += 1

            if (downloaded + skipped) % 10 == 0 or (downloaded + skipped) == total:
                print(f"  progress: {downloaded+skipped}/{total}  downloaded={downloaded} skipped={skipped}")

    print("✅ Sync complete")
    print({"total": total, "downloaded": downloaded, "skipped": skipped})


if __name__ == "__main__":
    main()
