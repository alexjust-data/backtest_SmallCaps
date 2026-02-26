from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from .contracts import EraConfig


def _iter_year_dirs(base: Path, ticker: str) -> Iterable[Path]:
    p = base / ticker
    if not p.exists():
        return []
    return [y for y in p.glob("year=*") if y.is_dir()]


def quote_month_to_days(quote_roots: tuple[Path, ...], ticker: str) -> dict[tuple[int, int], int]:
    out: dict[tuple[int, int], int] = {}
    for qroot in quote_roots:
        for y in _iter_year_dirs(qroot, ticker):
            try:
                yy = int(y.name.split("=", 1)[1])
            except Exception:
                continue
            for m in y.glob("month=*"):
                if not m.is_dir():
                    continue
                try:
                    mm = int(m.name.split("=", 1)[1])
                except Exception:
                    continue
                n_days = sum(1 for _ in m.glob("day=*/quotes.parquet"))
                if n_days <= 0:
                    continue
                k = (yy, mm)
                out[k] = max(out.get(k, 0), n_days)
    return out


def ohlcv_months(ohlcv_root: Path, ticker: str) -> set[tuple[int, int]]:
    out: set[tuple[int, int]] = set()
    for y in _iter_year_dirs(ohlcv_root, ticker):
        try:
            yy = int(y.name.split("=", 1)[1])
        except Exception:
            continue
        for m in y.glob("month=*"):
            if not m.is_dir():
                continue
            try:
                mm = int(m.name.split("=", 1)[1])
            except Exception:
                continue
            if (m / "minute.parquet").exists():
                out.add((yy, mm))
    return out


def compute_ticker_evidence(ticker: str, eras: tuple[EraConfig, ...]) -> dict:
    per_era = []
    all_quote_months: set[tuple[int, int]] = set()
    all_ohlcv_months: set[tuple[int, int]] = set()
    overlap_quote_days_total = 0

    for era in eras:
        q_map = quote_month_to_days(era.quote_roots, ticker)
        o_set = ohlcv_months(era.ohlcv_root, ticker)
        q_set = set(q_map.keys())
        ov = sorted(q_set & o_set)

        overlap_days = sum(q_map.get(k, 0) for k in ov)
        overlap_quote_days_total += overlap_days

        all_quote_months |= q_set
        all_ohlcv_months |= o_set

        per_era.append(
            {
                "era": era.name,
                "q_months_n": len(q_set),
                "o_months_n": len(o_set),
                "overlap_months_n": len(ov),
                "overlap_quote_days": overlap_days,
                "first_overlap": ov[0] if ov else None,
                "last_overlap": ov[-1] if ov else None,
            }
        )

    all_ov = sorted(all_quote_months & all_ohlcv_months)
    q_first = min(all_quote_months) if all_quote_months else None
    q_last = max(all_quote_months) if all_quote_months else None
    o_first = min(all_ohlcv_months) if all_ohlcv_months else None
    year_gap = None
    if q_last is not None and o_first is not None:
        year_gap = int(o_first[0] - q_last[0])

    return {
        "ticker": ticker,
        "q_months_total": len(all_quote_months),
        "o_months_total": len(all_ohlcv_months),
        "overlap_months_total": len(all_ov),
        "overlap_quote_days_total": int(overlap_quote_days_total),
        "q_first": q_first,
        "q_last": q_last,
        "o_first": o_first,
        "o_last": max(all_ohlcv_months) if all_ohlcv_months else None,
        "year_gap_o_first_minus_q_last": year_gap,
        "has_overlap_any": len(all_ov) > 0,
        "per_era": per_era,
    }
