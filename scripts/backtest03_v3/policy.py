from __future__ import annotations

from .contracts import Thresholds


def classify_evidence(row: dict, thr: Thresholds) -> tuple[str, str]:
    overlap_months = int(row.get("overlap_months_total", 0) or 0)
    overlap_days = int(row.get("overlap_quote_days_total", 0) or 0)
    year_gap = row.get("year_gap_o_first_minus_q_last")

    if overlap_months <= 0:
        if year_gap is not None and year_gap >= thr.continuity_gap_years:
            return "excluded", "no_overlap_symbol_continuity_gap"
        return "excluded", "no_overlap_quotes_ohlcv_2004_2025"

    if overlap_months < thr.min_overlap_months:
        return "manual_review", "low_overlap_months"

    if overlap_days < thr.min_overlap_quote_days:
        return "manual_review", "low_overlap_quote_days"

    return "eligible", "eligible_overlap_multi_era"
