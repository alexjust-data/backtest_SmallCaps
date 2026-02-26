from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class EraConfig:
    name: str
    ohlcv_root: Path
    quote_roots: tuple[Path, ...]


@dataclass(frozen=True)
class Thresholds:
    min_overlap_months: int = 3
    min_overlap_quote_days: int = 20
    continuity_gap_years: int = 2


@dataclass(frozen=True)
class PipelineConfig:
    project_root: Path
    data_root: Path
    universe_source: Path
    output_root: Path
    eras: tuple[EraConfig, ...]
    thresholds: Thresholds


def default_config(project_root: Path) -> PipelineConfig:
    data_root = Path(r"C:\TSIS_Data\data")
    return PipelineConfig(
        project_root=project_root,
        data_root=data_root,
        universe_source=project_root / "runs" / "backtest" / "01_dataset_builder" / "universe_audit_trail.parquet",
        output_root=project_root / "runs" / "data_quality" / "03_time_coverage_v3",
        eras=(
            EraConfig(
                name="2004_2018",
                ohlcv_root=data_root / "ohlcv_intraday_1m" / "2004_2018",
                quote_roots=(data_root / "quotes_p95_2004_2018",),
            ),
            EraConfig(
                name="2019_2025",
                ohlcv_root=data_root / "ohlcv_intraday_1m" / "2019_2025",
                # We include both roots because some environments keep duplicated/split mirrors.
                quote_roots=(data_root / "quotes_p95_2019_2025", data_root / "quotes_p95"),
            ),
        ),
        thresholds=Thresholds(),
    )
