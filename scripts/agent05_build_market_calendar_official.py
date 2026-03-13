#!/usr/bin/env python
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

try:
    import exchange_calendars as xcals
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "exchange_calendars no esta disponible. Instala dependencia para generar calendario oficial."
    ) from exc


@dataclass(frozen=True)
class Config:
    calendar: str
    start: str
    end: str
    tz: str
    out_dir: Path
    write_csv: bool
    write_parquet: bool
    write_yearly: bool


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def build_calendar_df(cfg: Config) -> pd.DataFrame:
    cal = xcals.get_calendar(cfg.calendar, start=cfg.start, end=cfg.end)
    sched = cal.schedule.loc[cfg.start:cfg.end].copy()
    if sched.empty:
        raise RuntimeError(
            f"Calendario vacio para {cfg.calendar} en rango {cfg.start}..{cfg.end}"
        )

    sched = sched.reset_index().rename(columns={"index": "session_date"})
    sched["session_date"] = pd.to_datetime(sched["session_date"]).dt.tz_localize(None)

    sched["open_utc"] = pd.to_datetime(sched["open"], utc=True)
    sched["close_utc"] = pd.to_datetime(sched["close"], utc=True)

    # Convertimos a ET para chequeos operativos (early close)
    sched["open_et"] = sched["open_utc"].dt.tz_convert(cfg.tz)
    sched["close_et"] = sched["close_utc"].dt.tz_convert(cfg.tz)

    # Early close aproximado: cierre antes de las 16:00 ET
    sched["is_early_close"] = (
        sched["close_et"].dt.hour < 16
    ) | ((sched["close_et"].dt.hour == 16) & (sched["close_et"].dt.minute == 0) & False)

    out = pd.DataFrame(
        {
            "session_date": sched["session_date"].dt.strftime("%Y-%m-%d"),
            "open_utc": sched["open_utc"].dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "close_utc": sched["close_utc"].dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "open_et": sched["open_et"].dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "close_et": sched["close_et"].dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "is_early_close": sched["is_early_close"].astype(bool),
        }
    )
    out["year"] = pd.to_datetime(out["session_date"]).dt.year.astype(int)
    out["month"] = pd.to_datetime(out["session_date"]).dt.month.astype(int)
    out["dow"] = pd.to_datetime(out["session_date"]).dt.day_name()
    out["calendar"] = cfg.calendar
    out["timezone"] = cfg.tz

    # Validaciones minimas de integridad
    if out["session_date"].duplicated().any():
        dup = out[out["session_date"].duplicated()]["session_date"].head(5).tolist()
        raise RuntimeError(f"Fechas duplicadas en calendario: {dup}")

    out = out.sort_values("session_date").reset_index(drop=True)
    return out


def write_outputs(df: pd.DataFrame, cfg: Config) -> dict:
    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    base_name = f"market_calendar_official_{cfg.calendar}_{cfg.start}_{cfg.end}".replace("-", "")
    written = []

    if cfg.write_parquet:
        p = cfg.out_dir / f"{base_name}.parquet"
        df.to_parquet(p, index=False)
        written.append(p)

    if cfg.write_csv:
        c = cfg.out_dir / f"{base_name}.csv"
        df.to_csv(c, index=False)
        written.append(c)

    if cfg.write_yearly:
        for year, sub in df.groupby("year", sort=True):
            if cfg.write_parquet:
                yp = cfg.out_dir / f"market_calendar_official_{year}.parquet"
                sub.to_parquet(yp, index=False)
                written.append(yp)
            if cfg.write_csv:
                yc = cfg.out_dir / f"market_calendar_official_{year}.csv"
                sub.to_csv(yc, index=False)
                written.append(yc)

    meta = {
        "calendar": cfg.calendar,
        "timezone": cfg.tz,
        "start": cfg.start,
        "end": cfg.end,
        "sessions": int(len(df)),
        "years": int(df["year"].nunique()),
        "early_close_sessions": int(df["is_early_close"].sum()),
        "first_session": str(df["session_date"].iloc[0]),
        "last_session": str(df["session_date"].iloc[-1]),
        "files": [],
    }

    for path in written:
        meta["files"].append(
            {
                "path": str(path),
                "size_bytes": int(path.stat().st_size),
                "sha256": _sha256(path),
            }
        )

    meta_path = cfg.out_dir / f"{base_name}.meta.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    return {"meta": meta, "meta_path": str(meta_path)}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Agent05: genera calendario oficial de mercado versionado (NYSE/NASDAQ via exchange_calendars)."
    )
    ap.add_argument("--calendar", default="XNYS", help="Codigo de calendario exchange_calendars (ej: XNYS, XNAS).")
    ap.add_argument("--start", default="2005-01-01", help="Fecha inicio YYYY-MM-DD")
    ap.add_argument("--end", default="2025-12-31", help="Fecha fin YYYY-MM-DD")
    ap.add_argument("--tz", default="America/New_York", help="Timezone para representacion local")
    ap.add_argument(
        "--out-dir",
        default=r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference",
        help="Directorio de salida",
    )
    ap.add_argument("--no-csv", action="store_true", help="No escribir CSV")
    ap.add_argument("--no-parquet", action="store_true", help="No escribir Parquet")
    ap.add_argument("--no-yearly", action="store_true", help="No escribir particion anual")
    return ap.parse_args()


def main() -> None:
    ns = parse_args()
    cfg = Config(
        calendar=ns.calendar,
        start=ns.start,
        end=ns.end,
        tz=ns.tz,
        out_dir=Path(ns.out_dir),
        write_csv=not ns.no_csv,
        write_parquet=not ns.no_parquet,
        write_yearly=not ns.no_yearly,
    )

    if not cfg.write_csv and not cfg.write_parquet:
        raise SystemExit("Debes habilitar al menos un formato de salida (CSV o Parquet)")

    df = build_calendar_df(cfg)
    out = write_outputs(df, cfg)

    print(json.dumps(
        {
            "status": "ok",
            "calendar": cfg.calendar,
            "start": cfg.start,
            "end": cfg.end,
            "out_dir": str(cfg.out_dir),
            "sessions": int(len(df)),
            "years": int(df["year"].nunique()),
            "early_close_sessions": int(df["is_early_close"].sum()),
            "first_session": str(df["session_date"].iloc[0]),
            "last_session": str(df["session_date"].iloc[-1]),
            "meta_json": out["meta_path"],
        },
        ensure_ascii=False,
    ))


if __name__ == "__main__":
    main()
