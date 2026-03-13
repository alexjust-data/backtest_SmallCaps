from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from pandas.errors import ParserError


def read_events(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except ParserError:
        try:
            return pd.read_csv(path, engine="python", on_bad_lines="skip")
        except Exception:
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def read_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def format_dt(s: str | None) -> str:
    if not s:
        return "-"
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(s)


def render(events: pd.DataFrame, state: dict, last_n: int) -> None:
    print(f"time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    total_processed = state.get("total_processed", 0)
    updated_utc = format_dt(state.get("updated_utc"))
    print(f"state.total_processed: {total_processed}")
    print(f"state.updated_utc   : {updated_utc}")

    if events.empty:
        print("events: empty")
        return

    print(f"events.rows         : {len(events)}")
    if "severity" in events.columns:
        sev = events["severity"].value_counts(dropna=False)
        print("severity.counts     :")
        for k, v in sev.items():
            print(f"  - {k}: {v}")

    if "processed_at_utc" in events.columns:
        print(f"events.latest_utc   : {events['processed_at_utc'].iloc[-1]}")

    if {"crossed_rows", "rows"}.issubset(set(events.columns)):
        tmp = events.copy()
        tmp["crossed_rows"] = pd.to_numeric(tmp["crossed_rows"], errors="coerce").fillna(0)
        tmp["rows"] = pd.to_numeric(tmp["rows"], errors="coerce").fillna(0)
        total_crossed = int(tmp["crossed_rows"].sum())
        total_rows = int(tmp["rows"].sum())
        ratio = (100.0 * total_crossed / total_rows) if total_rows > 0 else 0.0
        print(f"crossed.total_rows  : {total_crossed}")
        print(f"crossed.ratio_pct   : {ratio:.6f}")

    print("last.events         :")
    cols = [c for c in ["processed_at_utc", "severity", "rows", "crossed_rows", "action", "file"] if c in events.columns]
    tail = events[cols].tail(last_n)
    print(tail.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor de agente quotes (laxo/estricto) en terminal")
    parser.add_argument("--events", required=True, help="Path a events.csv")
    parser.add_argument("--state", required=True, help="Path a state.json")
    parser.add_argument("--interval", type=int, default=10, help="Segundos entre refrescos")
    parser.add_argument("--last-n", type=int, default=10, help="Ultimos N eventos a mostrar")
    parser.add_argument("--once", action="store_true", help="Ejecutar una sola vez")
    args = parser.parse_args()

    events_path = Path(args.events)
    state_path = Path(args.state)

    while True:
        os.system("cls" if os.name == "nt" else "clear")
        print("quotes-agent-monitor")
        print(f"events: {events_path}")
        print(f"state : {state_path}")
        print("-" * 100)

        events = read_events(events_path)
        state = read_state(state_path)
        render(events, state, args.last_n)

        if args.once:
            break
        time.sleep(max(1, args.interval))


if __name__ == "__main__":
    main()
