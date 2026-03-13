from pathlib import Path
import ast
import json
import pandas as pd
import matplotlib.pyplot as plt

RUN_DIR = Path(globals().get("RUN_DIR", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\ohlcv_daily_audit\20260309_ohlcv_daily_session_01"))
EVENTS_CURRENT_CSV = Path(globals().get("EVENTS_CURRENT_CSV", RUN_DIR / "ohlcv_daily_events_current.csv"))
EVENTS_HISTORY_CSV = Path(globals().get("EVENTS_HISTORY_CSV", RUN_DIR / "ohlcv_daily_events_history.csv"))
LIVE_STATUS_JSON = Path(globals().get("LIVE_STATUS_JSON", RUN_DIR / "live_status_ohlcv_daily.json"))
OUT_DIR = Path(globals().get("OUT_DIR", RUN_DIR / "audit_plots_daily"))
OUT_DIR.mkdir(parents=True, exist_ok=True)

TOP_N_CAUSES = int(globals().get("TOP_N_CAUSES", 15))
FIGSIZE = tuple(globals().get("FIGSIZE", (12, 5)))


def _parse_listlike(v):
    if isinstance(v, list):
        return [str(x) for x in v]
    if pd.isna(v):
        return []
    s = str(v).strip()
    if s in ("", "[]", "nan", "None"):
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            x = json.loads(s.replace("'", '"'))
            if isinstance(x, list):
                return [str(i) for i in x]
        except Exception:
            try:
                x = ast.literal_eval(s)
                if isinstance(x, list):
                    return [str(i) for i in x]
            except Exception:
                pass
    return [s]


if not EVENTS_CURRENT_CSV.exists():
    raise FileNotFoundError(f"No existe EVENTS_CURRENT_CSV: {EVENTS_CURRENT_CSV}")

cur = pd.read_csv(EVENTS_CURRENT_CSV)
hist = pd.read_csv(EVENTS_HISTORY_CSV) if EVENTS_HISTORY_CSV.exists() else pd.DataFrame()

print("RUN_DIR:", RUN_DIR)
print("EVENTS_CURRENT rows:", len(cur))
print("EVENTS_HISTORY rows:", len(hist))

# 1) Severidad current
if "severity" in cur.columns and not cur.empty:
    sev = cur.groupby("severity", dropna=False).size().reset_index(name="count").sort_values("count", ascending=False)
    plt.figure(figsize=FIGSIZE)
    plt.bar(sev["severity"].astype(str), sev["count"].astype(int))
    plt.title("OHLCV Daily Audit - Severity (current)")
    plt.xlabel("Severity")
    plt.ylabel("Files")
    plt.tight_layout()
    p = OUT_DIR / "01_severity_current.png"
    plt.savefig(p, dpi=140)
    plt.close()
    print("saved:", p)
    print(sev.to_string(index=False))

# 2) Evolucion acumulada por corrida/tiempo
if not hist.empty and "processed_at_utc" in hist.columns and "severity" in hist.columns:
    h = hist.copy()
    h["processed_at_utc"] = pd.to_datetime(h["processed_at_utc"], errors="coerce", utc=True)
    h = h.dropna(subset=["processed_at_utc"])
    if not h.empty:
        h["date_hour"] = h["processed_at_utc"].dt.floor("h")
        ts = h.groupby(["date_hour", "severity"], dropna=False).size().reset_index(name="count")
        piv = ts.pivot(index="date_hour", columns="severity", values="count").fillna(0).sort_index()
        cum = piv.cumsum()
        plt.figure(figsize=(12, 6))
        for c in cum.columns:
            plt.plot(cum.index, cum[c], label=str(c))
        plt.title("OHLCV Daily Audit - Cumulative processed by severity")
        plt.xlabel("UTC hour")
        plt.ylabel("Cumulative files")
        plt.legend()
        plt.tight_layout()
        p = OUT_DIR / "02_cumulative_by_severity.png"
        plt.savefig(p, dpi=140)
        plt.close()
        print("saved:", p)

# 3) Top causas (issues + warns) en current
rows = []
for _, r in cur.iterrows():
    for it in _parse_listlike(r.get("issues", [])):
        rows.append(("issue", it))
    for wt in _parse_listlike(r.get("warns", [])):
        rows.append(("warn", wt))

if rows:
    cdf = pd.DataFrame(rows, columns=["cause_type", "cause"])
    top = cdf.groupby(["cause_type", "cause"], dropna=False).size().reset_index(name="count").sort_values("count", ascending=False).head(TOP_N_CAUSES)
    lab = top["cause_type"].astype(str) + " | " + top["cause"].astype(str)
    plt.figure(figsize=(12, max(4, TOP_N_CAUSES * 0.35)))
    plt.barh(lab.iloc[::-1], top["count"].iloc[::-1])
    plt.title(f"OHLCV Daily Audit - Top {TOP_N_CAUSES} causes (current)")
    plt.xlabel("Count")
    plt.tight_layout()
    p = OUT_DIR / "03_top_causes_current.png"
    plt.savefig(p, dpi=140)
    plt.close()
    print("saved:", p)
    print(top.to_string(index=False))
else:
    print("No causes found in current.")

# 4) Cobertura por year_path (si existe columna)
if "year_path" in cur.columns and not cur.empty:
    yc = cur.groupby(["year_path", "severity"], dropna=False).size().reset_index(name="count")
    piv = yc.pivot(index="year_path", columns="severity", values="count").fillna(0).sort_index()
    plt.figure(figsize=(12, 5))
    bottom = None
    for c in piv.columns:
        vals = piv[c].values
        plt.bar(piv.index.astype(str), vals, bottom=bottom, label=str(c))
        bottom = vals if bottom is None else bottom + vals
    plt.title("OHLCV Daily Audit - Files by year_path and severity")
    plt.xlabel("Year")
    plt.ylabel("Files")
    plt.xticks(rotation=90)
    plt.legend()
    plt.tight_layout()
    p = OUT_DIR / "04_year_severity_stacked.png"
    plt.savefig(p, dpi=140)
    plt.close()
    print("saved:", p)

# 5) Snapshot de live_status
if LIVE_STATUS_JSON.exists():
    live = json.loads(LIVE_STATUS_JSON.read_text(encoding="utf-8"))
    print("\nLive status snapshot:")
    for k in ["all_files_total", "all_files_scanned", "discovery_complete", "candidates", "batch_files", "events_current_rows", "retry_pending_files"]:
        if k in live:
            print(f"- {k}: {live[k]}")

print("\nPlots dir:", OUT_DIR)
