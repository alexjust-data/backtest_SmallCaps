from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import polars as pl

REQUIRED_COLS = [
    "ticker",
    "cik",
    "event_type",
    "event_date",
    "source_name",
    "source_doc_type",
    "source_url",
    "source_title",
    "notes",
]


def _http_json(url: str, user_agent: str, timeout_sec: int) -> dict:
    req = Request(url, headers={"User-Agent": user_agent, "Accept": "application/json"})
    with urlopen(req, timeout=timeout_sec) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _load_tickers(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Tickers file does not exist: {path}")
    txt = path.read_text(encoding="utf-8").strip()
    if not txt:
        return []
    # Accept one-per-line plain text (including single-line file).
    if "," not in txt and all(ch not in txt for ch in [";", "\t"]):
        return sorted({x.strip().upper() for x in txt.splitlines() if x.strip()})
    df = pl.read_csv(path)
    col = "ticker" if "ticker" in df.columns else df.columns[0]
    return sorted({str(x).strip().upper() for x in df[col].to_list() if str(x).strip()})


def _sec_ticker_cik_map(company_json: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    # SEC company_tickers_exchange.json has {fields:[...],data:[[...],...]}
    if "fields" in company_json and "data" in company_json:
        fields = company_json["fields"]
        idx_t = fields.index("ticker") if "ticker" in fields else None
        idx_c = fields.index("cik") if "cik" in fields else None
        if idx_t is not None and idx_c is not None:
            for row in company_json["data"]:
                try:
                    t = str(row[idx_t]).strip().upper()
                    c = str(row[idx_c]).strip()
                    if t and c:
                        out[t] = c
                except Exception:
                    continue
            return out
    # Fallback: legacy dict style.
    for v in company_json.values():
        try:
            t = str(v.get("ticker", "")).strip().upper()
            c = str(v.get("cik_str", v.get("cik", ""))).strip()
            if t and c:
                out[t] = c
        except Exception:
            continue
    return out


def _build_filing_url(cik: str, accession_number: str, primary_document: str) -> str:
    cik_no_pad = str(int(cik))
    acc = accession_number.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_no_pad}/{acc}/{primary_document}"


def _event_rows_from_submissions(
    ticker: str,
    cik: str,
    submissions: dict,
    require_primary_doc: bool,
) -> list[dict]:
    recent = submissions.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])
    accession_numbers = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])

    n = min(len(forms), len(filing_dates), len(accession_numbers), len(primary_docs))
    rows: list[dict] = []
    listed_candidates: list[tuple[str, str, str]] = []
    delisted_candidates: list[tuple[str, str, str]] = []

    listed_forms = {"8-A12B", "8-A12G", "10-12B"}

    for i in range(n):
        form = str(forms[i]).strip().upper()
        fdate = str(filing_dates[i]).strip()
        acc = str(accession_numbers[i]).strip()
        doc = str(primary_docs[i]).strip()
        if not fdate or not acc:
            continue
        if require_primary_doc and not doc:
            continue
        try:
            url = _build_filing_url(cik, acc, doc if doc else "index.html")
        except Exception:
            continue

        if form in listed_forms:
            listed_candidates.append((fdate, form, url))
        if form.startswith("25"):
            delisted_candidates.append((fdate, form, url))

    # Earliest listing candidate.
    if listed_candidates:
        listed_candidates.sort(key=lambda x: x[0])
        d, form, url = listed_candidates[0]
        rows.append(
            {
                "ticker": ticker,
                "cik": str(cik),
                "event_type": "listed",
                "event_date": d,
                "source_name": "SEC",
                "source_doc_type": form,
                "source_url": url,
                "source_title": f"SEC {form} filing (auto candidate)",
                "notes": "AUTO_CANDIDATE_FROM_SEC_SUBMISSIONS_NOT_FINAL",
            }
        )

    # Latest delist candidate.
    if delisted_candidates:
        delisted_candidates.sort(key=lambda x: x[0])
        d, form, url = delisted_candidates[-1]
        rows.append(
            {
                "ticker": ticker,
                "cik": str(cik),
                "event_type": "delisted",
                "event_date": d,
                "source_name": "SEC",
                "source_doc_type": form,
                "source_url": url,
                "source_title": f"SEC {form} filing (auto candidate)",
                "notes": "AUTO_CANDIDATE_FROM_SEC_SUBMISSIONS_NOT_FINAL",
            }
        )

    return rows


def _normalize_event_df(df: pl.DataFrame) -> pl.DataFrame:
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in events dataframe: {missing}")
    return (
        df.select(REQUIRED_COLS)
        .with_columns(
            [
                pl.col("ticker").cast(pl.Utf8).str.strip_chars().str.to_uppercase(),
                pl.col("cik").cast(pl.Utf8).str.strip_chars(),
                pl.col("event_type").cast(pl.Utf8).str.strip_chars().str.to_lowercase(),
                pl.col("event_date").cast(pl.Utf8),
                pl.col("source_name").cast(pl.Utf8).str.strip_chars(),
                pl.col("source_doc_type").cast(pl.Utf8).str.strip_chars(),
                pl.col("source_url").cast(pl.Utf8).str.strip_chars(),
                pl.col("source_title").cast(pl.Utf8),
                pl.col("notes").cast(pl.Utf8),
            ]
        )
        .unique(
            subset=[
                "ticker",
                "cik",
                "event_type",
                "event_date",
                "source_name",
                "source_doc_type",
                "source_url",
            ],
            keep="first",
        )
        .sort(["ticker", "event_date", "event_type"])
    )


def _write_csv(path: Path, df: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download SEC-based official event candidates and optionally merge into official_ticker_events.csv"
    )
    parser.add_argument("--tickers-file", required=True, help="Path to tickers list (one per line or CSV with ticker col).")
    parser.add_argument("--out-candidates-csv", required=True, help="Output CSV with SEC candidate events.")
    parser.add_argument(
        "--merge-into-official-csv",
        required=False,
        default="",
        help="If provided, append/merge candidates into this official_ticker_events.csv target.",
    )
    parser.add_argument(
        "--user-agent",
        required=True,
        help="SEC requires identifiable User-Agent, e.g. 'YourName your@email.com'.",
    )
    parser.add_argument("--timeout-sec", type=int, default=30)
    parser.add_argument("--sleep-ms", type=int, default=120)
    parser.add_argument("--max-tickers", type=int, default=0, help="Process only first N tickers (0 = all).")
    parser.add_argument(
        "--require-primary-doc",
        action="store_true",
        help="Drop filings without primaryDocument.",
    )
    args = parser.parse_args()

    tickers = _load_tickers(Path(args.tickers_file))
    if args.max_tickers > 0:
        tickers = tickers[: args.max_tickers]
    if not tickers:
        raise ValueError("No tickers provided")

    company_url = "https://www.sec.gov/files/company_tickers_exchange.json"
    try:
        company_json = _http_json(company_url, args.user_agent, args.timeout_sec)
    except Exception as exc:
        raise RuntimeError(
            f"Unable to download SEC company ticker map from {company_url}. "
            "Check network access / firewall and SEC User-Agent policy."
        ) from exc
    t2c = _sec_ticker_cik_map(company_json)

    rows: list[dict] = []
    misses = 0
    errors = 0
    for i, t in enumerate(tickers, start=1):
        cik = t2c.get(t)
        if not cik:
            misses += 1
            continue
        try:
            cik_pad = str(int(cik)).zfill(10)
            sub_url = f"https://data.sec.gov/submissions/CIK{cik_pad}.json"
            sub = _http_json(sub_url, args.user_agent, args.timeout_sec)
            rows.extend(_event_rows_from_submissions(t, cik, sub, args.require_primary_doc))
        except (HTTPError, URLError, TimeoutError):
            errors += 1
        except Exception:
            errors += 1
        if args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)
        if i % 100 == 0:
            print(f"progress {i}/{len(tickers)}")

    cand_df = _normalize_event_df(pl.DataFrame(rows, schema={c: pl.Utf8 for c in REQUIRED_COLS}))
    out_cand = Path(args.out_candidates_csv)
    _write_csv(out_cand, cand_df)

    merged_rows = cand_df.height
    if args.merge_into_official_csv:
        target = Path(args.merge_into_official_csv)
        if target.exists():
            base = _normalize_event_df(pl.read_csv(target))
            merged = _normalize_event_df(pl.concat([base, cand_df], how="vertical_relaxed"))
        else:
            merged = cand_df
        _write_csv(target, merged)
        merged_rows = merged.height
        print(f"Merged official CSV rows: {merged_rows}")

    print(
        json.dumps(
            {
                "tickers_input": len(tickers),
                "ticker_missing_cik": misses,
                "ticker_errors": errors,
                "candidate_events_rows": int(cand_df.height),
                "out_candidates_csv": str(out_cand),
                "merged_official_rows": int(merged_rows) if args.merge_into_official_csv else None,
            },
            indent=2,
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
