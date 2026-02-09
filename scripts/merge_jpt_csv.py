# scripts/merge_jpt_csv.py
from __future__ import annotations

from pathlib import Path
import csv
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MASTER = ROOT / "jpt_scraper" / "data" / "jpt.csv"
NEW = ROOT / "jpt_scraper" / "data" / "jpt_new.csv"
OUT = MASTER

EXPECTED_COLS = ["url", "title", "excerpt", "published_date", "topics", "tags", "scraped_at"]

# Your dataset should be ~8k+ unique URLs and go back to 2012.
MIN_UNIQUE_URLS = 7000
MAX_ALLOWED_MIN_DATE = pd.Timestamp("2014-01-01")  # anything newer is suspicious


def _read_strict(p: Path, sep: str) -> pd.DataFrame:
    """
    Read CSV/TSV robustly. We avoid Sniffer because it can mis-detect delimiters on text-heavy data.
    """
    if not p.exists():
        return pd.DataFrame()

    return pd.read_csv(
        p,
        sep=sep,
        dtype=str,
        keep_default_na=False,
        engine="python",          # handles multiline quoted fields reliably
        quotechar='"',
        doublequote=True,
        escapechar="\\",
    )


def _read_master_or_new(p: Path) -> pd.DataFrame:
    """
    Prefer comma-CSV. If it doesn't look right, fallback to TSV.
    """
    # Try comma CSV first (your file is comma CSV)
    df = _read_strict(p, sep=",")

    # If the parse doesn't produce a URL column, try TSV
    if not df.empty:
        cols = [c.strip().lstrip("\ufeff") for c in df.columns]
        if "url" in cols:
            df.columns = cols
            return df

    # Fallback: TSV
    df2 = _read_strict(p, sep="\t")
    if not df2.empty:
        df2.columns = [c.strip().lstrip("\ufeff") for c in df2.columns]
    return df2


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]

    # Map URL header variants to "url"
    lower_map = {c.lower(): c for c in df.columns}
    for candidate in ["url", "link", "permalink", "href"]:
        if "url" not in df.columns and candidate in lower_map:
            df = df.rename(columns={lower_map[candidate]: "url"})
            break

    # Ensure expected columns exist
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = ""

    # Clean strings
    for c in EXPECTED_COLS:
        df[c] = df[c].astype(str).str.strip()

    # Keep only rows with URL
    df = df[df["url"] != ""].copy()
    return df


def _master_guard(old: pd.DataFrame) -> None:
    """
    Refuse to overwrite MASTER if it looks truncated/mis-parsed.
    """
    if old.empty:
        raise RuntimeError("ABORT: master parsed empty. Not overwriting.")

    unique_urls = old["url"].nunique()

    dt = pd.to_datetime(old["published_date"], errors="coerce")
    min_dt = dt.min() if dt.notna().any() else pd.NaT

    if unique_urls < MIN_UNIQUE_URLS:
        raise RuntimeError(
            f"ABORT: master looks wrong (too few unique URLs: {unique_urls} < {MIN_UNIQUE_URLS}). Not overwriting."
        )

    if pd.notna(min_dt) and min_dt > MAX_ALLOWED_MIN_DATE:
        raise RuntimeError(
            f"ABORT: master history too recent (min published_date {min_dt.date()} > {MAX_ALLOWED_MIN_DATE.date()}). Not overwriting."
        )


def main() -> None:
    old_raw = _read_master_or_new(MASTER)
    new_raw = _read_master_or_new(NEW)

    old = _normalize(old_raw)
    new = _normalize(new_raw)

    if old.empty and new.empty:
        print("Both master and new are empty. Nothing to do.")
        return

    # Guard: protect the full-history dataset from being overwritten by a bad parse/truncation
    if MASTER.exists() and not old_raw.empty:
        _master_guard(old)

    combined = pd.concat([old, new], ignore_index=True)

    # De-dupe by URL (keep newest scraped_at)
    combined["_scraped"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
    combined = combined.sort_values(["url", "_scraped"], ascending=[True, True])
    combined = combined.drop_duplicates(subset=["url"], keep="last").drop(columns=["_scraped"])

    # Sort newest first by published_date
    combined["_pub"] = pd.to_datetime(combined["published_date"], errors="coerce")
    combined = combined.sort_values("_pub", ascending=False, na_position="last").drop(columns=["_pub"])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUT, index=False, encoding="utf-8")

    print(f"Merged OK: old={len(old)} new={len(new)} => out={len(combined)}")
    print(f"Unique URLs: old={old['url'].nunique() if not old.empty else 0} new={new['url'].nunique() if not new.empty else 0} out={combined['url'].nunique()}")
    print(f"Wrote: {OUT}")


if __name__ == "__main__":
    main()
