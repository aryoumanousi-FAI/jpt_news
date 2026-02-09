from __future__ import annotations

from pathlib import Path
import csv
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MASTER = ROOT / "jpt_scraper" / "data" / "jpt.csv"
NEW = ROOT / "jpt_scraper" / "data" / "jpt_new.csv"
OUT = MASTER

EXPECTED_COLS = ["url", "title", "excerpt", "published_date", "topics", "tags", "scraped_at"]

# Your full history is ~8k URLs and goes back to ~2012
MIN_UNIQUE_URLS = 5000
MAX_ALLOWED_MIN_DATE = pd.Timestamp("2014-01-01")  # master should start earlier than this


def _read_with_csv_module(path: Path) -> list[dict]:
    """
    Multiline-safe CSV reader using Python stdlib csv module.
    This avoids pandas.read_csv quirks with messy quoting/multiline fields.
    """
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return []

        # Normalize header names (strip BOM/whitespace)
        fieldnames = [h.strip().lstrip("\ufeff") for h in reader.fieldnames]
        reader.fieldnames = fieldnames

        rows: list[dict] = []
        for r in reader:
            # r keys will match reader.fieldnames
            rows.append(r)

    return rows


def _records_to_df(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=EXPECTED_COLS)

    df = pd.DataFrame(rows)

    # Normalize headers
    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]

    # Ensure expected columns exist
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = ""

    # Clean strings
    for c in EXPECTED_COLS:
        df[c] = df[c].astype(str).str.strip()

    # Drop rows without URL
    df = df[df["url"] != ""].copy()
    return df


def _guard_master(df_master: pd.DataFrame) -> None:
    if df_master.empty:
        raise RuntimeError("ABORT: master parsed empty. Not overwriting.")

    unique_urls = df_master["url"].nunique()

    dt = pd.to_datetime(df_master["published_date"], errors="coerce")
    min_dt = dt.min() if dt.notna().any() else pd.NaT

    if unique_urls < MIN_UNIQUE_URLS:
        raise RuntimeError(
            f"ABORT: master too small (unique URLs {unique_urls} < {MIN_UNIQUE_URLS}). Not overwriting."
        )

    if pd.notna(min_dt) and min_dt > MAX_ALLOWED_MIN_DATE:
        raise RuntimeError(
            f"ABORT: master looks recent-only (min published_date {min_dt.date()} > {MAX_ALLOWED_MIN_DATE.date()}). Not overwriting."
        )


def main() -> None:
    # Read using csv module (multiline safe)
    old_rows = _read_with_csv_module(MASTER)
    new_rows = _read_with_csv_module(NEW)

    old = _records_to_df(old_rows)
    new = _records_to_df(new_rows)

    # Helpful minimal diagnostics (won't spam)
    print(f"MASTER parsed rows: {len(old)} | unique URLs: {old['url'].nunique() if not old.empty else 0}")
    print(f"NEW parsed rows:    {len(new)} | unique URLs: {new['url'].nunique() if not new.empty else 0}")

    if old.empty and new.empty:
        print("Both master and new are empty. Nothing to do.")
        return

    if MASTER.exists():
        _guard_master(old)

    combined = pd.concat([old, new], ignore_index=True)

    # De-dupe by URL (keep newest scraped_at)
    combined["_scraped"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
    combined = combined.sort_values(["url", "_scraped"], ascending=[True, True])
    combined = combined.drop_duplicates(subset=["url"], keep="last").drop(columns=["_scraped"])

    # Sort newest first by published_date
    combined["_pub"] = pd.to_datetime(combined["published_date"], errors="coerce")
    combined = combined.sort_values("_pub", ascending=False, na_position="last").drop(columns=["_pub"])

    OUT.parent.mkdir(parents=True, exist_ok=True)

    # Write consistently; csv.QUOTE_MINIMAL keeps file sane
    combined.to_csv(
        OUT,
        index=False,
        encoding="utf-8",
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\n",
    )

    print(f"Merged OK: old={len(old)} new={len(new)} => out={len(combined)}")
    print(f"Unique URLs out: {combined['url'].nunique()}")
    print(f"Wrote: {OUT}")


if __name__ == "__main__":
    main()
