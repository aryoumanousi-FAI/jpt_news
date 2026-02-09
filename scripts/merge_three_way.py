from __future__ import annotations

from pathlib import Path
import pandas as pd


# -------------------
# PATHS
# -------------------
SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent

SCRAPY_ROOT = REPO_ROOT / "jpt_scraper"
DATA_DIR = SCRAPY_ROOT / "data"

MASTER_CSV = DATA_DIR / "jpt_master.csv"   # NEVER touched
DAILY_CSV = DATA_DIR / "jpt_daily.csv"     # Overwritten daily
MERGED_CSV = DATA_DIR / "jpt.csv"           # Rebuilt every run


def load_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        print(f"{label} not found: {path} (using empty)")
        return pd.DataFrame()
    df = pd.read_csv(path)
    print(f"{label} loaded: {len(df)} rows")
    return df


def main() -> None:
    master_df = load_csv(MASTER_CSV, "MASTER")
    daily_df = load_csv(DAILY_CSV, "DAILY")

    if master_df.empty and daily_df.empty:
        raise RuntimeError("Both MASTER and DAILY are empty. Nothing to merge.")

    for name, df in [("MASTER", master_df), ("DAILY", daily_df)]:
        if not df.empty and "url" not in df.columns:
            raise ValueError(f"{name} CSV missing required 'url' column.")

    # Combine: MASTER first, DAILY second
    combined = pd.concat([master_df, daily_df], ignore_index=True)

    # Normalize datetimes if present
    if "scraped_at" in combined.columns:
        combined["scraped_at"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
        combined = combined.sort_values("scraped_at", ascending=True, kind="mergesort")

    # Deduplicate so DAILY wins on conflicts
    merged = combined.drop_duplicates(subset=["url"], keep="last")

    # Optional final ordering
    if "published_date" in merged.columns:
        merged["published_date"] = pd.to_datetime(merged["published_date"], errors="coerce")
        sort_cols = ["published_date"] + (["scraped_at"] if "scraped_at" in merged.columns else [])
        asc = [False] + ([False] if "scraped_at" in merged.columns else [])
        merged = merged.sort_values(sort_cols, ascending=asc, kind="mergesort")
    elif "scraped_at" in merged.columns:
        merged = merged.sort_values("scraped_at", ascending=False, kind="mergesort")

    # Atomic write (merged only)
    tmp = MERGED_CSV.with_suffix(".csv.tmp")
    merged.to_csv(tmp, index=False)
    tmp.replace(MERGED_CSV)

    print(f"Merged written: {MERGED_CSV}")
    print(f"Rows: {len(merged)} | Unique URLs: {merged['url'].nunique()}")


if __name__ == "__main__":
    main()
