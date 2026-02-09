from __future__ import annotations

from pathlib import Path
import pandas as pd


# --- PATHS ---
SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent

SCRAPY_ROOT = REPO_ROOT / "jpt_scraper"
DATA_DIR = SCRAPY_ROOT / "data"

MASTER_CSV = DATA_DIR / "jpt_master.csv"   # NEVER TOUCH
DAILY_CSV = DATA_DIR / "jpt_daily.csv"     # overwritten every run
MERGED_CSV = DATA_DIR / "jpt.csv"          # regenerated every run (your app can read this)

# --- BEHAVIOR ---
# Keep one row per URL in MERGED_CSV.
# If a URL exists in both master and daily, daily wins (recommended).
DAILY_WINS_ON_DUPLICATE = True


def _load_csv(path: Path, name: str) -> pd.DataFrame:
    if not path.exists():
        print(f"{name} not found at {path}. Using empty DataFrame.")
        return pd.DataFrame()
    df = pd.read_csv(path)
    print(f"{name} loaded: {len(df)} rows from {path}")
    return df


def main() -> None:
    master_df = _load_csv(MASTER_CSV, "MASTER")
    daily_df = _load_csv(DAILY_CSV, "DAILY")

    if daily_df.empty and master_df.empty:
        raise RuntimeError("Both MASTER and DAILY are empty/missing. Nothing to merge.")

    # Validate required column if data exists
    for label, df in [("MASTER", master_df), ("DAILY", daily_df)]:
        if not df.empty and "url" not in df.columns:
            raise ValueError(f"{label} CSV missing required 'url' column.")

    combined = pd.concat([master_df, daily_df], ignore_index=True)

    # Parse datetimes if present (helps stable ordering)
    if "scraped_at" in combined.columns:
        combined["scraped_at"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
    if "published_date" in combined.columns:
        combined["published_date"] = pd.to_datetime(combined["published_date"], errors="coerce")

    # Decide which row to keep when URL duplicates exist
    # We sort so the "winner" ends up last (then keep="last")
    if "scraped_at" in combined.columns:
        combined = combined.sort_values("scraped_at", ascending=True, kind="mergesort")

    keep_rule = "last" if DAILY_WINS_ON_DUPLICATE else "first"
    # IMPORTANT: concat order is [master, daily]
    # - keep="last" => daily wins
    # - keep="first" => master wins
    merged = combined.drop_duplicates(subset=["url"], keep=keep_rule)

    # Nice final ordering
    if "published_date" in merged.columns:
        sort_cols = ["published_date"] + (["scraped_at"] if "scraped_at" in merged.columns else [])
        asc = [False] + ([False] if "scraped_at" in merged.columns else [])
        merged = merged.sort_values(sort_cols, ascending=asc, kind="mergesort")
    elif "scraped_at" in merged.columns:
        merged = merged.sort_values("scraped_at", ascending=False, kind="mergesort")

    # Atomic write of merged only (MASTER is never written)
    tmp_out = MERGED_CSV.with_suffix(".csv.tmp")
    merged.to_csv(tmp_out, index=False)
    tmp_out.replace(MERGED_CSV)

    print(f"Merged written: {MERGED_CSV}")
    print(f"Merged rows: {len(merged)} | unique urls: {merged['url'].nunique() if 'url' in merged.columns else 'n/a'}")


if __name__ == "__main__":
    main()
