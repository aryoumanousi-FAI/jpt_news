from __future__ import annotations

import os
import subprocess
from pathlib import Path
import pandas as pd

# --- PATH CONFIGURATION ---
# Script:  repo_root/scripts/update_csv.py
# Parent:  repo_root/scripts
# Root:    repo_root
SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent

# Define where the spider and data live
# Structure: repo_root/jpt_scraper/data/jpt.csv
SCRAPY_ROOT = REPO_ROOT / "jpt_scraper"
DATA_DIR = SCRAPY_ROOT / "data"
CSV_PATH = DATA_DIR / "jpt.csv"
TMP_OUT = DATA_DIR / "_new.csv"

# Config
SPIDER_NAME = os.getenv("SPIDER_NAME", "jpt_latest")
MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))


def run_scrape() -> None:
    """Runs Scrapy and writes to _new.csv"""
    if TMP_OUT.exists():
        TMP_OUT.unlink()

    print(f"--- Starting Scrape ---")
    print(f"Spider: {SPIDER_NAME}")
    print(f"Master CSV Path: {CSV_PATH}")
    
    # Check if master exists before we start, just for logging
    if CSV_PATH.exists():
        print(f"  -> Found master CSV at {CSV_PATH}")
    else:
        print(f"  -> WARNING: Master CSV NOT found at {CSV_PATH}. This will be a fresh file if not fixed.")

    cmd = [
        "scrapy", "crawl", SPIDER_NAME,
        "-a", f"max_pages={MAX_PAGES}",
        "-a", "stop_at_last_date=1",
        "-a", f"csv_path={CSV_PATH}",
        "-O", str(TMP_OUT),
    ]

    # Run inside jpt_scraper folder so scrapy.cfg is found
    subprocess.run(cmd, cwd=str(SCRAPY_ROOT), check=True)


def merge_dedupe() -> int:
    print(f"\n--- Starting Merge ---")
    
    if not DATA_DIR.exists():
        DATA_DIR.mkdir(parents=True)

    # 1. Load New Data
    if not TMP_OUT.exists():
        print("No new scrape output found. Exiting.")
        return 0
    
    new_df = pd.read_csv(TMP_OUT)
    print(f"New rows scraped: {len(new_df)}")

    if "url" not in new_df.columns:
        raise ValueError("New scrape output missing 'url' column.")

    # 2. Load Old Data
    if CSV_PATH.exists():
        old_df = pd.read_csv(CSV_PATH)
        print(f"Existing rows loaded: {len(old_df)}")
    else:
        print("Master CSV not found. Starting fresh.")
        old_df = pd.DataFrame()

    # 3. Combine
    combined = pd.concat([old_df, new_df], ignore_index=True)
    total_before_dedupe = len(combined)

    # 4. Clean & Sort
    if "scraped_at" in combined.columns:
        combined["scraped_at"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
        combined = combined.sort_values(by="scraped_at")

    # 5. Deduplicate (Keep newest version of the URL)
    combined = combined.drop_duplicates(subset=["url"], keep="last")

    # Final Sort
    if "published_date" in combined.columns:
        combined["published_date"] = pd.to_datetime(combined["published_date"], errors="coerce")
        combined = combined.sort_values(by=["published_date", "scraped_at"], ascending=[False, False])

    # 6. Save
    combined.to_csv(CSV_PATH, index=False)
    
    final_count = len(combined)
    added = final_count - len(old_df)
    print(f"Merge Complete. Final Total: {final_count} (Added: {added})")
    
    return max(added, 0)


def main() -> None:
    try:
        run_scrape()
        merge_dedupe()
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        exit(1)


if __name__ == "__main__":
    main()
