from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
import pandas as pd

# --- PATH CONFIGURATION ---
SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent

# Based on your structure:
# repo/jpt_scraper/data/jpt.csv
SCRAPY_ROOT = REPO_ROOT / "jpt_scraper"
DATA_DIR = SCRAPY_ROOT / "data"
CSV_PATH = DATA_DIR / "jpt.csv"
TMP_OUT = DATA_DIR / "_new.csv"

SPIDER_NAME = os.getenv("SPIDER_NAME", "jpt_latest")
MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))

def check_paths():
    """Debugs paths and ensures Master CSV exists."""
    print(f"--- Path Check ---")
    print(f"Repo Root:  {REPO_ROOT}")
    print(f"Data Dir:   {DATA_DIR}")
    print(f"Master CSV: {CSV_PATH}")

    if not DATA_DIR.exists():
        print(f"ERROR: Data directory not found at {DATA_DIR}")
        print(f"Contents of {SCRAPY_ROOT}:")
        if SCRAPY_ROOT.exists():
            print(os.listdir(SCRAPY_ROOT))
        else:
            print("  (Scrapy Root not found)")
        sys.exit(1)

    if not CSV_PATH.exists():
        print(f"CRITICAL ERROR: Master CSV not found at {CSV_PATH}")
        print(f"Contents of {DATA_DIR}:")
        print(os.listdir(DATA_DIR))
        print("\n!!! ABORTING TO PREVENT OVERWRITE !!!")
        print("Please check if jpt.csv is in your git repo or if it is ignored by .gitignore")
        sys.exit(1) # <--- THIS STOPS THE OVERWRITE
    
    print("SUCCESS: Master CSV found.")

def run_scrape() -> None:
    if TMP_OUT.exists():
        TMP_OUT.unlink()

    print(f"\n--- Starting Scrape ---")
    cmd = [
        "scrapy", "crawl", SPIDER_NAME,
        "-a", f"max_pages={MAX_PAGES}",
        "-a", "stop_at_last_date=1",
        "-a", f"csv_path={CSV_PATH}",
        "-O", str(TMP_OUT),
    ]
    subprocess.run(cmd, cwd=str(SCRAPY_ROOT), check=True)

def merge_dedupe() -> int:
    print(f"\n--- Starting Merge ---")
    
    # 1. Read New Data
    if not TMP_OUT.exists():
        print("No new scrape output found.")
        return 0
    new_df = pd.read_csv(TMP_OUT)
    print(f"New rows: {len(new_df)}")

    # 2. Read Old Data (Guaranteed to exist by check_paths)
    old_df = pd.read_csv(CSV_PATH)
    print(f"Old rows: {len(old_df)}")

    # 3. Combine
    combined = pd.concat([old_df, new_df], ignore_index=True)
    
    # 4. Clean & Sort
    if "scraped_at" in combined.columns:
        combined["scraped_at"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
        combined = combined.sort_values(by="scraped_at")

    # 5. Deduplicate
    combined = combined.drop_duplicates(subset=["url"], keep="last")

    # Final Sort
    if "published_date" in combined.columns:
        combined["published_date"] = pd.to_datetime(combined["published_date"], errors="coerce")
        combined = combined.sort_values(by=["published_date", "scraped_at"], ascending=[False, False])

    # 6. Save
    combined.to_csv(CSV_PATH, index=False)
    
    added = len(combined) - len(old_df)
    print(f"Merged. Final count: {len(combined)} (Added: {added})")
    return max(added, 0)

def main() -> None:
    check_paths() # <--- SAFETY FIRST
    run_scrape()
    merge_dedupe()

if __name__ == "__main__":
    main()
