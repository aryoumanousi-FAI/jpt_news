from __future__ import annotations

import os
import subprocess
from pathlib import Path
import pandas as pd

# Assumes structure:
# jpt_scraper/           (Repo Root / Scrapy Project Root)
#   scrapy.cfg
#   jpt_scraper/         (Python Package)
#     scripts/           (Where this file is)
#       update_csv.py
#     data/              (Where csvs are)
#       jpt.csv
#
# So parents[1] gets us to 'jpt_scraper' package folder.
# If your data is in the *outer* root, change this to parents[2].
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = PACKAGE_ROOT / "data" / "jpt.csv"
TMP_OUT = PACKAGE_ROOT / "data" / "_new.csv"
PROJECT_ROOT = PACKAGE_ROOT.parent  # The folder containing scrapy.cfg

# Must match the 'name' defined in your spider class
SPIDER_NAME = os.getenv("SPIDER_NAME", "jpt_latest") 
MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))

def run_scrape() -> None:
    """
    Runs the Scrapy spider and outputs to TMP_OUT.
    """
    if TMP_OUT.exists():
        TMP_OUT.unlink()

    print(f"--- Starting Scrape ---")
    print(f"Spider: {SPIDER_NAME}")
    print(f"Output: {TMP_OUT}")
    print(f"Reference CSV: {CSV_PATH}")

    # Build command
    # We pass 'stop_at_last_date=1' and 'csv_path' so the spider 
    # knows when to stop crawling (optimization).
    cmd = [
        "scrapy",
        "crawl",
        SPIDER_NAME,
        "-a", f"max_pages={MAX_PAGES}",
        "-a", "stop_at_last_date=1",
        "-a", f"csv_path={CSV_PATH}",
        "-O", str(TMP_OUT),
    ]

    # Run from PROJECT_ROOT so scrapy.cfg is found
    subprocess.run(cmd, cwd=str(PROJECT_ROOT), check=True)

def merge_dedupe() -> int:
    print(f"\n--- Starting Merge ---")
    
    if not CSV_PATH.exists():
        # If master doesn't exist, just rename new to master
        if TMP_OUT.exists():
            print("Master CSV not found. Renaming new scan to master.")
            TMP_OUT.rename(CSV_PATH)
            return pd.read_csv(CSV_PATH).shape[0]
        else:
            print("No master CSV and no new data. Exiting.")
            return 0

    if not TMP_OUT.exists():
        print("No new scrape output found; nothing to merge.")
        return 0

    old_df = pd.read_csv(CSV_PATH)
    new_df = pd.read_csv(TMP_OUT)

    if "url" not in new_df.columns:
        raise ValueError("New scrape output must include a 'url' column for de-duplication.")

    combined = pd.concat([old_df, new_df], ignore_index=True)

    # Convert dates for sorting
    if "scraped_at" in combined.columns:
        combined["scraped_at"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
        combined = combined.sort_values(by=["scraped_at"], ascending=True)

    # De-dupe by URL (keep newest row if same URL appears)
    combined = combined.drop_duplicates(subset=["url"], keep="last")

    # Sort for readability
    if "published_date" in combined.columns:
        combined["published_date"] = pd.to_datetime(combined["published_date"], errors="coerce")
        combined = combined.sort_values(by=["published_date", "scraped_at"], ascending=[False, False])

    combined.to_csv(CSV_PATH, index=False)

    added = len(combined) - len(old_df)
    return max(added, 0)

def main() -> None:
    run_scrape()
    added = merge_dedupe()
    print(f"Done. Added {added} new rows.")

if __name__ == "__main__":
    main()
