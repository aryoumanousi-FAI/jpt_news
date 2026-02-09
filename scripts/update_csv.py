from __future__ import annotations

import os
import subprocess
from pathlib import Path
import pandas as pd

# --- PATH CONFIGURATION ---
# Script location:  repo_root/scripts/update_csv.py
# SCRIPTS_DIR:      repo_root/scripts
# REPO_ROOT:        repo_root/
SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent

# The folder containing scrapy.cfg
SCRAPY_ROOT = REPO_ROOT / "jpt_scraper"

# The folder containing the CSVs (inside jpt_scraper based on your structure)
DATA_DIR = SCRAPY_ROOT / "data"
CSV_PATH = DATA_DIR / "jpt.csv"
TMP_OUT = DATA_DIR / "_new.csv"

# --- SPIDER CONFIGURATION ---
# Must match the 'name' in your spider class (JptLatestSpider)
SPIDER_NAME = os.getenv("SPIDER_NAME", "jpt_latest")
# Must match the argument in your spider's __init__
MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))


def run_scrape() -> None:
    """
    Runs the Scrapy spider from the SCRAPY_ROOT directory.
    """
    if TMP_OUT.exists():
        TMP_OUT.unlink()

    print(f"--- Starting Scrape ---")
    print(f"CWD (Scrapy Root): {SCRAPY_ROOT}")
    print(f"Spider: {SPIDER_NAME}")
    print(f"Output: {TMP_OUT}")

    # Build command
    cmd = [
        "scrapy", "crawl", SPIDER_NAME,
        "-a", f"max_pages={MAX_PAGES}",
        "-a", "stop_at_last_date=1",
        "-a", f"csv_path={CSV_PATH}",
        "-O", str(TMP_OUT),
    ]

    # IMPORTANT: Run inside the jpt_scraper folder so it finds scrapy.cfg
    subprocess.run(cmd, cwd=str(SCRAPY_ROOT), check=True)


def merge_dedupe() -> int:
    print(f"\n--- Starting Merge ---")
    
    # Ensure data directory exists
    if not DATA_DIR.exists():
        DATA_DIR.mkdir(parents=True)

    if not CSV_PATH.exists():
        # If master doesn't exist, try to use the new scan as master
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

    # Read Files
    old_df = pd.read_csv(CSV_PATH)
    new_df = pd.read_csv(TMP_OUT)

    if "url" not in new_df.columns:
        raise ValueError("New scrape output must include a 'url' column.")

    # Combine
    combined = pd.concat([old_df, new_df], ignore_index=True)

    # Clean & Sort
    if "scraped_at" in combined.columns:
        combined["scraped_at"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
        combined = combined.sort_values(by="scraped_at")

    # Deduplicate (Keep newest by scraped_at)
    combined = combined.drop_duplicates(subset=["url"], keep="last")

    # Final Sort (by published date)
    if "published_date" in combined.columns:
        combined["published_date"] = pd.to_datetime(combined["published_date"], errors="coerce")
        combined = combined.sort_values(by=["published_date", "scraped_at"], ascending=[False, False])

    # Save
    combined.to_csv(CSV_PATH, index=False)

    added = len(combined) - len(old_df)
    return max(added, 0)


def main() -> None:
    # Run the scrape
    try:
        run_scrape()
    except subprocess.CalledProcessError as e:
        print(f"Scrapy failed with error code {e.returncode}. Check logs.")
        exit(1)

    # Run the merge
    added = merge_dedupe()
    print(f"Done. Added {added} new rows.")


if __name__ == "__main__":
    main()
