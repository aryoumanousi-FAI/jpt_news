from __future__ import annotations

from pathlib import Path
import pandas as pd

# This points to 'jpt_scraper/' (the folder containing 'scripts/')
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Adjusted paths: assumed to be in 'jpt_scraper/data/'
MASTER = PROJECT_ROOT / "data" / "jpt.csv"
NEW = PROJECT_ROOT / "data" / "_new.csv"

def main() -> None:
    if not MASTER.exists():
        # Fallback: if master doesn't exist, try to find it relative to current working dir
        # This helps if running from different contexts
        print(f"Master CSV not found at {MASTER}. Checking current directory...")
        if Path("data/jpt.csv").exists():
             global MASTER
             MASTER = Path("data/jpt.csv")
        else:
             raise FileNotFoundError(f"Master CSV not found: {MASTER}")

    if not NEW.exists():
        print("No _new.csv found. Nothing to merge.")
        return

    print(f"Loading master from: {MASTER}")
    old_df = pd.read_csv(MASTER)
    new_df = pd.read_csv(NEW)

    if "url" not in new_df.columns:
        raise ValueError("New scrape output must include a 'url' column for deduping.")

    # Combine
    combined = pd.concat([old_df, new_df], ignore_index=True)

    # 1. Convert scraped_at to datetime to sort correctly
    if "scraped_at" in combined.columns:
        combined["scraped_at"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
        combined = combined.sort_values("scraped_at")

    # 2. De-dupe by URL, keeping the last (newest) entry
    combined = combined.drop_duplicates(subset=["url"], keep="last")

    # 3. Sort by published date for tidiness
    if "published_date" in combined.columns:
        combined["published_date"] = pd.to_datetime(combined["published_date"], errors="coerce")
        combined = combined.sort_values(["published_date", "scraped_at"], ascending=[False, False])

    combined.to_csv(MASTER, index=False)
    print(f"Merged. Old={len(old_df)} NewScrape={len(new_df)} Final={len(combined)}")

if __name__ == "__main__":
    main()
