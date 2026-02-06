from __future__ import annotations

import os
from pathlib import Path
import pandas as pd
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = ROOT / "jpt_scraper" / "data" / "jpt.csv"
TMP_PATH = ROOT / "jpt_scraper" / "data" / "jpt_new.csv"

def main() -> int:
    ROOT.chdir = os.chdir(str(ROOT))

    # Ensure CSV exists
    if not CSV_PATH.exists():
        print(f"ERROR: Missing {CSV_PATH}. Create the master file first.")
        return 2

    # Get latest date in existing CSV
    old = pd.read_csv(CSV_PATH)
    if "published_date" not in old.columns or old.empty:
        last_date = None
    else:
        old["published_date"] = pd.to_datetime(old["published_date"], errors="coerce")
        last_date = old["published_date"].max()
        last_date = None if pd.isna(last_date) else last_date.date().isoformat()

    print(f"Last date in CSV: {last_date}")

    # Run spider with a small page limit (fast daily run)
    # You can bump max_pages if you want extra safety.
    cmd = [
        sys.executable, "-m", "scrapy", "crawl", "jpt_latest",
        "-a", "max_pages=3",
        "-a", "refresh_existing=0",
        "-O", str(TMP_PATH),
    ]
    print("Running:", " ".join(cmd))
    subprocess.check_call(cmd, cwd=str(ROOT / "jpt_scraper"))

    if not TMP_PATH.exists():
        print("No temp CSV produced.")
        return 3

    new = pd.read_csv(TMP_PATH)

    # Combine + dedupe by url
    combined = pd.concat([old, new], ignore_index=True)

    if "url" in combined.columns:
        combined["url"] = combined["url"].astype(str)
        combined = combined.drop_duplicates(subset=["url"], keep="last")

    # Sort newest first
    if "published_date" in combined.columns:
        combined["published_date"] = pd.to_datetime(combined["published_date"], errors="coerce")
        combined = combined.sort_values("published_date", ascending=False, na_position="last")
        combined["published_date"] = combined["published_date"].dt.date.astype(str)

    combined.to_csv(CSV_PATH, index=False)
    print(f"Updated: {CSV_PATH} ({len(combined)} rows)")

    # Cleanup
    try:
        TMP_PATH.unlink(missing_ok=True)
    except Exception:
        pass

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
