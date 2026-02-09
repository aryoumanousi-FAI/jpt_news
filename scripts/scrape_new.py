from __future__ import annotations

import os
import subprocess
from pathlib import Path


# --- PATHS ---
SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent

SCRAPY_ROOT = REPO_ROOT / "jpt_scraper"
DATA_DIR = SCRAPY_ROOT / "data"
NEW_CSV = DATA_DIR / "_new.csv"

# --- CONFIG ---
SPIDER_NAME = os.getenv("SPIDER_NAME", "jpt_latest")
MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if NEW_CSV.exists():
        NEW_CSV.unlink()

    print("--- Scrape step ---")
    print(f"SCRAPY_ROOT: {SCRAPY_ROOT}")
    print(f"Spider:      {SPIDER_NAME}")
    print(f"MAX_PAGES:   {MAX_PAGES}")
    print(f"Output:      {NEW_CSV}")

    cmd = [
        "scrapy",
        "crawl",
        SPIDER_NAME,
        "-a",
        f"max_pages={MAX_PAGES}",
        "-O",
        str(NEW_CSV),
    ]

    subprocess.run(cmd, cwd=str(SCRAPY_ROOT), check=True)


if __name__ == "__main__":
    main()
