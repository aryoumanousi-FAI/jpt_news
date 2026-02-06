# Scrapy settings for jpt_scraper project
# Docs: https://docs.scrapy.org/en/latest/topics/settings.html

from __future__ import annotations

import os
from pathlib import Path

BOT_NAME = "jpt_scraper"

SPIDER_MODULES = ["jpt_scraper.spiders"]
NEWSPIDER_MODULE = "jpt_scraper.spiders"

# --- Paths ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent  # .../jpt_scraper
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# --- Politeness / identity ---
ROBOTSTXT_OBEY = True
USER_AGENT = "Mozilla/5.0 (compatible; JPTNewsBot/1.0; +https://jpt.spe.org)"

# --- Concurrency / throttling ---
# Keep this reasonable so you don't hammer the site
CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 8
DOWNLOAD_DELAY = 0.35

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 0.5
AUTOTHROTTLE_MAX_DELAY = 10.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0
AUTOTHROTTLE_DEBUG = False

# --- Reliability ---
RETRY_ENABLED = True
RETRY_TIMES = 3
DOWNLOAD_TIMEOUT = 30

# --- Output: CSV feed export ---
# Writes items to data/jpt.csv
FEEDS = {
    str(DATA_DIR / "jpt.csv"): {
        "format": "csv",
        "encoding": "utf-8",
        "overwrite": True,      # set False if you want append behavior
        "store_empty": False,
    }
}
FEED_EXPORT_ENCODING = "utf-8"

# --- Pipelines ---
# No DB pipeline when using FEEDS
ITEM_PIPELINES = {}

# --- Logging ---
LOG_LEVEL = os.getenv("SCRAPY_LOG_LEVEL", "INFO")

# --- Future-proof / required by newer Scrapy ---
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
