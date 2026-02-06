from __future__ import annotations

import csv
import re
from datetime import datetime, timezone
from pathlib import Path

import scrapy
from dateutil import parser as dateparser

from jpt_scraper.items import JptScraperItem

BASE = "https://jpt.spe.org"
START_URL = f"{BASE}/latest-news"

MONTH_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b"
)


def parse_date_from_text(text: str) -> str | None:
    """Extracts 'Month D, YYYY' from text and returns 'YYYY-MM-DD'."""
    m = MONTH_DATE_RE.search(text or "")
    if not m:
        return None
    try:
        dt = dateparser.parse(m.group(0))
        return dt.date().isoformat()
    except Exception:
        return None


def clean_list(xs) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for x in xs or []:
        x = " ".join(str(x).split()).strip()
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out


def read_last_date_from_csv(csv_path: str | None) -> str | None:
    """
    Reads max(published_date) from existing master CSV.
    Assumes published_date is YYYY-MM-DD strings.
    """
    if not csv_path:
        return None

    path = Path(csv_path)
    if not path.exists():
        return None

    try:
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            dates = [r.get("published_date") for r in reader if r.get("published_date")]
        return max(dates) if dates else None
    except Exception:
        return None


class JptLatestSpider(scrapy.Spider):
    name = "jpt_latest"
    allowed_domains = ["jpt.spe.org"]
    start_urls = [START_URL]

    custom_settings = {
        "CONCURRENT_REQUESTS": 12,
        "AUTOTHROTTLE_ENABLED": True,
        "DOWNLOAD_DELAY": 0.3,
        "ROBOTSTXT_OBEY": True,
        "FEED_EXPORT_ENCODING": "utf-8",
        "LOG_LEVEL": "INFO",
    }

    def __init__(
        self,
        max_pages: int = 0,
        refresh_existing: int = 0,
        stop_at_last_date: int = 0,
        csv_path: str | None = None,
        *args,
        **kwargs,
    ):
        """
        max_pages:
          - 0 means "no limit" (crawl until stop condition / no Next link)
          - otherwise crawl at most this many listing pages

        refresh_existing:
          - 0 = normal
          - 1 = force refresh (if you later add upsert logic)

        stop_at_last_date:
          - 0 = off
          - 1 = stop paging when listing reaches articles <= last_date in csv_path

        csv_path:
          - path to master CSV (used to compute last_date)
        """
        super().__init__(*args, **kwargs)
        self.max_pages = int(max_pages)
        self.refresh_existing = int(refresh_existing)
        self.pages_seen = 0

        self.stop_at_last_date = int(stop_at_last_date)
        self.last_date = read_last_date_from_csv(csv_path) if self.stop_at_last_date else None

        if self.last_date:
            self.logger.info(f"Hard stop enabled. Last date in CSV: {self.last_date}")

    def parse(self, response: scrapy.http.Response):
        self.pages_seen += 1

        for promo in response.css("div.PromoB"):
            href = promo.css("div.PromoB-title a::attr(href)").get()
            if not href:
                continue
            url = response.urljoin(href)

            title = " ".join((promo.css("div.PromoB-title a::text").get() or "").split())
            excerpt = " ".join((promo.css("div.PromoB-description::text").get() or "").split())

            byline_text = " ".join(
                promo.css("div.PromoB-by-line::text, div.PromoB-by-line *::text").getall()
            ).strip()
            published_date = parse_date_from_text(byline_text)
            if not published_date:
                continue

            # HARD STOP: if we reached already-known dates, stop crawling further pages
            if self.last_date and published_date <= self.last_date:
                self.logger.info(
                    f"Reached existing date {published_date} <= {self.last_date}. Stopping."
                )
                return

            yield response.follow(
                url,
                callback=self.parse_article,
                meta={
                    "url": url,
                    "title": title,
                    "excerpt": excerpt,
                    "published_date": published_date,
                },
            )

        # Next page
        if self.max_pages == 0 or self.pages_seen < self.max_pages:
            next_href = response.css("div.ListE-nextPage a[rel='next']::attr(href)").get()
            if next_href:
                yield response.follow(next_href, callback=self.parse)

    def parse_article(self, response: scrapy.http.Response):
        url = response.meta["url"]
        title = response.meta.get("title") or ""
        excerpt = response.meta.get("excerpt") or ""
        published_date = response.meta["published_date"]

        container = response.css("div.ArticlePage-tags-container")

        topics = container.xpath(
            ".//div[contains(@class,'ArticlePage-tags')][.//h2[contains(.,'Topics')]]"
            "//div[contains(@class,'ArticlePage-tags-list')]//a[contains(@href,'/topic/')]/text()"
        ).getall()

        tags = container.xpath(
            ".//div[contains(@class,'ArticlePage-tags')][.//h2[contains(.,'Tags')]]"
            "//div[contains(@class,'ArticlePage-tags-list')]//a[contains(@href,'/tag/')]/text()"
        ).getall()

        if not topics:
            topics = response.css("div.ArticlePage-tags-container a[href*='/topic/']::text").getall()

        if not tags:
            tags = response.css("div.ArticlePage-tags-container a[href*='/tag/']::text").getall()

        yield JptScraperItem(
            url=url,
            title=title,
            excerpt=excerpt,
            published_date=published_date,
            topics=clean_list(topics),
            tags=clean_list(tags),
            scraped_at=datetime.now(timezone.utc).isoformat(),
            refresh_existing=self.refresh_existing,
        )
