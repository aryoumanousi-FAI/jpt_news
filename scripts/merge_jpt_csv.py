from __future__ import annotations

from pathlib import Path
import pandas as pd
import csv

ROOT = Path(__file__).resolve().parents[1]
MASTER = ROOT / "jpt_scraper" / "data" / "jpt.csv"
NEW = ROOT / "jpt_scraper" / "data" / "jpt_new.csv"
OUT = MASTER

EXPECTED_COLS = ["url", "title", "excerpt", "published_date", "topics", "tags", "scraped_at"]

# Your dataset is ~8k+ URLs and goes back to 2012.
MIN_UNIQUE_URLS = 5000
MAX_ALLOWED_MIN_DATE = pd.Timestamp("2014-01-01")  # master should start earlier than this


def _read_multiline_csv(path: Path) -> pd.DataFrame:
    """
    Read a comma CSV that may contain quoted multiline fields (e.g., excerpt).
    engine=python is slower but correct for multiline content.
    """
    if not path.exists():
        return pd.DataFrame()

    return pd.read_csv(
        path,
        sep=",",
        dtype=str,
        keep_default_na=False,
        engine="python",
        quotechar='"',
        doublequote=True,
    )


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # clean headers
    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]

    # ensure expected columns exist
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = ""

    # clean values
    for c in EXPECTED_COLS:
        df[c] = df[c].astype(str).str.strip()

    # drop missing urls
    df = df[df["url"] != ""].copy()
    return df


def _guard_master(master: pd.DataFrame) -> None:
    """
    Prevent overwriting history if master is accidentally small or too recent.
    """
    if master.empty:
        raise RuntimeError("ABORT: master parsed empty. Not overwriting.")

    unique_urls = master["url"].nunique()

    dt = pd.to_datetime(master["published_date"], errors="coerce")
    min_dt = dt.min() if dt.notna().any() else pd.NaT

    # If URL count is big enough, we consider it safe even if some dates are missing.
    if unique_urls >= MIN_UNIQUE_URLS:
        # additionally ensure it's not a "recent-only" dataset
        if pd.notna(min_dt) and min_dt > MAX_ALLOWED_MIN_DATE:
            raise RuntimeError(
                f"ABORT: master looks recent-only (min published_date {min_dt.date()}). Not overwriting."
            )
        return

    raise RuntimeError(
        f"ABORT: master too small (unique URLs {unique_urls} < {MIN_UNIQUE_URLS}). Not overwriting."
    )


def main() -> None:
    old_raw = _read_multiline_csv(MASTER)
    new_raw = _read_multiline_csv(NEW)

    old = _normalize(old_raw)
    new = _normalize(new_raw)

    if old.empty and new.empty:
        print("Both master and new are empty. Nothing to do.")
        return

    # Guard: only if master exists (first run can be allowed if you choose)
    if MASTER.exists():
        _guard_master(old)

    combined = pd.concat([old, new], ignore_index=True)

    # De-dupe by URL (keep newest scraped_at)
    combined["_scraped"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
    combined = combined.sort_values(["url", "_scraped"], ascending=[True, True])
    combined = combined.drop_duplicates(subset=["url"], keep="last").drop(columns=["_scraped"])

    # Sort newest first by published_date
    combined["_pub"] = pd.to_datetime(combined["published_date"], errors="coerce")
    combined = combined.sort_values("_pub", ascending=False, na_position="last").drop(columns=["_pub"])

    OUT.parent.mkdir(parents=True, exist_ok=True)

    # Write consistently (this helps keep the file stable over time)
    combined.to_csv(
        OUT,
        index=False,
        encoding="utf-8",
        quoting=csv.QUOTE_MINIMAL,
    )

    print(f"Merged OK: old={len(old)} new={len(new)} => out={len(combined)}")
    print(f"Unique URLs out: {combined['url'].nunique()}")
    print(f"Wrote: {OUT}")


if __name__ == "__main__":
    main()
