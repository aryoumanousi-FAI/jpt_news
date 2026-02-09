from __future__ import annotations

from pathlib import Path
import csv
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MASTER = ROOT / "jpt_scraper" / "data" / "jpt.csv"
NEW = ROOT / "jpt_scraper" / "data" / "jpt_new.csv"
OUT = MASTER

EXPECTED_COLS = ["url", "title", "excerpt", "published_date", "topics", "tags", "scraped_at"]

# --- Safety expectations for your dataset ---
# Full history should go back to ~2012 and be thousands of unique URLs.
MIN_UNIQUE_URLS = 7000
MAX_ALLOWED_MIN_DATE = pd.Timestamp("2014-01-01")  # if master starts after this, it's suspicious


def _sniff_sep(p: Path) -> str:
    sample = p.read_text(encoding="utf-8", errors="ignore")[:50000].replace("\x00", "")
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
        return dialect.delimiter
    except Exception:
        first_line = sample.splitlines()[0] if sample.splitlines() else ""
        return "\t" if first_line.count("\t") >= 2 else ","


def _read_any_delim(p: Path) -> pd.DataFrame:
    if not p.exists():
        return pd.DataFrame()
    sep = _sniff_sep(p)
    return pd.read_csv(
        p,
        sep=sep,
        dtype=str,
        keep_default_na=False,
        engine="python",
    )


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]

    lower_map = {c.lower(): c for c in df.columns}
    for candidate in ["url", "link", "permalink", "href"]:
        if "url" not in df.columns and candidate in lower_map:
            df = df.rename(columns={lower_map[candidate]: "url"})
            break
    return df


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = _normalize_columns(df)

    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = ""

    for c in EXPECTED_COLS:
        df[c] = df[c].astype(str).str.strip()

    df = df[df["url"] != ""].copy()
    return df


def _master_looks_valid(old: pd.DataFrame) -> tuple[bool, str]:
    """Return (ok, reason_if_not_ok)."""
    if old.empty:
        return False, "master is empty after parsing"

    unique_urls = old["url"].nunique()

    # parse min published_date
    dt = pd.to_datetime(old["published_date"], errors="coerce")
    min_dt = dt.min() if dt.notna().any() else pd.NaT

    if unique_urls < MIN_UNIQUE_URLS:
        return False, f"too few unique URLs ({unique_urls} < {MIN_UNIQUE_URLS})"

    if pd.notna(min_dt) and min_dt > MAX_ALLOWED_MIN_DATE:
        return False, f"history too recent (min published_date {min_dt.date()} > {MAX_ALLOWED_MIN_DATE.date()})"

    # If min_dt is NaT, we still allow it only if URLs are large (already checked)
    return True, "ok"


def main() -> None:
    old_raw = _read_any_delim(MASTER)
    new_raw = _read_any_delim(NEW)

    old = _normalize(old_raw)
    new = _normalize(new_raw)

    if old.empty and new.empty:
        print("Both master and new are empty. Nothing to do.")
        return

    # Guard: never overwrite a suspicious / truncated master
    if MASTER.exists() and not old_raw.empty:
        ok, reason = _master_looks_valid(old)
        if not ok:
            raise RuntimeError(
                f"ABORT: master jpt.csv looks wrong ({reason}). Not overwriting."
            )

    combined = pd.concat([old, new], ignore_index=True)

    # Keep newest scraped_at per URL
    combined["_scraped"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
    combined = combined.sort_values(["url", "_scraped"], ascending=[True, True])
    combined = combined.drop_duplicates(subset=["url"], keep="last").drop(columns=["_scraped"])

    # Sort newest first by published_date
    combined["_pub"] = pd.to_datetime(combined["published_date"], errors="coerce")
    combined = combined.sort_values("_pub", ascending=False, na_position="last").drop(columns=["_pub"])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUT, index=False, encoding="utf-8")

    print(f"Merged OK: old={len(old)} new={len(new)} => out={len(combined)}")
    print(f"Wrote: {OUT}")


if __name__ == "__main__":
    main()
