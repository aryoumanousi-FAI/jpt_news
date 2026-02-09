from __future__ import annotations

from pathlib import Path
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]  # repo root
MASTER = ROOT / "jpt_scraper" / "data" / "jpt.csv"
NEW    = ROOT / "jpt_scraper" / "data" / "jpt_new.csv"
OUT    = MASTER


def _read_csv(p: Path) -> pd.DataFrame:
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p)

def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # Ensure expected columns exist
    for c in ["url", "title", "excerpt", "published_date", "topics", "tags", "scraped_at"]:
        if c not in df.columns:
            df[c] = ""

    # Normalize url
    df["url"] = df["url"].astype(str).str.strip()
    df = df[df["url"].astype(bool)].copy()

    # Parse dates safely (YYYY-MM-DD)
    df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce").dt.date.astype(str)
    # If parsing failed, keep original string
    df["published_date"] = df["published_date"].replace("NaT", "")

    return df

def main():
    old = _normalize(_read_csv(MASTER))
    new = _normalize(_read_csv(NEW))

    if new.empty and old.empty:
        print("Both master and new are empty. Nothing to do.")
        return

    # Combine and de-dupe by URL (keep newest scrape)
    combined = pd.concat([old, new], ignore_index=True)

    # Prefer rows with newer scraped_at if present; otherwise last occurrence wins
    if "scraped_at" in combined.columns:
        combined["scraped_at_sort"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
        combined = combined.sort_values(["url", "scraped_at_sort"], ascending=[True, True])
        combined = combined.drop_duplicates(subset=["url"], keep="last").drop(columns=["scraped_at_sort"])
    else:
        combined = combined.drop_duplicates(subset=["url"], keep="last")

    # Sort newest first if dates parseable
    dt = pd.to_datetime(combined["published_date"], errors="coerce")
    combined = combined.assign(_pd=dt).sort_values("_pd", ascending=False, na_position="last").drop(columns=["_pd"])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUT, index=False, encoding="utf-8")
    print(f"Merged: old={len(old)} new={len(new)} => out={len(combined)}")
    print(f"Wrote: {OUT}")

if __name__ == "__main__":
    main()

