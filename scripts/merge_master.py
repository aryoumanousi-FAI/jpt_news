from __future__ import annotations

from pathlib import Path
import pandas as pd


# --- PATHS ---
SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent

SCRAPY_ROOT = REPO_ROOT / "jpt_scraper"
DATA_DIR = SCRAPY_ROOT / "data"

MASTER_CSV = DATA_DIR / "jpt.csv"
NEW_CSV = DATA_DIR / "_new.csv"

# --- BEHAVIOR ---
# New data should update existing URLs (no duplicate URLs in final master).
NEW_WINS_ON_DUPLICATE = True

# Prevent accidental wipe if something goes wrong (paths, spider output, etc.)
GUARD_AGAINST_SHRINK = True

# Delete _new.csv after a successful merge
DELETE_NEW_AFTER_MERGE = True


def main() -> None:
    if not NEW_CSV.exists():
        print("No _new.csv found. Nothing to merge.")
        return

    new_df = pd.read_csv(NEW_CSV)
    if "url" not in new_df.columns:
        raise ValueError("_new.csv missing required 'url' column.")

    old_df = pd.read_csv(MASTER_CSV) if MASTER_CSV.exists() else pd.DataFrame()

    old_unique = (
        old_df["url"].nunique()
        if (not old_df.empty and "url" in old_df.columns)
        else len(old_df)
    )

    # Combine
    combined = pd.concat([old_df, new_df], ignore_index=True)

    # If scraped_at exists, sort so "newest" ends up last (then keep="last")
    if "scraped_at" in combined.columns:
        combined["scraped_at"] = pd.to_datetime(combined["scraped_at"], errors="coerce")
        combined = combined.sort_values("scraped_at", ascending=True, kind="mergesort")

    # Deduplicate so final has NO duplicate URLs
    keep_rule = "last" if NEW_WINS_ON_DUPLICATE else "first"
    combined = combined.drop_duplicates(subset=["url"], keep=keep_rule)

    # Optional: nice ordering for output
    if "published_date" in combined.columns:
        combined["published_date"] = pd.to_datetime(combined["published_date"], errors="coerce")
        if "scraped_at" in combined.columns:
            combined = combined.sort_values(
                by=["published_date", "scraped_at"],
                ascending=[False, False],
                kind="mergesort",
            )
        else:
            combined = combined.sort_values(
                by=["published_date"],
                ascending=[False],
                kind="mergesort",
            )
    elif "scraped_at" in combined.columns:
        combined = combined.sort_values("scraped_at", ascending=False, kind="mergesort")

    # Guard: never overwrite master with fewer unique URLs than before
    if GUARD_AGAINST_SHRINK and MASTER_CSV.exists() and old_unique > 0:
        new_unique = combined["url"].nunique()
        if new_unique < old_unique:
            raise RuntimeError(
                f"ABORT: merged master would shrink (unique urls {new_unique} < {old_unique}). "
                "Not overwriting jpt.csv."
            )

    # Atomic write
    tmp_master = MASTER_CSV.with_suffix(".csv.tmp")
    combined.to_csv(tmp_master, index=False)
    tmp_master.replace(MASTER_CSV)

    print(f"Master updated: {len(old_df)} -> {len(combined)} rows")
    print(f"Unique URLs in master: {combined['url'].nunique()}")

    if DELETE_NEW_AFTER_MERGE:
        NEW_CSV.unlink(missing_ok=True)
        print("Deleted _new.csv after merge.")


if __name__ == "__main__":
    main()
