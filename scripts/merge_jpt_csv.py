from __future__ import annotations

from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
MASTER = REPO_ROOT / "jpt_scraper" / "data" / "jpt.csv"
NEW = REPO_ROOT / "jpt_scraper" / "data" / "_new.csv"


def main() -> None:
    if not MASTER.exists():
        raise FileNotFoundError(f"Master CSV not found: {MASTER}")
    if not NEW.exists():
        print("No _new.csv found. Nothing to merge.")
        return

    old_df = pd.read_csv(MASTER)
    new_df = pd.read_csv(NEW)

    if "url" not in new_df.columns:
        raise ValueError("New scrape output must include a 'url' column.")

    combined = pd.concat([old_df, new_df], ignore_index=True)

    # Keep newest row per URL
    combined["scraped_at"] = pd.to_datetime(
        combined.get("scraped_at"), errors="coerce"
    )
    combined = combined.sort_values("scraped_at")
    combined = combined.drop_duplicates(subset=["url"], keep="last")

    # Optional tidy sort
    if "published_date" in combined.columns:
        combined["published_date"] = pd.to_datetime(
            combined["published_date"], errors="coerce"
        )
        combined = combined.sort_values(
            ["published_date", "scraped_at"],
            ascending=[False, False],
        )

    combined.to_csv(MASTER, index=False)
    print(f"Merged → {len(old_df)} → {len(combined)} rows")


if __name__ == "__main__":
    main()
