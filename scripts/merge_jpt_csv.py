from pathlib import Path
import pandas as pd

MASTER = Path("jpt_scraper/data/jpt.csv")
NEW = Path("jpt_scraper/data/jpt_new.csv")

if not NEW.exists():
    print("No jpt_new.csv produced. Nothing to merge.")
    raise SystemExit(0)

new_df = pd.read_csv(NEW)

if MASTER.exists():
    old_df = pd.read_csv(MASTER)
    df = pd.concat([old_df, new_df], ignore_index=True)
else:
    df = new_df

# Deduplicate by URL, keep newest scrape
df = df.drop_duplicates(subset=["url"], keep="last")

# Sort newest first if dates exist
if "published_date" in df.columns:
    df = df.sort_values("published_date", ascending=False)

df.to_csv(MASTER, index=False, encoding="utf-8")
print(f"Updated master CSV: {MASTER}  (rows={len(df)})")

# Optional cleanup
NEW.unlink()
print("Deleted temporary jpt_new.csv")
