# app.py — JPT News Explorer (CSV) with:
# - Last updated banner (file mtime + latest scraped_at)
# - Tag + Topic normalization (Title Case + acronym preservation)
# - Canonical tag mapping using all_tags.csv (case-insensitive; column: tag)
# - Country filter (derived from tags) + manual additions: US, UK, UAE
# - Cascading filters across Topics, Tags, Countries
# - Toggle OR/AND matching for Topics, Tags, Countries via UI
# - Clickable links IN the main table (HTML)
# - Pagination (25 rows/page)
# - Header bold + centered; Date single-line (no wrap)

from __future__ import annotations

import ast
import html
import re
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Set

import pandas as pd
import streamlit as st


# -------------------
# Config
# -------------------
DATA_PATH = Path("jpt_scraper/data/jpt.csv")
ALL_TAGS_PATH = Path("all_tags.csv")  # must contain column: tag
PAGE_SIZE = 25


# -------------------
# Normalization
# -------------------
WORD_SPLIT_RE = re.compile(r"(\s+|[-/])")  # keep separators

BASE_ACRONYMS = {
    "AI", "ML", "US", "UK", "UAE", "LNG", "CCS", "CO2", "CO₂", "M&A", "HSE", "OPEC",
    "NGL", "FPSO", "FLNG", "EOR", "IOR", "NPT", "R&D", "API", "ISO", "NACE",
    "IIoT", "OT", "IT", "SCADA", "PLC", "DCS", "ESG", "GHG",
}

COUNTRY_ABBREV = {
    "US": "US",
    "U.S.": "US",
    "USA": "US",
    "United States": "US",
    "United States Of America": "US",
    "UK": "UK",
    "U.K.": "UK",
    "United Kingdom": "UK",
    "Great Britain": "UK",
    "Britain": "UK",
    "UAE": "UAE",
    "U.A.E.": "UAE",
    "United Arab Emirates": "UAE",
}


def _normalize_text(x) -> str:
    if x is None:
        return ""
    if isinstance(x, float) and pd.isna(x):
        return ""
    return " ".join(str(x).split()).strip()


def _parse_listish(value) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]

    s = str(value).strip()
    if not s:
        return []

    if s.startswith("[") and s.endswith("]"):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, list):
                return [str(v).strip() for v in parsed if str(v).strip()]
        except Exception:
            pass

    return [p.strip() for p in s.split(",") if p.strip()]


def _looks_like_acronym(token: str, acronyms: Set[str]) -> bool:
    if not token:
        return False
    t = token.strip()

    if t.upper() in acronyms:
        return True
    if re.fullmatch(r"[A-Za-z]{1,4}\d{1,3}", t):
        return True
    if re.fullmatch(r"[A-Z]{2,}", t):
        return True
    if re.search(r"[&.]", t) and re.search(r"[A-Za-z]", t):
        return True

    return False


def _smart_title_token(token: str, acronyms: Set[str]) -> str:
    raw = token.strip()
    if not raw:
        return token

    if raw.isspace() or raw in {"-", "/"}:
        return raw

    if _looks_like_acronym(raw, acronyms):
        up = raw.upper()
        return "CO2" if up == "CO₂" else up

    # preserve intentional internal casing (e.g., "iPhone", "eBay", "McDermott")
    if any(c.isupper() for c in raw[1:]) and any(c.islower() for c in raw):
        return raw

    return raw[:1].upper() + raw[1:].lower()


def normalize_phrase(s: str, acronyms: Set[str]) -> str:
    s = _normalize_text(s)
    if not s:
        return ""
    parts = WORD_SPLIT_RE.split(s)
    out = "".join(_smart_title_token(p, acronyms) for p in parts)
    out = re.sub(r"\s+", " ", out).strip()
    out = out.replace("Co2", "CO2").replace("Co₂", "CO2")
    return out


def load_master_tags(path: Path) -> List[str]:
    if not path.exists():
        return []
    df = pd.read_csv(path)
    if "tag" not in df.columns:
        return []
    return [_normalize_text(x) for x in df["tag"].tolist() if _normalize_text(x)]


def build_acronym_set(master_tags: List[str]) -> Set[str]:
    acronyms = set(BASE_ACRONYMS)
    for t in master_tags:
        t = _normalize_text(t)
        if not t:
            continue
        if re.fullmatch(r"[A-Z0-9&./-]{2,}", t) and re.search(r"[A-Z]", t):
            acronyms.add(t.upper())
        if re.search(r"[&.]", t) and re.search(r"[A-Za-z]", t):
            acronyms.add(t.upper())
    return acronyms


def build_canonical_tag_map(master_tags: List[str], acronyms: Set[str]) -> Dict[str, str]:
    m: Dict[str, str] = {}
    for t in master_tags:
        key = _normalize_text(t).lower()
        if not key:
            continue
        m[key] = normalize_phrase(t, acronyms)
    return m


# -------------------
# Countries
# -------------------
@st.cache_resource
def build_country_set_cached() -> Set[str]:
    out: Set[str] = {"US", "UK", "UAE"}
    try:
        import pycountry  # type: ignore

        for c in pycountry.countries:
            for name in [getattr(c, "name", None), getattr(c, "official_name", None), getattr(c, "common_name", None)]:
                if not name:
                    continue
                out.add(COUNTRY_ABBREV.get(name, name))
    except Exception:
        out |= {
            "Canada", "Mexico", "Brazil", "Argentina", "Norway", "France", "Germany", "Italy", "Spain",
            "Australia", "India", "China", "Japan", "Saudi Arabia", "Qatar", "Kuwait", "Oman",
            "Iraq", "Iran", "Libya", "Nigeria", "Angola", "Egypt",
        }
    return out


def canonical_country_from_tag(tag: str) -> str | None:
    t = _normalize_text(tag)
    if not t:
        return None
    if t in COUNTRY_ABBREV:
        return COUNTRY_ABBREV[t]
    tl = t.lower()
    for k, v in COUNTRY_ABBREV.items():
        if tl == k.lower():
            return v
    return None


# -------------------
# Last updated banner
# -------------------
def format_last_updated(csv_path: Path, df: pd.DataFrame) -> str:
    try:
        mtime = datetime.fromtimestamp(csv_path.stat().st_mtime)
        file_updated = mtime.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        file_updated = "Unknown"

    data_updated = None
    if "scraped_at" in df.columns:
        s = pd.to_datetime(df["scraped_at"], errors="coerce", utc=True)
        if s.notna().any():
            latest = s.max()
            data_updated = latest.strftime("%Y-%m-%d %H:%M:%S UTC")

    if data_updated:
        return f"Last updated at **{file_updated}** (file) • Latest scrape in data: **{data_updated}**"
    return f"Last updated at **{file_updated}**"


# -------------------
# Data loading
# -------------------
@st.cache_data(ttl=60)
def load_data(csv_path: str, master_tags_path: str) -> pd.DataFrame:
    data_path = Path(csv_path)
    if not data_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(data_path)

    expected = ["url", "title", "excerpt", "published_date", "topics", "tags", "scraped_at", "refresh_existing"]
    for c in expected:
        if c not in df.columns:
            df[c] = ""

    df["url"] = df["url"].map(_normalize_text)
    df["title"] = df["title"].map(_normalize_text)
    df["excerpt"] = df["excerpt"].map(_normalize_text)
    df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce").dt.date

    df["topics_list_raw"] = df["topics"].map(_parse_listish)
    df["tags_list_raw"] = df["tags"].map(_parse_listish)

    master_tags = load_master_tags(Path(master_tags_path))
    acronyms = build_acronym_set(master_tags)
    canon_map = build_canonical_tag_map(master_tags, acronyms)

    def norm_tag(x: str) -> str:
        x0 = _normalize_text(x)
        if not x0:
            return ""
        key = x0.lower()
        if key in canon_map:
            return canon_map[key]
        return normalize_phrase(x0, acronyms)

    df["tags_list"] = df["tags_list_raw"].apply(
        lambda xs: [norm_tag(t) for t in (xs or []) if norm_tag(t)]
    )

    df["topics_list"] = df["topics_list_raw"].apply(
        lambda xs: [normalize_phrase(_normalize_text(t), acronyms) for t in (xs or []) if _normalize_text(t)]
    )

    df = df[df["url"].astype(bool)].copy()
    df = df.drop_duplicates(subset=["url"], keep="last")
    df = df.sort_values("published_date", ascending=False, na_position="last").reset_index(drop=True)

    country_set = build_country_set_cached()

    def countries_from_tags(tags: List[str]) -> List[str]:
        found: Set[str] = set()
        for t in tags or []:
            c = canonical_country_from_tag(t)
            if c:
                found.add(c)
        for t in tags or []:
            if t in country_set:
                found.add(t)
        return sorted(found)

    df["countries_list"] = df["tags_list"].apply(countries_from_tags)
    return df


# -------------------
# Filtering
# -------------------
def match_keywords(text: str, keywords: list[str], any_mode: bool) -> bool:
    if not keywords:
        return True
    t = (text or "").lower()
    ks = [k.lower() for k in keywords]
    return any(k in t for k in ks) if any_mode else all(k in t for k in ks)


def must_include_all(selected: list[str], row_values: list[str]) -> bool:
    if not selected:
        return True
    row_set = set(row_values or [])
    return all(s in row_set for s in selected)


def must_include_any(selected: list[str], row_values: list[str]) -> bool:
    if not selected:
        return True
    row_set = set(row_values or [])
    return any(s in row_set for s in selected)


def apply_match(selected: list[str], row_values: list[str], mode: str) -> bool:
    # mode: "OR" or "AND"
    if mode == "AND":
        return must_include_all(selected, row_values)
    return must_include_any(selected, row_values)


def available_values_from_subset(subset: pd.DataFrame, col_list: str) -> list[str]:
    values: set[str] = set()
    for xs in subset[col_list].tolist():
        for x in xs or []:
            values.add(x)
    return sorted(values)


def apply_filters(
    df: pd.DataFrame,
    start_d: date,
    end_d: date,
    keyword_list: list[str],
    any_mode: bool,
    selected_topics: list[str],
    selected_tags: list[str],
    selected_countries: list[str],
    topics_mode: str,
    tags_mode: str,
    countries_mode: str,
) -> pd.Series:
    mask = pd.Series(True, index=df.index)

    mask &= df["published_date"].between(start_d, end_d, inclusive="both")

    if keyword_list:
        combined = (df["title"].fillna("") + " " + df["excerpt"].fillna("")).astype(str)
        mask &= combined.apply(lambda t: match_keywords(t, keyword_list, any_mode))

    if selected_topics:
        mask &= df["topics_list"].apply(lambda xs: apply_match(selected_topics, xs, topics_mode))

    if selected_tags:
        mask &= df["tags_list"].apply(lambda xs: apply_match(selected_tags, xs, tags_mode))

    if selected_countries:
        mask &= df["countries_list"].apply(lambda xs: apply_match(selected_countries, xs, countries_mode))

    return mask


def make_html_link(url: str, title: str) -> str:
    u = html.escape(url, quote=True)
    t = html.escape(title)
    return f'<a href="{u}" target="_blank" rel="noopener noreferrer">{t}</a>'


# -------------------
# UI
# -------------------
st.set_page_config(page_title="JPT News", layout="wide")
st.title("JPT News Explorer")

if not DATA_PATH.exists():
    st.warning(
        f"Could not find your CSV at `{DATA_PATH}`.\n\n"
        "Update `DATA_PATH` at the top of `app.py` to point to your exported CSV."
    )
    st.stop()

df = load_data(str(DATA_PATH), str(ALL_TAGS_PATH))
st.info(format_last_updated(DATA_PATH, df))

if df.empty:
    st.info("No rows found in the CSV (or required columns missing).")
    st.stop()

st.caption(
    f"Loaded **{len(df)}** articles from **{DATA_PATH.as_posix()}**. "
    "Filters use title + excerpt + normalized topics + normalized tags + published date."
)

min_date = df["published_date"].min()
max_date = df["published_date"].max()
if pd.isna(min_date) or min_date is None:
    min_date = date(2000, 1, 1)
if pd.isna(max_date) or max_date is None:
    max_date = date.today()

for key in ["selected_topics", "selected_tags", "selected_countries", "topics_mode", "tags_mode", "countries_mode"]:
    if key not in st.session_state:
        if key.endswith("_mode"):
            st.session_state[key] = "OR"
        else:
            st.session_state[key] = []

# Defaults:
# - Topics: OR
# - Countries: OR
# - Tags: AND (as you requested originally, but now toggleable)
st.session_state.tags_mode = st.session_state.get("tags_mode", "AND")

with st.sidebar:
    st.header("Filters")

    keyword_input = st.text_input("Keywords (comma-separated)", "")
    keyword_mode = st.radio("Keyword match", ["Any keyword", "All keywords"], index=0)
    keyword_list = [k.strip() for k in keyword_input.split(",") if k.strip()]
    any_mode = (keyword_mode == "Any keyword")

    st.divider()

    start_d, end_d = st.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    st.divider()
    st.subheader("Match mode")
    st.radio("Topics match", ["OR", "AND"], key="topics_mode", horizontal=True)
    st.radio("Tags match", ["OR", "AND"], key="tags_mode", horizontal=True)
    st.radio("Countries match", ["OR", "AND"], key="countries_mode", horizontal=True)

    st.divider()

    # Topics depend on everything except topics
    mask_topics = apply_filters(
        df=df,
        start_d=start_d,
        end_d=end_d,
        keyword_list=keyword_list,
        any_mode=any_mode,
        selected_topics=[],
        selected_tags=st.session_state.selected_tags,
        selected_countries=st.session_state.selected_countries,
        topics_mode=st.session_state.topics_mode,
        tags_mode=st.session_state.tags_mode,
        countries_mode=st.session_state.countries_mode,
    )
    avail_topics = available_values_from_subset(df[mask_topics], "topics_list")
    st.session_state.selected_topics = [t for t in st.session_state.selected_topics if t in avail_topics]
    st.multiselect(
        f"Topics ({'match any' if st.session_state.topics_mode=='OR' else 'match all'})",
        options=avail_topics,
        key="selected_topics",
    )

    # Tags depend on everything except tags
    mask_tags = apply_filters(
        df=df,
        start_d=start_d,
        end_d=end_d,
        keyword_list=keyword_list,
        any_mode=any_mode,
        selected_topics=st.session_state.selected_topics,
        selected_tags=[],
        selected_countries=st.session_state.selected_countries,
        topics_mode=st.session_state.topics_mode,
        tags_mode=st.session_state.tags_mode,
        countries_mode=st.session_state.countries_mode,
    )
    avail_tags = available_values_from_subset(df[mask_tags], "tags_list")
    st.session_state.selected_tags = [t for t in st.session_state.selected_tags if t in avail_tags]
    st.multiselect(
        f"Tags ({'match any' if st.session_state.tags_mode=='OR' else 'match all'})",
        options=avail_tags,
        key="selected_tags",
    )

    # Countries depend on everything except countries
    mask_countries = apply_filters(
        df=df,
        start_d=start_d,
        end_d=end_d,
        keyword_list=keyword_list,
        any_mode=any_mode,
        selected_topics=st.session_state.selected_topics,
        selected_tags=st.session_state.selected_tags,
        selected_countries=[],
        topics_mode=st.session_state.topics_mode,
        tags_mode=st.session_state.tags_mode,
        countries_mode=st.session_state.countries_mode,
    )
    avail_countries = available_values_from_subset(df[mask_countries], "countries_list")
    st.session_state.selected_countries = [c for c in st.session_state.selected_countries if c in avail_countries]
    st.multiselect(
        f"Country ({'match any' if st.session_state.countries_mode=='OR' else 'match all'})",
        options=avail_countries,
        key="selected_countries",
    )

    st.divider()
    st.caption("Tip: your GitHub Action / scheduler updates the CSV. Refresh this page to see new data.")

final_mask = apply_filters(
    df=df,
    start_d=start_d,
    end_d=end_d,
    keyword_list=keyword_list,
    any_mode=any_mode,
    selected_topics=st.session_state.selected_topics,
    selected_tags=st.session_state.selected_tags,
    selected_countries=st.session_state.selected_countries,
    topics_mode=st.session_state.topics_mode,
    tags_mode=st.session_state.tags_mode,
    countries_mode=st.session_state.countries_mode,
)

results = df[final_mask].copy()

st.subheader(f"Results ({len(results)})")

if results.empty:
    st.info("No matches. Try widening the date range or removing some filters.")
    st.stop()

# Pagination
total_rows = len(results)
total_pages = max(1, (total_rows + PAGE_SIZE - 1) // PAGE_SIZE)

colA, colB, colC = st.columns([2, 3, 5])
with colA:
    page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)
with colB:
    st.caption(f"{total_pages} page(s) • {PAGE_SIZE} rows/page")
with colC:
    st.caption("Showing newest first")

start_i = (page - 1) * PAGE_SIZE
end_i = start_i + PAGE_SIZE
page_df = results.iloc[start_i:end_i].copy()

page_df["Date"] = page_df["published_date"].astype(str)
page_df["Countries"] = page_df["countries_list"].apply(lambda xs: ", ".join(xs or []))
page_df["Topics"] = page_df["topics_list"].apply(lambda xs: ", ".join(xs or []))
page_df["Tags"] = page_df["tags_list"].apply(lambda xs: ", ".join(xs or []))
page_df["Article"] = page_df.apply(lambda r: make_html_link(r["url"], r["title"]), axis=1)

table = page_df[["Date", "Article", "Countries", "Topics", "Tags", "excerpt"]].rename(columns={"excerpt": "Excerpt"})

css = """
<style>
table { width: 100%; border-collapse: collapse; }
thead th {
  font-weight: 700 !important;
  text-align: center !important;
  border-bottom: 2px solid #ddd;
  padding: 10px 8px;
}
tbody td {
  border-bottom: 1px solid #eee;
  padding: 8px;
  vertical-align: top;
}
tbody td:first-child, thead th:first-child {
  white-space: nowrap;   /* Date column single-line */
}
</style>
"""

st.markdown(css, unsafe_allow_html=True)
st.write(table.to_html(escape=False, index=False), unsafe_allow_html=True)

st.caption(f"Showing rows {start_i + 1}-{min(end_i, total_rows)} of {total_rows}.")

# Download filtered results (all rows, not just this page)
download_df = results.copy()
download_df["topics"] = download_df["topics_list"].apply(lambda xs: ", ".join(xs or []))
download_df["tags"] = download_df["tags_list"].apply(lambda xs: ", ".join(xs or []))
download_df["countries"] = download_df["countries_list"].apply(lambda xs: ", ".join(xs or []))

cols_out = [
    c for c in [
        "published_date", "url", "title", "excerpt",
        "topics", "tags", "countries", "scraped_at"
    ]
    if c in download_df.columns
]

st.download_button(
    "Download filtered results (CSV)",
    data=download_df[cols_out].to_csv(index=False).encode("utf-8"),
    file_name="jpt_filtered.csv",
    mime="text/csv",
)
