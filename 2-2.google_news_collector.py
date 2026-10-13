# -*- coding: utf-8 -*-
"""
GTI STEP2-2 GOOGLE NEWS BROAD v5
- Google News RSS broad collection only
- 72-hour collection window
- No strict customs relevance filtering here (Step3-2 owns selection)
- Dedup URL/title
- Always rewrites 2-2.google_news_raw.xlsx for the current run
"""

from __future__ import annotations
import os, re, time
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import quote
import pandas as pd
import feedparser
from bs4 import BeautifulSoup

BASE_DIR = Path(os.getenv("GTI_BASE_DIR", r"C:\Temp"))
KEYWORD_FILE = BASE_DIR / "keyword.xlsx"
OUTPUT_FILE = BASE_DIR / "2-2.google_news_raw.xlsx"

LOOKBACK_HOURS = int(os.getenv("GTI_LOOKBACK_HOURS", "72"))
MAX_KEYWORDS = int(os.getenv("GTI_GOOGLE_MAX_KEYWORDS", "220"))
MAX_ROWS = int(os.getenv("GTI_GOOGLE_MAX_ROWS", "2500"))
SLEEP_SEC = float(os.getenv("GTI_GOOGLE_SLEEP_SEC", "0.03"))

FINAL_COLS = [
    "date", "title", "url", "google_url", "source", "summary",
    "collected_at", "keyword", "language", "publisher", "category",
    "importance", "importance_score", "score_reason", "url_decode_status",
    "original_url_candidate", "rss_url",
]

LOCALES = {
    "KR": ("ko", "KR", "KR:ko"),
    "KO": ("ko", "KR", "KR:ko"),
    "EN": ("en", "US", "US:en"),
    "CN": ("zh-CN", "CN", "CN:zh-Hans"),
    "ZH": ("zh-CN", "CN", "CN:zh-Hans"),
    "JP": ("ja", "JP", "JP:ja"),
    "JA": ("ja", "JP", "JP:ja"),
    "ES": ("es", "ES", "ES:es"),
    "PT": ("pt-BR", "BR", "BR:pt-419"),
    "VI": ("vi", "VN", "VN:vi"),
    "HI": ("hi", "IN", "IN:hi"),
}


def log(msg: str) -> None:
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}")


def clean(v) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return re.sub(r"\s+", " ", str(v)).strip()


def clean_html(v) -> str:
    return re.sub(r"\s+", " ", BeautifulSoup(clean(v), "html.parser").get_text(" ", strip=True)).strip()


def active(v) -> bool:
    return clean(v).upper() not in {"N", "NO", "0", "FALSE", "OFF", "INACTIVE"}


def importance_score(v) -> int:
    s = clean(v).upper()
    table = {"HIGH": 100, "H": 100, "A": 100, "MEDIUM": 70, "M": 70, "B": 70, "LOW": 50, "L": 50, "C": 50}
    if s in table:
        return table[s]
    try:
        return int(float(s))
    except Exception:
        return 50


def normalize_title(v) -> str:
    t = clean_html(v).lower()
    if " - " in t:
        t = t.rsplit(" - ", 1)[0]
    t = re.sub(r"[^\w가-힣一-龥ぁ-ゔァ-ヴー\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def entry_datetime(entry):
    for k in ["published_parsed", "updated_parsed"]:
        v = getattr(entry, k, None)
        if v:
            try:
                return datetime(*v[:6])
            except Exception:
                pass
    return None


def publisher_of(entry, title: str) -> str:
    try:
        src = entry.get("source", {})
        if isinstance(src, dict) and clean(src.get("title")):
            return clean(src.get("title"))
    except Exception:
        pass
    if " - " in title:
        return title.rsplit(" - ", 1)[-1].strip()
    return ""


def read_keywords() -> pd.DataFrame:
    if not KEYWORD_FILE.exists():
        raise FileNotFoundError(f"keyword file not found: {KEYWORD_FILE}")
    df = pd.read_excel(KEYWORD_FILE)
    df.columns = [str(c).strip() for c in df.columns]
    lower = {c.lower(): c for c in df.columns}

    kw_col = lower.get("keyword") or lower.get("키워드")
    if not kw_col:
        raise ValueError("keyword.xlsx: keyword column not found")

    lang_col = lower.get("language")
    cat_col = lower.get("category")
    imp_col = lower.get("importance")
    act_col = lower.get("active")

    out = pd.DataFrame()
    out["keyword"] = df[kw_col].fillna("").astype(str).map(clean)
    out["language"] = df[lang_col].fillna("EN").astype(str).map(clean) if lang_col else "EN"
    out["category"] = df[cat_col].fillna("").astype(str).map(clean) if cat_col else ""
    out["importance"] = df[imp_col].fillna("").astype(str).map(clean) if imp_col else ""
    if act_col:
        out = out[df[act_col].map(active)].copy()
    out = out[out["keyword"].ne("")].drop_duplicates(["keyword", "language"]).head(MAX_KEYWORDS)
    return out.reset_index(drop=True)


def rss_url(keyword: str, language: str) -> str:
    hl, gl, ceid = LOCALES.get(language.upper(), LOCALES["EN"])
    return f"https://news.google.com/rss/search?q={quote(keyword)}&hl={hl}&gl={gl}&ceid={ceid}"


def collect() -> pd.DataFrame:
    kws = read_keywords()
    log(f"keywords={len(kws)} / lookback={LOOKBACK_HOURS}h")
    cutoff = datetime.now() - timedelta(hours=LOOKBACK_HOURS)
    rows = []

    for idx, r in kws.iterrows():
        kw = clean(r["keyword"])
        lang = clean(r["language"]) or "EN"
        feed_url = rss_url(kw, lang)

        try:
            feed = feedparser.parse(feed_url)
        except Exception as exc:
            log(f"FETCH FAIL: {kw} / {type(exc).__name__}")
            continue

        for e in feed.entries:
            dt = entry_datetime(e)
            if dt is None or dt < cutoff or dt > datetime.now() + timedelta(hours=2):
                continue

            title = clean_html(e.get("title", ""))
            if not title:
                continue
            gurl = clean(e.get("link", ""))
            summary = clean_html(e.get("summary", e.get("description", "")))
            publisher = publisher_of(e, title)

            rows.append({
                "date": dt,
                "title": title,
                "url": gurl,                 # Step3/4 may resolve publisher URL later
                "google_url": gurl,
                "source": publisher or "Google News",
                "summary": summary,
                "collected_at": datetime.now(),
                "keyword": kw,
                "language": lang,
                "publisher": publisher,
                "category": clean(r.get("category")),
                "importance": clean(r.get("importance")),
                "importance_score": importance_score(r.get("importance")),
                "score_reason": "BROAD_COLLECTION",
                "url_decode_status": "GOOGLE_RSS",
                "original_url_candidate": "",
                "rss_url": feed_url,
            })

        if (idx + 1) % 25 == 0:
            log(f"progress {idx+1}/{len(kws)} / raw={len(rows)}")
        time.sleep(SLEEP_SEC)

    df = pd.DataFrame(rows, columns=FINAL_COLS)
    if df.empty:
        return df

    df["_title_key"] = df["title"].map(normalize_title)
    df["_url_key"] = df["google_url"].fillna("").astype(str).str.strip()
    df = df.sort_values(["importance_score", "date"], ascending=[False, False], kind="stable")
    before = len(df)
    df = df.drop_duplicates("_url_key", keep="first")
    df = df.drop_duplicates("_title_key", keep="first")
    df = df.drop(columns=["_title_key", "_url_key"])
    df = df.head(MAX_ROWS).reset_index(drop=True)
    log(f"DEDUP: {before} -> {len(df)}")
    return df


def safe_write(df: pd.DataFrame) -> None:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_excel(OUTPUT_FILE, index=False)
    except PermissionError:
        alt = OUTPUT_FILE.with_name(f"{OUTPUT_FILE.stem}_{datetime.now():%Y%m%d_%H%M%S}.xlsx")
        df.to_excel(alt, index=False)
        raise PermissionError(f"{OUTPUT_FILE} locked; saved alternate {alt}")


def main() -> int:
    log("GTI STEP2-2 GOOGLE NEWS BROAD v5 START")
    df = collect()
    safe_write(df)
    log(f"SAVED: {OUTPUT_FILE} / rows={len(df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
