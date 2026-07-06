# GTI FINAL CORE v6 - RSS/Site News with Selenium Google URL recovery
# =========================================================
# GTI STEP2-3 - RSS NEWS RAW MASTER VERSION v3.4
# Purpose
# - Read RSS feeds from C:/Temp/gti_master.xlsx first, then sites.xlsx fallback
# - Scan all sheets, not only Sheet1/site_rss
# - Support active RSS rows from type=rss, parser=rss_parser, or RSS-looking URL
# - Keep Google Alert / Google News redirect decoding
# - Preserve existing output schema: C:/Temp/2-3.rss_news_raw.xlsx
# =========================================================

import os
import re
import socket
import warnings
import importlib.util
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote, urlencode, urlunparse

import pandas as pd
import feedparser
from bs4 import BeautifulSoup
from dateutil import parser

print("GTI STEP2-3 RSS/SITE NEWS-ONLY START v3.4 MASTER")

# ===================== CONFIG =====================
BASE_DIR = os.getenv("GTI_BASE_DIR", r"C:/Temp")
OUTPUT_FILE = os.getenv("GTI_RSS_OUTPUT_FILE", os.path.join(BASE_DIR, "2-3.rss_news_raw.xlsx"))
MASTER_FILE = os.getenv("GTI_MASTER_FILE", os.path.join(BASE_DIR, "gti_master.xlsx"))
SITES_FILE_FALLBACK = os.getenv("GTI_SITES_FILE", os.path.join(BASE_DIR, "sites.xlsx"))
SITE_CRAWLER_FILE = os.getenv("GTI_SITE_CRAWLER_FILE", os.path.join(BASE_DIR, "1.site_crawler.py"))
STEP1_SITE_NEWS_FILE = os.getenv("GTI_STEP1_SITE_NEWS_FILE", os.path.join(BASE_DIR, "1-2.site_news_raw.xlsx"))
os.makedirs(BASE_DIR, exist_ok=True)

LOOKBACK_HOURS = int(os.getenv("GTI_LOOKBACK_HOURS", "72"))
CUT_OFF = datetime.now() - timedelta(hours=LOOKBACK_HOURS)
MAX_FEEDS_TO_READ = int(os.getenv("GTI_RSS_MAX_FEEDS", "500"))
MAX_ITEMS_PER_FEED = int(os.getenv("GTI_RSS_MAX_ITEMS_PER_FEED", "300"))
RSS_FETCH_TIMEOUT = int(os.getenv("GTI_RSS_FETCH_TIMEOUT", "15"))
socket.setdefaulttimeout(RSS_FETCH_TIMEOUT)

# If master reading fails or has too few RSS feeds, optional hard safety fallback.
# Keep N by default because GTI master should be the source of truth.
ENABLE_LEGACY_GOOGLE_ALERT_FALLBACK = os.getenv("GTI_RSS_LEGACY_FALLBACK", "N").strip().upper() == "Y"

FINAL_COLS = [
    "date", "title", "headline", "url", "source", "feed_name",
    "summary", "collected_at",
    "keyword", "category", "importance", "importance_score",
    "score_reason", "url_type", "canonical_url", "url_decode_status",
    "source_channel", "agency", "site_type"
]

TZINFOS = {
    "JST": 9 * 3600,
    "KST": 9 * 3600,
    "CST": 8 * 3600,
    "PST": -8 * 3600,
    "PDT": -7 * 3600,
    "EST": -5 * 3600,
    "EDT": -4 * 3600,
    "CET": 1 * 3600,
    "CEST": 2 * 3600,
    "GMT": 0,
    "UTC": 0,
}

# ===================== MASTER LOAD =====================

def _clean_value(value):
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _is_active(value):
    # If active column is missing, caller should treat row as active.
    v = _clean_value(value).upper()
    return v in {"1", "1.0", "Y", "YES", "TRUE", "T", "ACTIVE", "OK", "?ъ슜"}


def _importance_from_row(row):
    pg = _clean_value(row.get("priority_group", row.get("priority", ""))).upper()
    must_keep = _clean_value(row.get("must_keep", "")).upper()
    official = _clean_value(row.get("official_flag", row.get("official", ""))).upper()

    if must_keep in {"Y", "YES", "TRUE", "1"}:
        return "HIGH", 100
    if pg in {"CORE", "HIGH", "CRITICAL", "A", "WATCH"}:
        return "HIGH", 100 if pg != "WATCH" else 70
    if pg in {"MEDIUM", "MID", "B"}:
        return "MEDIUM", 70
    if pg in {"REFERENCE", "LOW", "C"}:
        return "LOW", 50
    if official in {"Y", "YES", "TRUE", "1"}:
        return "HIGH", 100

    for k in ["source_weight", "source_score", "importance_score"]:
        try:
            weight = float(row.get(k, 0) or 0)
            if weight >= 80:
                return "HIGH", 100
            if weight >= 5:
                return "MEDIUM", 70
        except Exception:
            pass
    return "LOW", 50


def _looks_like_rss_url(url):
    u = _clean_value(url).lower()
    return any(x in u for x in ["/rss", "rss.", ".rss", "feed", "feeds/", "alerts/feeds", "atom", "xml"])


def _is_news_site_type(value):
    return _clean_value(value).lower() in {"", "news", "site_news", "press", "release"}


def _is_rss_row(row, c_type=None, c_parser=None, c_url=None):
    type_v = _clean_value(row.get(c_type, "")).lower() if c_type else ""
    parser_v = _clean_value(row.get(c_parser, "")).lower() if c_parser else ""
    url_v = _clean_value(row.get(c_url, "")) if c_url else ""
    return type_v == "rss" or parser_v == "rss_parser" or _looks_like_rss_url(url_v)


def _norm_col_map(df):
    return {str(c).strip().lower(): c for c in df.columns}


def _get_col(colmap, *names):
    for name in names:
        c = colmap.get(name.lower())
        if c is not None:
            return c
    return None


def _extract_feeds_from_df(df, source_file, sheet_name):
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    colmap = _norm_col_map(df)

    c_name = _get_col(colmap, "site_name", "feed_name", "source_name", "name", "agency")
    c_url = _get_col(colmap, "url", "rss_url", "link", "source_url")
    if not c_name or not c_url:
        return []

    c_type = _get_col(colmap, "type", "source_type")
    c_parser = _get_col(colmap, "parser")
    c_active = _get_col(colmap, "active", "use", "enabled", "status")
    c_policy = _get_col(colmap, "policy_type", "category", "topic")
    c_country = _get_col(colmap, "country")
    c_site_type = _get_col(colmap, "site_type", "track")
    c_include = _get_col(colmap, "include_keywords", "keyword", "keywords")
    c_exclude = _get_col(colmap, "exclude_keywords")

    type_s = df[c_type].fillna("").astype(str).str.lower().str.strip() if c_type else pd.Series([""] * len(df), index=df.index)
    parser_s = df[c_parser].fillna("").astype(str).str.lower().str.strip() if c_parser else pd.Series([""] * len(df), index=df.index)
    url_s = df[c_url].fillna("").astype(str)

    # RSS determination: explicit type/parser OR RSS-looking URL.
    mask = (type_s == "rss") | (parser_s == "rss_parser") | url_s.apply(_looks_like_rss_url)

    if c_active:
        # status=OK should be accepted, blank/inactive should not.
        mask = mask & df[c_active].apply(_is_active)

    rss_df = df[mask].copy()
    feeds = []
    seen_urls = set()
    for _, row in rss_df.iterrows():
        url = _clean_value(row.get(c_url, ""))
        feed_name = _clean_value(row.get(c_name, ""))
        if not url.startswith("http") or not feed_name:
            continue
        url_key = url.lower().strip()
        if url_key in seen_urls:
            continue
        seen_urls.add(url_key)

        policy_type = _clean_value(row.get(c_policy, "")) if c_policy else ""
        include_kw = _clean_value(row.get(c_include, "")) if c_include else ""
        keyword = include_kw or policy_type or feed_name
        category = (policy_type or "RSS").upper().replace(" ", "_").replace("/", "_")
        importance, importance_score = _importance_from_row(row)

        feeds.append({
            "feed_name": feed_name,
            "url": url,
            "keyword": keyword,
            "category": category,
            "importance": importance,
            "importance_score": importance_score,
            "country": _clean_value(row.get(c_country, "")) if c_country else "",
            "site_type": _clean_value(row.get(c_site_type, "")) if c_site_type else "news",
            "exclude_keywords": _clean_value(row.get(c_exclude, "")) if c_exclude else "",
            "_source_file": str(source_file),
            "_source_sheet": str(sheet_name),
        })

    return feeds


def load_rss_feeds_from_master():
    """Load RSS feeds from gti_master.xlsx first, sites.xlsx fallback.

    v3.3 behavior:
    - Scan all sheets, not just Sheet1.
    - Prefer sheet named site_rss when it exists, but still scan all others.
    - Return first workbook with non-empty feeds.
    """
    candidates = [MASTER_FILE, SITES_FILE_FALLBACK]
    last_error = ""

    for master_path in candidates:
        if not os.path.exists(master_path):
            continue
        try:
            xl = pd.ExcelFile(master_path)
            sheet_order = []
            for preferred in ["site_rss", "rss", "rss_master", "source_master", "sites", "Sheet1"]:
                if preferred in xl.sheet_names and preferred not in sheet_order:
                    sheet_order.append(preferred)
            for s in xl.sheet_names:
                if s not in sheet_order:
                    sheet_order.append(s)

            all_feeds = []
            for sheet in sheet_order:
                try:
                    df = pd.read_excel(master_path, sheet_name=sheet)
                    feeds = _extract_feeds_from_df(df, master_path, sheet)
                    print(f"?뱲 RSS master scan: {master_path} / sheet={sheet} / feeds={len(feeds)}")
                    all_feeds.extend(feeds)
                except Exception as e:
                    print(f"?좑툘 RSS sheet scan failed: {master_path}/{sheet}: {type(e).__name__}: {e}")

            # De-duplicate across sheets.
            out = []
            seen = set()
            for f in all_feeds:
                key = f["url"].lower().strip()
                if key in seen:
                    continue
                seen.add(key)
                out.append(f)
                if len(out) >= MAX_FEEDS_TO_READ:
                    break

            print(f"?뱲 RSS master loaded: {master_path} / total feeds={len(out)}")
            if out:
                return out
            last_error = f"RSS active rows not found in {master_path}"
        except Exception as e:
            last_error = f"{master_path}: {type(e).__name__}: {e}"
            print(f"?좑툘 RSS master load failed: {last_error}")

    if ENABLE_LEGACY_GOOGLE_ALERT_FALLBACK:
        print("?좑툘 RSS legacy fallback enabled, but legacy feed list is intentionally not embedded in v3.3. Check gti_master.xlsx/site_rss.")

    raise RuntimeError(
        "RSS master 濡쒕뱶 ?ㅽ뙣: gti_master.xlsx ?먮뒗 sites.xlsx?먯꽌 active RSS ?됱쓣 李얠? 紐삵뻽?듬땲?? "
        f"last_error={last_error}"
    )


def _sheet_order(xl):
    order = []
    for preferred in ["SITE_RSS", "site_rss", "sites", "Sheet1"]:
        if preferred in xl.sheet_names and preferred not in order:
            order.append(preferred)
    for sheet in xl.sheet_names:
        if sheet not in order:
            order.append(sheet)
    return order


def load_site_news_rows_from_master():
    """Load active non-RSS news site rows for STEP2-3 site news collection."""
    rows = []
    for master_path in [MASTER_FILE, SITES_FILE_FALLBACK]:
        if not os.path.exists(master_path):
            continue
        try:
            xl = pd.ExcelFile(master_path)
            for sheet in _sheet_order(xl):
                try:
                    df = pd.read_excel(master_path, sheet_name=sheet)
                except Exception:
                    continue
                if df.empty:
                    continue
                df.columns = [str(c).strip() for c in df.columns]
                colmap = _norm_col_map(df)
                c_name = _get_col(colmap, "site_name", "feed_name", "source_name", "name", "agency")
                c_url = _get_col(colmap, "url", "source_url", "link")
                if not c_name or not c_url:
                    continue
                c_type = _get_col(colmap, "type", "source_type")
                c_parser = _get_col(colmap, "parser")
                c_active = _get_col(colmap, "active", "use", "enabled", "status")
                c_site_type = _get_col(colmap, "site_type", "track")
                c_policy = _get_col(colmap, "policy_type", "category", "topic")
                c_include = _get_col(colmap, "include_keywords", "keyword", "keywords")
                c_exclude = _get_col(colmap, "exclude_keywords")

                for _, row in df.iterrows():
                    if c_active and not _is_active(row.get(c_active, "")):
                        continue
                    site_type = _clean_value(row.get(c_site_type, "")) if c_site_type else "news"
                    if not _is_news_site_type(site_type):
                        continue
                    if _is_rss_row(row, c_type, c_parser, c_url):
                        continue
                    url = _clean_value(row.get(c_url, ""))
                    name = _clean_value(row.get(c_name, ""))
                    if not url.startswith("http") or not name:
                        continue
                    rows.append({
                        "site_name": name,
                        "url": url,
                        "type": _clean_value(row.get(c_type, "generic")) if c_type else "generic",
                        "parser": _clean_value(row.get(c_parser, "")) if c_parser else "",
                        "site_type": "news",
                        "policy_type": _clean_value(row.get(c_policy, "")) if c_policy else "",
                        "include_keywords": _clean_value(row.get(c_include, "")) if c_include else "",
                        "exclude_keywords": _clean_value(row.get(c_exclude, "")) if c_exclude else "",
                        "_source_file": str(master_path),
                        "_source_sheet": str(sheet),
                    })
            if rows:
                break
        except Exception as e:
            print(f"SITE NEWS master load warning: {master_path} / {type(e).__name__}: {e}")

    seen = set()
    out = []
    for row in rows:
        key = row["url"].lower().strip()
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    print(f"SITE NEWS master loaded: {len(out)}")
    return out


def load_site_crawler_module():
    path = Path(SITE_CRAWLER_FILE)
    if not path.exists():
        print(f"SITE NEWS skipped: crawler file not found: {path}")
        return None
    spec = importlib.util.spec_from_file_location("gti_site_crawler_for_step23", str(path))
    if not spec or not spec.loader:
        print(f"SITE NEWS skipped: cannot import crawler: {path}")
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


try:
    RSS_FEEDS = load_rss_feeds_from_master()
except Exception as e:
    print(f"RSS master load warning: {type(e).__name__}: {e}")
    RSS_FEEDS = []


def collect_site_news():
    """Collect non-RSS site news using the existing STEP1 site crawler functions."""
    site_rows = load_site_news_rows_from_master()
    if not site_rows:
        return pd.DataFrame(columns=FINAL_COLS)

    crawler = load_site_crawler_module()
    if crawler is None:
        return pd.DataFrame(columns=FINAL_COLS)

    # Reset shared state in the imported crawler module.
    crawler.results = []
    crawler.rejects = []

    for row in site_rows:
        source_url = row["url"]
        agency = row["site_name"]
        type_value = str(row.get("type") or "generic").strip().lower()
        try:
            type_value, fixed_parser, site_type = crawler.normalize_source_config(
                source_url,
                type_value,
                row.get("parser", ""),
                "news",
            )
            parser_name = crawler.infer_parser(type_value, fixed_parser)
            print(f"SITE FETCH: {agency} | parser={parser_name} | {source_url}")
            before = len(crawler.results)
            if parser_name == "customs_board":
                count = crawler.crawl_korea(source_url, agency, "news")
            elif parser_name == "ustr_parser":
                count = crawler.crawl_ustr(source_url, agency, "news")
            elif parser_name == "cbp_parser":
                count = crawler.crawl_cbp(source_url, agency, "news")
            elif parser_name == "eu_parser":
                count = crawler.crawl_eu(source_url, agency, "news")
            elif parser_name == "wto_parser":
                count = crawler.crawl_wto(source_url, agency, "news")
            elif parser_name == "card_parser":
                count = crawler.crawl_card(source_url, agency, "news")
            elif parser_name == "motie_parser":
                count = crawler.crawl_motie(source_url, agency, "news")
            elif parser_name == "krcaa_parser":
                count = crawler.crawl_krcaa(source_url, agency, "news")
            elif parser_name == "custra_parser":
                count = crawler.crawl_custra(source_url, agency, "news")
            elif parser_name == "oecd_parser":
                count = crawler.crawl_oecd(source_url, agency, "news")
            elif parser_name == "usitc_parser":
                count = crawler.crawl_usitc(source_url, agency, "news")
            elif parser_name == "nsp_parser":
                count = crawler.crawl_nsp(source_url, agency, "news")
            elif parser_name == "table_date":
                if "fta.motir.go.kr/ftamain/promo/news/trend" in source_url.lower():
                    count = crawler.crawl_fta_trend(source_url, agency, "news")
                else:
                    count = crawler.crawl_table(source_url, agency, "news")
            else:
                count = crawler.crawl_generic(source_url, agency, "news")

            if count == 0:
                count = crawler.crawl_site_hint(pd.Series(row), source_url, agency, "news")
            print(f"   -> {len(crawler.results) - before} rows")
        except Exception as e:
            print(f"   SITE ERROR: {agency}: {type(e).__name__}: {e}")

    if not crawler.results:
        return pd.DataFrame(columns=FINAL_COLS)

    raw = pd.DataFrame(crawler.results)
    if raw.empty:
        return pd.DataFrame(columns=FINAL_COLS)
    raw["date"] = pd.to_datetime(raw.get("date"), errors="coerce")
    raw = raw[raw["date"].notna() & (raw["date"] >= CUT_OFF)].copy()
    if raw.empty:
        return pd.DataFrame(columns=FINAL_COLS)

    rows = pd.DataFrame()
    rows["date"] = raw["date"]
    rows["title"] = raw.get("title", "")
    rows["url"] = raw.get("url", "")
    rows["source"] = raw.get("source", "")
    rows["feed_name"] = raw.get("agency", "")
    rows["summary"] = ""
    rows["collected_at"] = raw.get("collected_at", datetime.now().replace(microsecond=0))
    rows["keyword"] = raw.get("agency", "")
    rows["category"] = "SITE_NEWS"
    rows["importance"] = "MEDIUM"
    rows["importance_score"] = 70
    rows["score_reason"] = "site_news"
    rows["url_type"] = rows["url"].apply(detect_url_type)
    rows["canonical_url"] = rows["url"].apply(canonicalize_url)
    rows["url_decode_status"] = rows["url"].apply(lambda u: "GOOGLE_REMAINED" if is_google_intermediate_url(u) else "RESOLVED_DIRECT")
    rows["source_channel"] = "site_news"
    rows["agency"] = raw.get("agency", "")
    rows["site_type"] = "news"
    for col in FINAL_COLS:
        if col not in rows.columns:
            rows[col] = ""
    return rows[FINAL_COLS]

# ===================== UTILS =====================

def parse_date(d):
    try:
        if not d:
            return None
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            dt = parser.parse(str(d), tzinfos=TZINFOS)
        if getattr(dt, "tzinfo", None):
            dt = dt.replace(tzinfo=None)
        return dt
    except Exception:
        return None


def clean_html(text):
    if text is None:
        return ""
    soup = BeautifulSoup(str(text), "html.parser")
    return re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).strip()


def normalize_text(text):
    return re.sub(r"\s+", " ", clean_html(text)).lower().strip()


def contains_any(text, terms):
    if isinstance(terms, str):
        terms = [x.strip() for x in re.split(r"[,;|]", terms) if x.strip()]
    t = normalize_text(text)
    return any(str(term).lower() in t for term in terms if str(term).strip())


def keyword_equals_any(keyword, terms):
    k = normalize_text(keyword)
    return any(k == normalize_text(term) for term in terms)


def normalize_url(url):
    if not url:
        return ""
    url = str(url).strip()
    url = re.sub(r"#.*$", "", url)
    return url


def canonicalize_url(url):
    if not url:
        return ""
    url = normalize_url(url)
    try:
        parsed = urlparse(url)
        netloc = parsed.netloc.lower()
        path = parsed.path or ""
        if netloc.startswith("m."):
            netloc = netloc[2:]
        if netloc.startswith("amp."):
            netloc = netloc[4:]
        path = re.sub(r"/amp/?$", "", path, flags=re.I)
        path = re.sub(r"(^|/)amp(/|$)", r"\1", path, flags=re.I)
        path = re.sub(r"//+", "/", path)
        qs = parse_qs(parsed.query, keep_blank_values=False)
        drop_params = {
            "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
            "utm_id", "gclid", "fbclid", "igshid", "mc_cid", "mc_eid", "output",
            "amp", "amp_js_v", "usqp", "feed", "partner",
        }
        kept = []
        for key in sorted(qs):
            if key.lower() in drop_params:
                continue
            for value in qs[key]:
                kept.append((key, value))
        query = urlencode(kept, doseq=True)
        return urlunparse((parsed.scheme.lower() or "https", netloc, path, "", query, ""))
    except Exception:
        return url.lower()


def detect_url_type(url):
    host = urlparse(str(url)).netloc.lower()
    path = urlparse(str(url)).path.lower()
    if "youtube.com" in host or "youtu.be" in host:
        return "youtube"
    if "amp" in host or "/amp" in path or path.endswith("/amp"):
        return "amp"
    if host.startswith("m."):
        return "mobile"
    return "article"


def decode_google_redirect(url):
    if not url:
        return ""
    url = str(url).strip()
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        for key in ["url", "q"]:
            if key in qs and qs[key]:
                candidate = unquote(str(qs[key][0])).strip()
                if candidate.startswith("http"):
                    return normalize_url(candidate)
    except Exception:
        pass
    return normalize_url(url)



# ======================================================================
# GTI FINAL CORE v5 - Selenium Google URL recovery
# 브라우저에서 Google News/Alert 링크를 누르면 원문이 열리는 케이스를
# headless Chrome으로 실제 열어 driver.current_url을 확보한다.
# ======================================================================
SELENIUM_GOOGLE_RESOLVE_ENABLED = os.getenv("GTI_SELENIUM_GOOGLE_RESOLVE", "1").strip().upper() not in {"0", "N", "NO", "FALSE"}
SELENIUM_GOOGLE_TIMEOUT = int(os.getenv("GTI_SELENIUM_GOOGLE_TIMEOUT", "20"))


def is_google_intermediate_url(value):
    u = str(value or "").lower().strip()
    return (
        "news.google.com" in u
        or "google.co.kr/alerts/feeds" in u
        or "google.com/alerts/feeds" in u
        or "google.co.kr/url?" in u
        or "google.com/url?" in u
    )


def resolve_google_url_by_selenium(url, timeout=None):
    """Return (resolved_url, status). Uses headless Chrome as final fallback."""
    u = str(url or "").strip()
    if not u:
        return "", "EMPTY_URL"
    if not is_google_intermediate_url(u):
        return u, "NOT_GOOGLE_URL"
    if not SELENIUM_GOOGLE_RESOLVE_ENABLED:
        return u, "SELENIUM_DISABLED"
    timeout = int(timeout or SELENIUM_GOOGLE_TIMEOUT)
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait

        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1365,900")
        options.add_argument("--lang=ko-KR")
        options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        )

        driver = webdriver.Chrome(options=options)
        try:
            driver.set_page_load_timeout(timeout)
            driver.get(u)

            def moved_away_from_google(d):
                cur = (d.current_url or "").lower()
                return bool(cur) and not is_google_intermediate_url(cur)

            try:
                WebDriverWait(driver, timeout).until(moved_away_from_google)
            except Exception:
                pass
            final_url = (driver.current_url or "").strip()
        finally:
            driver.quit()

        if final_url and not is_google_intermediate_url(final_url):
            return final_url, "RESOLVED_SELENIUM"
        return final_url or u, "GOOGLE_REMAINED"
    except Exception as exc:
        return u, f"SELENIUM_FAILED:{type(exc).__name__}"


def extract_title(entry):
    return clean_html(entry.get("title", ""))


def extract_summary(entry):
    return clean_html(entry.get("summary", entry.get("description", "")))


def extract_url(entry):
    # 1. Google Alert summary/internal HTML links first
    try:
        soup = BeautifulSoup(entry.get("summary", ""), "html.parser")
        for a in soup.find_all("a"):
            href = a.get("href", "")
            if href.startswith("http"):
                return decode_google_redirect(href)
    except Exception:
        pass

    # 2. entry link fallback
    link = entry.get("link", "")
    if not link:
        # Atom feeds may use links list.
        try:
            for l in entry.get("links", []) or []:
                href = l.get("href", "")
                if href:
                    link = href
                    break
        except Exception:
            pass
    return decode_google_redirect(link)



def extract_url_with_status(entry):
    raw = extract_url(entry)
    if not raw:
        return "", "EMPTY_URL"
    if is_google_intermediate_url(raw):
        resolved, status = resolve_google_url_by_selenium(raw, timeout=SELENIUM_GOOGLE_TIMEOUT)
        if resolved and not is_google_intermediate_url(resolved):
            return normalize_url(resolved), status
        return normalize_url(raw), status or "GOOGLE_REMAINED"
    return normalize_url(raw), "RESOLVED_DIRECT"

# ===================== SCORE CONTROL =====================

STRONG_KEEP_TERMS = [
    "section 301", "301조", "tariff ceiling", "tariff cap", "관세상한",
    "cepa", "forced labor tariff", "forced labor", "uflpa", "tariff", "tariffs",
    "customs duty", "export control", "entity list", "anti-dumping", "antidumping",
    "countervailing", "ad/cvd", "관세", "수출통제", "반덤핑", "상계관세", "무역구제",
]

AEO_SUPPORT_CONTEXT = [
    "customs", "authorized economic operator", "mutual recognition", "mra",
    "관세청", "통관", "수출입안전관리", "종합인증우수업체", "인증수출자",
]

AEO_NOISE_CONTEXT = [
    "american eagle", "aeo inc", "aeo stock", "aeo shares", "aeo 二쇨?",
    "retailer", "apparel", "?섎쪟", "?⑥뀡", "二쇱떇", "利앷텒",
]


def adjust_importance_score(keyword, title, summary, url, base_score):
    text = f"{title} {summary}"
    score = int(base_score or 50)
    reasons = []
    url_type = detect_url_type(url)

    if contains_any(text, STRONG_KEEP_TERMS):
        score += 30
        reasons.append("strong_policy_signal")

    if keyword_equals_any(keyword, ["aeo"]) or contains_any(text, ["aeo"]):
        if contains_any(text, AEO_NOISE_CONTEXT):
            score -= 60
            reasons.append("aeo_noise_penalty")
        elif not contains_any(text, AEO_SUPPORT_CONTEXT):
            score -= 35
            reasons.append("aeo_without_customs_context")

    if url_type == "youtube":
        score -= 45
        reasons.append("youtube_low_priority")

    return max(0, min(score, 150)), ", ".join(reasons) or "base", url_type


# ===================== STEP2-3 FINAL NEWS QUALITY GATE =====================

TRADE_NEWS_CONTEXT_TERMS = [
    "customs", "tariff", "duty", "duties", "trade", "import", "export", "origin",
    "fta", "cepa", "epa", "hs code", "classification", "valuation", "drawback",
    "anti-dumping", "antidumping", "countervailing", "safeguard", "section 301",
    "section 232", "export control", "entity list", "uflpa", "forced labor",
    "cbam", "carbon border", "quota", "sanction",
    "\uad00\uc138", "\ud1b5\uad00", "\ud1b5\uc0c1", "\ubb34\uc5ed", "\uc218\uc785", "\uc218\ucd9c",
    "\uc6d0\uc0b0\uc9c0", "\ud488\ubaa9\ubd84\ub958", "\uc138\uc728", "\ud658\uae09", "\ubcf4\uc138",
    "\ubc18\ub364\ud551", "\uc0c1\uacc4\uad00\uc138", "\ub364\ud551\ubc29\uc9c0\uad00\uc138",
    "\uc218\ucd9c\ud1b5\uc81c", "\uac15\uc81c\ub178\ub3d9", "\ud560\ub2f9\uad00\uc138",
]

NEWS_NOISE_TERMS = [
    "stock price", "shares", "share price", "market cap", "crypto", "bitcoin",
    "coin", "earnings preview", "earnings outlook", "investment recommendation",
    "youtube", "vod", "video", "shorts", "podcast",
    "\uc8fc\uac00", "\uc2dc\ucd1d", "\ucf54\uc778", "\ube44\ud2b8\ucf54\uc778", "\uc554\ud638\ud654\ud3d0",
    "\uc99d\uad8c", "\ud22c\uc790\uc758\uacac", "\ubaa9\ud45c\uc8fc\uac00", "\uc2e4\uc801 \uc804\ub9dd",
    "\uc601\uc0c1", "\uc720\ud29c\ube0c", "\uc1fc\uce20",
]

NEWS_FORCE_KEEP_TERMS = [
    "tariff", "customs", "anti-dumping", "countervailing", "export control",
    "section 301", "section 232", "fta", "cbam",
    "\uad00\uc138", "\ud1b5\uad00", "\ubc18\ub364\ud551", "\uc0c1\uacc4\uad00\uc138",
    "\uc218\ucd9c\ud1b5\uc81c", "\uc6d0\uc0b0\uc9c0", "\ud488\ubaa9\ubd84\ub958",
]


def _news_text(row):
    return " ".join(
        _clean_value(row.get(c, ""))
        for c in ["title", "headline", "summary", "keyword", "category", "feed_name", "agency", "source"]
    ).lower()


def has_trade_news_context(row):
    return any(term.lower() in _news_text(row) for term in TRADE_NEWS_CONTEXT_TERMS)


def is_news_noise(row):
    text = _news_text(row)
    title = _clean_value(row.get("title", "") or row.get("headline", "")).lower()
    title_noise = any(term.lower() in title for term in NEWS_NOISE_TERMS)
    title_force = any(term.lower() in title for term in NEWS_FORCE_KEEP_TERMS)
    if title_noise and not title_force:
        return True
    if _clean_value(row.get("url_type", "")).lower() == "youtube" and not title_force:
        return True
    if not any(term.lower() in text for term in NEWS_NOISE_TERMS):
        return False
    return not any(term.lower() in text for term in NEWS_FORCE_KEEP_TERMS)


def require_headline_url(df, context):
    if df is None or df.empty:
        return df.copy() if df is not None else pd.DataFrame(columns=FINAL_COLS)
    work = df.copy()
    if "headline" not in work.columns:
        work["headline"] = work.get("title", "")
    else:
        blank = work["headline"].fillna("").astype(str).str.strip().eq("")
        if "title" in work.columns:
            work.loc[blank, "headline"] = work.loc[blank, "title"]
    mask = (
        work["title"].fillna("").astype(str).str.strip().ne("")
        & work["url"].fillna("").astype(str).str.startswith("http")
    )
    removed = int((~mask).sum())
    if removed:
        print(f"QUALITY {context}: removed without title/url = {removed}")
    return work[mask].copy()


def filter_trade_news(df, context):
    if df is None or df.empty:
        return df.copy() if df is not None else pd.DataFrame(columns=FINAL_COLS)
    work = require_headline_url(df, context)
    before = len(work)
    context_mask = work.apply(has_trade_news_context, axis=1)
    noise_mask = work.apply(is_news_noise, axis=1)
    out = work[context_mask & ~noise_mask].copy()
    print(
        f"QUALITY {context}: trade_context {before}->{int(context_mask.sum())}, "
        f"noise_removed={int((context_mask & noise_mask).sum())}, kept={len(out)}"
    )
    return out


def load_step1_site_news_compat():
    """Import 1-2.site_news_raw.xlsx into STEP2-3 as compatibility site-news input."""
    if not os.path.exists(STEP1_SITE_NEWS_FILE):
        return pd.DataFrame(columns=FINAL_COLS)
    try:
        raw = pd.read_excel(STEP1_SITE_NEWS_FILE)
    except Exception as exc:
        print(f"STEP1 site-news compat read failed: {STEP1_SITE_NEWS_FILE} / {type(exc).__name__}: {exc}")
        return pd.DataFrame(columns=FINAL_COLS)
    if raw.empty:
        return pd.DataFrame(columns=FINAL_COLS)
    out = pd.DataFrame()
    out["date"] = raw.get("date", "")
    out["title"] = raw.get("headline", raw.get("title", ""))
    out["headline"] = out["title"]
    out["url"] = raw.get("url", "")
    out["source"] = raw.get("source", "")
    out["feed_name"] = raw.get("agency", "STEP1 site news")
    out["summary"] = raw.get("regulation_fallback_body", raw.get("summary", ""))
    out["collected_at"] = raw.get("collected_at", datetime.now().replace(microsecond=0))
    out["keyword"] = raw.get("matched_policy_terms", raw.get("keyword", ""))
    out["category"] = "STEP1_SITE_NEWS"
    out["importance"] = "MEDIUM"
    out["importance_score"] = pd.to_numeric(raw.get("policy_score", 70), errors="coerce").fillna(70).clip(0, 150)
    out["score_reason"] = raw.get("law1_news_reason", "step1_site_news_compat")
    out["url_type"] = out["url"].apply(detect_url_type)
    out["canonical_url"] = out["url"].apply(canonicalize_url)
    out["url_decode_status"] = out["url"].apply(lambda u: "GOOGLE_REMAINED" if is_google_intermediate_url(u) else "RESOLVED_DIRECT")
    out["source_channel"] = "step1_site_news"
    out["agency"] = raw.get("agency", "")
    out["site_type"] = "news"
    for col in FINAL_COLS:
        if col not in out.columns:
            out[col] = ""
    out = out[FINAL_COLS]
    out = filter_trade_news(out, "step1_site_news_compat")
    print(f"STEP1 site-news compat loaded: {len(out)} / source={STEP1_SITE_NEWS_FILE}")
    return out

# ===================== COLLECT =====================

def _entry_date(entry):
    for key in ["published", "updated", "created", "pubDate"]:
        dt = parse_date(entry.get(key, ""))
        if dt:
            return dt
    try:
        if getattr(entry, "published_parsed", None):
            return datetime(*entry.published_parsed[:6])
    except Exception:
        pass
    try:
        if getattr(entry, "updated_parsed", None):
            return datetime(*entry.updated_parsed[:6])
    except Exception:
        pass
    return None


def collect():
    rows = []
    for feed_info in RSS_FEEDS:
        feed_name = feed_info["feed_name"]
        feed_url = feed_info["url"]
        exclude_keywords = feed_info.get("exclude_keywords", "")

        print("FETCH:", feed_name, "|", feed_url)
        try:
            feed = feedparser.parse(feed_url)
        except Exception as exc:
            print(f"   [RSS SKIP] fetch/parse failed: {type(exc).__name__}: {exc}")
            continue
        if getattr(feed, "bozo", False):
            # Keep going; many feeds are bozo but still usable.
            print(f"   [RSS WARN] feed parse warning: {type(getattr(feed, 'bozo_exception', None)).__name__}")

        for e in (feed.entries or [])[:MAX_ITEMS_PER_FEED]:
            title = extract_title(e)
            if not title:
                continue

            dt = _entry_date(e)
            if not dt:
                # RSS without date cannot be safely included in 24h daily radar.
                continue
            if dt < CUT_OFF:
                continue

            summary = extract_summary(e)
            if exclude_keywords and contains_any(f"{title} {summary}", exclude_keywords):
                continue

            url, url_decode_status = extract_url_with_status(e)
            base_score = feed_info.get("importance_score", 50)
            importance_score, score_reason, url_type = adjust_importance_score(
                feed_info.get("keyword", ""), title, summary, url, base_score
            )
            canonical_url = canonicalize_url(url)

            rows.append({
                "date": dt,
                "title": title,
                "url": url,
                "source": feed_url,
                "feed_name": feed_name,
                "summary": summary,
                "collected_at": datetime.now().replace(microsecond=0),
                "keyword": feed_info.get("keyword", ""),
                "category": feed_info.get("category", ""),
                "importance": feed_info.get("importance", ""),
                "importance_score": importance_score,
                "score_reason": score_reason,
                "url_type": url_type,
                "canonical_url": canonical_url,
                "url_decode_status": url_decode_status,
                "source_channel": "rss",
                "agency": feed_name,
                "site_type": "news",
            })

    return pd.DataFrame(rows)

# ===================== DEDUP =====================

def dedup(df):
    before = len(df)
    df["url"] = df["url"].apply(normalize_url)
    df["canonical_url"] = df["url"].apply(canonicalize_url)
    df["url_key"] = df["canonical_url"].astype(str).str.strip().str.lower()
    df["title_key"] = (
        df["title"].astype(str)
        .str.lower()
        .str.replace(r"\s+-\s+[^-]+$", "", regex=True)
        .str.replace(r"[^\w\s]", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    df = df.sort_values(["importance_score", "date"], ascending=[False, False])
    df_url = df[df["url_key"] != ""].drop_duplicates(subset=["url_key"], keep="first")
    df_no_url = df[df["url_key"] == ""].drop_duplicates(subset=["title_key"], keep="first")
    out = pd.concat([df_url, df_no_url], ignore_index=True)
    out = out.drop(columns=["url_key", "title_key"], errors="ignore")
    print(f"?뱤 DEDUP URL/TITLE: {before} -> {len(out)}")
    return out

# ===================== MAIN =====================

def main():
    print(f"LOOKBACK_HOURS={LOOKBACK_HOURS}")
    site_df = collect_site_news()
    step1_site_df = load_step1_site_news_compat()
    rss_df = collect()
    site_df = filter_trade_news(site_df, "site_news")
    rss_df = filter_trade_news(rss_df, "rss")
    df = pd.concat([site_df, step1_site_df, rss_df], ignore_index=True, sort=False)
    print("Collected site_news:", len(site_df))
    print("Collected step1_site_news:", len(step1_site_df))
    print("Collected rss:", len(rss_df))
    print("Collected total:", len(df))

    if df.empty:
        print("??NO DATA")
        pd.DataFrame(columns=FINAL_COLS).to_excel(OUTPUT_FILE, index=False)
        print("?뮶 saved empty file:", OUTPUT_FILE)
        print("?뱄툘 If this is unexpected, check that gti_master.xlsx has Google Alert RSS rows in any sheet and active=Y/1.")
        return

    df = dedup(df)
    df = df.sort_values(["importance_score", "date"], ascending=[False, False]).head(300)
    df = require_headline_url(df, "final_output")
    for col in FINAL_COLS:
        if col not in df.columns:
            df[col] = ""
    df = df[FINAL_COLS]
    df.to_excel(OUTPUT_FILE, index=False)

    print("?뱚 SAVED:", OUTPUT_FILE)
    print("??DONE:", len(df))


if __name__ == "__main__":
    main()

