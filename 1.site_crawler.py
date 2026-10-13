# -*- coding: utf-8 -*-
r"""
GTI STEP1 FINAL COMPLETE - sites.xlsx 운영형

Input:
- C:\temp\sites.xlsx

Output:
- C:\temp\1.site_news_raw.xlsx        : 최종 유효 게시물
- C:\temp\1.site_news_audit.xlsx      : 전체 수집/진단 원본
- C:\temp\1-1.regulation_raw.xlsx     : 법규 / 공식 정부문서

Output columns:
date / title / url / source / collected_at / agency / site_type / date_status

Classification rule:
- site_type = regulation → 정부/공식기관 원문 문서
- site_type = news       → 보도자료/뉴스/기관소식/예외 포함

Search period:
- HOURS_BACK = 72
"""

import json
import os
import re
import time
import warnings
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import URLError
from urllib.parse import parse_qs, quote, unquote, urlencode, urljoin, urlparse
from urllib.request import Request, urlopen

import pandas as pd
import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except Exception:
    SELENIUM_AVAILABLE = False


BASE_DIR = Path(os.getenv("GTI_BASE_DIR", r"C:\temp"))
SITE_FILE = BASE_DIR / "sites.xlsx"

OUT_ALL_FILE = BASE_DIR / "1.site_news_raw.xlsx"
OUT_AUDIT_FILE = BASE_DIR / "1.site_news_audit.xlsx"
OUT_REG_FILE = BASE_DIR / "1-1.regulation_raw.xlsx"
REJECT_FILE = BASE_DIR / "1.site_news_reject_debug.xlsx"
FINAL_EXCLUDED_FILE = BASE_DIR / "1.site_news_final_excluded.xlsx"
POLICY_CANDIDATES_FILE = BASE_DIR / "1.site_policy_candidates.xlsx"
CRAWL_HEALTH_FILE = BASE_DIR / "1.site_crawl_health.xlsx"

HOURS_BACK = max(24, int(os.getenv("GTI_REGULATION_HOURS_BACK", "72")))
OVERSEAS_HOURS_BACK = max(24, int(os.getenv("GTI_OVERSEAS_HOURS_BACK", "168")))
MAX_PER_SITE = 30
MAX_GWANBO_ITEMS = 250
LAWMAKING_MAX_PAGES = max(1, int(os.getenv("GTI_LAWMAKING_MAX_PAGES", "20")))
# Set to 30 for a one-time recovery run. Zero keeps the normal 72-hour window.
LAWMAKING_BACKFILL_DAYS = max(0, int(os.getenv("GTI_LAWMAKING_BACKFILL_DAYS", "0")))
SLEEP_SEC = 0.5

results = []
rejects = []
crawl_health = []
site_run_status = {}
site_coverage_status = {}

BAD_TITLE_CONTAINS = [
    "로그인", "회원가입", "사이트맵", "skip", "menu", "home",
    "privacy", "cookie", "contact", "about us", "accessibility",
    "facebook", "twitter", "youtube", "instagram", "linkedin",
    "검색", "전체메뉴", "본문", "바로가기", "이전", "다음",
    "처음", "마지막", "다운로드", "첨부파일", "자주묻는질문", "faq",
    "네이버 블로그", "블로그", "blog.naver.com",
    "관보보기", "일자별 기간별", "마이페이지", "관심 관보",
    "발행예고보기", "내일관보", "관보분석",
    "등록·채용 신고", "관세사 · 법인 징계현황",
    "개인정보처리방침", "이메일무단수집거부",
    "copyright", "sitemap", "language", "print", "share",
    "Policies, Procedures and Directives",
    "General Aviation Airport Fact Sheets",
    "Media Releases",
    "Announcements",
    "Spotlights",
    "Press Officers",
    "Social Media Directory",
    "Accountability and Transparency",
    "FOIA Reading Room",
    "Stats and Summaries",
    "Documents Library",
    "Legal Notices",
    "Frontline Digital Magazine",
    "Comunicados de Prensa",
    "Publications Catalog",
    "최초 사용자", "가이드", "공직자 재산공개", "병역사항 공개",
    "관인", "목차 다운로드", "전체 다운로드", "타임스탬프",
]

BAD_TITLE_EXACT = {
    "", "-", "0", "new", "more", "보기", "상세보기", "검색",
    "공지사항", "보도자료", "고시", "공고", "훈령", "예규",
    "뉴스", "news", "home", "menu", "목록", "english", "한국어",
    "media releases", "announcements", "spotlights", "press officers",
    "directorates", "helpdesk", "feedback", "website policy",
}

TRADE_WORDS = [
    "관세", "통관", "수입", "수출", "무역", "통상", "고시", "공고",
    "훈령", "예규", "입법예고", "행정예고", "FTA", "원산지",
    "customs", "tariff", "trade", "import", "export", "notice",
    "regulation", "announcement", "directive", "policy",
    "news", "press", "release", "commission", "investigation",
]


def clean_text(value: str) -> str:
    if value is None:
        return ""
    value = unescape(str(value))
    value = value.replace("\xa0", " ").replace("\u200b", "")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_url_for_compare(value: str) -> str:
    """Normalize URL enough to compare list page/source URLs with article URLs."""
    if value is None:
        return ""

    value = str(value).strip()
    if not value:
        return ""

    parsed = urlparse(value)
    path = parsed.path.rstrip("/")
    query = parsed.query

    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}?{query}".rstrip("?")


def is_active_value(value):
    if pd.isna(value):
        return False
    if isinstance(value, (int, float)):
        return int(value) == 1
    return str(value).strip().lower() in [
        "1", "1.0", "y", "yes", "true", "t", "사용", "active"
    ]


def normalize_site_type(value):
    v = clean_text(value).lower()
    if v in ["regulation", "reg", "law", "official", "official_government"]:
        return "regulation"
    return "news"


def normalize_date(value):
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    m = re.search(r"令和\s*(\d{1,2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", s)
    if m:
        try:
            return datetime(int(m.group(1)) + 2018, int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    m = re.search(r"(20\d{2})\s*[年년]\s*(\d{1,2})\s*[月월]\s*(\d{1,2})\s*[日일]", s)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    m = re.fullmatch(r"\s*(\d{1,2})[./-](\d{1,2})[./-](20\d{2})(?:\s+.*)?\s*", s)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        if hasattr(dt, "to_pydatetime"):
            dt = dt.to_pydatetime()
        if getattr(dt, "tzinfo", None):
            dt = dt.replace(tzinfo=None)
        return dt
    except Exception:
        return None


def parse_site_recent_date(value):
    dt = normalize_date(value)
    if dt:
        return dt

    s = clean_text(value)
    if not s:
        return None

    m = re.search(r"(\d{1,2})\s*월\s*(\d{1,2})\s*일", s)
    if m:
        now = datetime.now()
        try:
            return datetime(now.year, int(m.group(1)), int(m.group(2)))
        except ValueError:
            return None

    return None


def extract_date_from_text(text):
    text = clean_text(text)
    now = datetime.now()

    patterns = [
        r"(20\d{2}[-/.]\s*\d{1,2}[-/.]\s*\d{1,2}\s+\d{1,2}:\d{2}(:\d{2})?)",
        r"(20\d{2}[-/.]\s*\d{1,2}[-/.]\s*\d{1,2})",
        r"(20\d{2}\.\s*\d{1,2}\.\s*\d{1,2}\.?)",
        r"(20\d{2}년\s*\d{1,2}월\s*\d{1,2}일)",
        r"(20\d{2}年\s*\d{1,2}月\s*\d{1,2}日)",
        r"(令和\s*\d{1,2}年\s*\d{1,2}月\s*\d{1,2}日)",
        r"(\d{1,2}[./-]\d{1,2}[./-]20\d{2})",
        r"([A-Z][a-z]{2,9}\s+\d{1,2},\s*20\d{2})",
        r"([A-Z][a-z]{2,9}\s+\d{1,2}\s+20\d{2})",
        r"(\d{1,2}\s+[A-Z][a-z]{2,9}\s+20\d{2})",
        r"(\d{1,2}\s+[A-Z][a-z]{2,9},?\s+20\d{2})",
        r"(\d{1,2}-[A-Z][a-z]{2}-\d{2,4})",
        r"(\d{1,2}-[A-Z][a-z]{2,9}-\d{2,4})",
    ]

    for p in patterns:
        m = re.search(p, text)
        if not m:
            continue
        s = m.group(1)
        s = s.replace("년", "-").replace("월", "-").replace("일", "")
        s = s.replace(".", "-").replace("/", "-")
        s = re.sub(r"-+$", "", s)
        dt = normalize_date(s)
        if dt:
            return dt

    m = re.search(r"(\d{1,2})\s*월\s*(\d{1,2})\s*일", text)
    if m:
        try:
            return datetime(now.year, int(m.group(1)), int(m.group(2)))
        except Exception:
            return None

    m = re.search(r"(^|\s)(\d{1,2})[./-](\d{1,2})(\s|$)", text)
    if m:
        try:
            return datetime(now.year, int(m.group(2)), int(m.group(3)))
        except Exception:
            return None

    return None


def extract_date_from_tag(tag):
    if tag is None:
        return None

    time_tag = tag.find("time")
    if time_tag:
        for attr in ["datetime", "content", "date"]:
            if time_tag.get(attr):
                dt = normalize_date(time_tag.get(attr))
                if dt:
                    return dt
        dt = extract_date_from_text(time_tag.get_text(" ", strip=True))
        if dt:
            return dt

    for key in [
        "article:published_time", "article:modified_time",
        "date", "dc.date", "dc:date", "pubdate",
        "publishdate", "published_time", "lastmod",
    ]:
        meta = (
            tag.find("meta", attrs={"property": key})
            or tag.find("meta", attrs={"name": key})
            or tag.find("meta", attrs={"itemprop": key})
        )
        if meta and meta.get("content"):
            dt = normalize_date(meta.get("content"))
            if dt:
                return dt

    return extract_date_from_text(tag.get_text(" ", strip=True))


def find_date_near_anchor(a):
    cur = a
    for _ in range(8):
        if cur is None:
            break
        text = clean_text(cur.get_text(" ", strip=True))
        dt = extract_date_from_text(text)
        if dt:
            return dt
        cur = cur.find_parent(["tr", "li", "div", "article", "section"])
    return None


def is_overseas_agency(agency):
    t = clean_text(agency).lower()
    overseas_hints = [
        "india", "dgft", "cbic", "japan", "customs(일본)", "vietnam",
        "mofcom", "gacc", "taxud", "eu ", "federal register", "ustr",
        "us cbp", "usitc", "brazil", "receita", "eec", "eurasian",
    ]
    return any(x in t for x in overseas_hints)


def is_recent(dt, agency=""):
    if dt is None:
        return False
    if isinstance(dt, date) and not isinstance(dt, datetime):
        dt = datetime.combine(dt, datetime.min.time())
    now = datetime.now()
    # Most government boards expose only a calendar date, not a posting time.
    # Treat HOURS_BACK as an inclusive day window so a 72-hour setting includes
    # the whole day 3 days ago, e.g. May 29 run includes all of May 26.
    hours_back = OVERSEAS_HOURS_BACK if is_overseas_agency(agency) else HOURS_BACK
    days_back = max(0, int(hours_back // 24))
    start_date = (now.date() - timedelta(days=days_back))
    return start_date <= dt.date() <= now.date()


def is_valid_title(title):
    title = clean_text(title)
    low = title.lower()

    if not title:
        return False
    if low in BAD_TITLE_EXACT:
        return False
    if len(title) < 8:
        return False
    if re.fullmatch(r"\d+", title):
        return False
    if any(x.lower() in low for x in BAD_TITLE_CONTAINS):
        return False
    return True


def is_menu_or_category_link(title, url, source=""):
    title = clean_text(title)
    low = title.lower()
    url_l = str(url or "").lower()
    source_l = str(source or "").lower()

    # USTR articles live below /about/.../press-releases/; /about/ alone is
    # normally navigational, but a concrete press-release detail URL is not.
    is_ustr_press_detail = "/press-releases/" in url_l and url_l != source_l

    if not title:
        return True
    if low.startswith(("http://", "https://", "www.")):
        return True
    if len(title) <= 7 and re.fullmatch(r"[a-z]{2}(\s+[a-z].*)?", low):
        return True

    menu_titles = {
        "more",
        "more news",
        "rss news feeds",
        "news & events",
        "wto news",
        "fta 강국,korea",
        "fta 이행·활용·대책",
        "지역별 통상진흥센터",
        "이행 관련 보고서",
        "복수국간협상개관",
        "미국무역대표부(ustr)",
        "business, economy, euro",
        "directorate-general for taxation and customs union",
        "application of eu law",
        "eu funding for customs and tax",
        "tax transparency and cooperation",
        "value added tax (vat)",
        "national tax administrations",
        "customs procedures for import and export",
        "eu trade relationships by country/region",
        "development and sustainability",
        "enforcement and protection",
        "foreign-trade zones board",
        "international trade administration",
        "industry and security bureau",
        "business & industry",
        "회원맞춤형서비스",
        "directorates",
        "helpdesk",
        "feedback",
        "website policy",
    }
    if low in menu_titles:
        return True

    menu_contains = [
        "sl sloven",
        "uk y",
        "more news",
        "language",
        "accessibility",
        "privacy",
        "cookie",
        "sitemap",
        "contact us",
        "about us",
    ]
    if any(x in low for x in menu_contains):
        return True

    menu_url_parts = [
        "/about/",
        "/topic/",
        "/topics/",
        "/development-topics",
        "/agencies/",
        "/business-and-industry",
        "/news_sl",
        "/news_uk",
        "/ftamain/kfta/",
        "/ftamain/apply/",
        "/ftamain/trade/",
        "/ftamain/promo/mag/",
        "/news/mediarusources/",
        "/statistics/statistics",
        "/legislation/laws",
        "/service/",
        "/statics/report/",
        "/eu-trade-relationships-country-and-region",
        "/help-exporters-and-importers",
        "/national-customs-administrations",
        "/customs/customs-procedures-import-and-export",
        "/pt-br/servicos/",
        "/pt-br/assuntos/aduana/",
    ]
    if any(x in url_l for x in menu_url_parts) and not is_ustr_press_detail:
        return True

    if url_l == source_l:
        return True

    return False


def has_title_date_conflict(title, dt):
    """Reject a rescued page date when the title clearly identifies an older period."""
    if dt is None:
        return False
    years = {int(x) for x in re.findall(r"(?<!\d)(20\d{2})(?!\d)", clean_text(title))}
    if not years or dt.year in years:
        return False
    return max(years) < dt.year - 1


def has_stale_explicit_title_year(title, reference=None):
    """A no-date item cannot become an urgent candidate when its title is clearly historical."""
    reference = reference or datetime.now()
    years = {int(x) for x in re.findall(r"(?<!\d)(20\d{2})(?!\d)", clean_text(title))}
    return bool(years) and max(years) < reference.year - 1


def extract_official_url_date(url):
    """Extract dates only from stable date-bearing official document URL patterns."""
    value = str(url or "")
    patterns = [
        # Federal Register: /documents/2026/10/08/...
        r"/documents/(20\d{2})/(\d{1,2})/(\d{1,2})/",
        # METI legacy: /english/press/2024/0827_003.html
        r"/press/(20\d{2})/(\d{2})(\d{2})(?:[_./-]|$)",
        # METI current: /english/press/2026/10/20261008002.html
        r"/press/20\d{2}/\d{1,2}/(20\d{2})(\d{2})(\d{2})\d*\.html",
        # Japan MOF notices: ka20260804_e.html, ka20260724-2_e.html
        r"(?:^|/)[a-z]*?(20\d{2})(\d{2})(\d{2})(?:[-_][^/]*)?\.html",
    ]
    for pattern in patterns:
        match = re.search(pattern, value, re.I)
        if not match:
            continue
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            continue
    return None


def has_url_date_conflict(url, dt):
    """Block a listing date copied onto an official document with another URL date."""
    if dt is None:
        return False
    url_date = extract_official_url_date(url)
    if url_date is None:
        return False
    return url_date != dt.date() or url_date > datetime.now().date()


def has_stale_official_url_date(url, reference=None):
    """Do not escalate an undated document when its official URL proves it is old."""
    url_date = extract_official_url_date(url)
    if url_date is None:
        return False
    reference = reference or datetime.now()
    days_back = max(0, int(OVERSEAS_HOURS_BACK // 24))
    return url_date < (reference.date() - timedelta(days=days_back))


def is_policy_content_candidate(title):
    """Keep policy/change signals; preserve the rest in excluded/audit workbooks."""
    text = clean_text(title)
    if not text:
        return False
    non_policy = re.compile(
        r"training|course|seminar|workshop|summit|conference|webinar|tender|"
        r"award|charity|donation|ceremony|inaugurat|statistics?|trade figures?|"
        r"sơ bộ tình hình|khánh thành|bàn giao|đào tạo|hội nghị|hội thảo|"
        r"laudo pericial|coverage of|community portal",
        re.I,
    )
    if non_policy.search(text):
        return False
    enforcement_news = re.compile(
        r"border patrol|coast guard|cocaine|meth(?:amphetamine)?|marijuana|"
        r"seiz(?:e|ed|ure)|smuggl|maritime border crossing|recruiting event",
        re.I,
    )
    if enforcement_news.search(text):
        return False
    section_337_case = (
        re.match(r"certain\s+", text, re.I)
        and re.search(r"investigation|commission determination|initial determination|section 337", text, re.I)
        and not re.search(r"anti[- ]?dumping|countervailing|customs duty|tariff|five[- ]year review", text, re.I)
    )
    if section_337_case:
        return False
    # Pure tax-administration/VAT items are outside the customs-policy radar.
    # Keep them only when the title also carries a customs/trade-border signal.
    tax_only = re.search(r"\bvat\b|tax administration|tax fraud|tax jurisdiction", text, re.I)
    customs_scope = re.search(
        r"customs|tariff|dut(?:y|ies)|import|export|border|cbam|origin|trade remedy|"
        r"관세|통관|수입|수출|원산지|세관|"
        r"hải quan|xuất khẩu|nhập khẩu|thuế quan",
        text,
        re.I,
    )
    if tax_only and not customs_scope:
        return False
    policy_change = re.compile(
        r"tariff|customs reform|customs procedure|customs duty|import duty|export duty|"
        r"handling fee|customs guidance|"
        r"anti[- ]?dumping|countervailing|safeguard|rules? of origin|"
        r"tariff.rate quota|\btrq\b|quota|allocation of quantity|"
        r"five[- ]year reviews?|institution of .*reviews?|determinations?|"
        r"regulation|directive|implementing act|amend(?:ment|ed|s)?|adopt(?:ed|s)?|"
        r"propos(?:al|es|ed)|consultation|notice|order|decree|law|legal text|"
        r"관세|통관|원산지|수출통제|무역구제|덤핑|상계|세이프가드|고시|공고|"
        r"규정|개정|제정|시행|입법|행정예고|"
        r"quy định|thủ tục|nghị định|thông tư|sửa đổi|ban hành|phân cấp|"
        r"gia công|sản xuất xuất khẩu|cửa quốc gia|phòng vệ thương mại",
        re.I,
    )
    return bool(policy_change.search(text))


def add_reject(reason, date_value, title, url, source, agency, site_type=""):
    rejects.append({
        "reason": reason,
        "date": str(date_value),
        "title": clean_text(title),
        "url": str(url),
        "source": str(source),
        "agency": str(agency),
        "site_type": site_type,
        "checked_at": now_str(),
    })


def add_result(date_value, title, url, source, agency, site_type, freshness_days=None):
    title = clean_text(title)
    url = str(url or "").strip()
    source = str(source or "").strip()
    agency = str(agency or "").strip()
    site_type = normalize_site_type(site_type)

    if not is_valid_title(title):
        add_reject("invalid_title", date_value, title, url, source, agency, site_type)
        return False

    if not url.startswith("http"):
        add_reject("invalid_url", date_value, title, url, source, agency, site_type)
        return False

    dt = normalize_date(date_value)
    if dt is None:
        dt = extract_date_from_text(str(date_value))

    if has_title_date_conflict(title, dt):
        add_reject("title_date_conflict", date_value, title, url, source, agency, site_type)
        dt = None

    if dt is None:
        date_out = ""
        date_status = "no_date"
    elif dt.year < 2000 or dt.year > datetime.now().year + 2:
        add_reject("implausible_date", date_value, title, url, source, agency, site_type)
        date_out = ""
        date_status = "no_date"
    else:
        date_out = dt.strftime("%Y-%m-%d %H:%M:%S")
        if freshness_days is None:
            recent = is_recent(dt, agency)
        else:
            start_date = datetime.now().date() - timedelta(days=max(0, int(freshness_days)))
            recent = start_date <= dt.date() <= datetime.now().date()
        date_status = "recent" if recent else "old_date"

    results.append({
        "date": date_out,
        "title": title,
        "url": url,
        "source": source,
        "collected_at": now_str(),
        "agency": agency,
        "site_type": site_type,
        "date_status": date_status,
    })
    return True


def fetch_html(url, attempts=3):
    """Fetch an official page without turning a transient failure into NO_NEW."""
    headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
                ),
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Cache-Control": "no-cache",
            }
    session = requests.Session()
    for attempt in range(max(1, int(attempts))):
        try:
            r = session.get(url, headers=headers, timeout=(12, 35), allow_redirects=True)
            if r.status_code < 400 and (r.text or "").strip():
                r.encoding = r.apparent_encoding or r.encoding
                return r
            if r.status_code not in {403, 408, 425, 429, 500, 502, 503, 504}:
                return None
        except requests.RequestException:
            pass
        if attempt + 1 < max(1, int(attempts)):
            time.sleep(1.5 * (attempt + 1))
    return None


def diagnose_zero_yield(url, site_recent_dt=None):
    """Distinguish a defensible NO_NEW from a silent parser failure."""
    res = fetch_html(url)
    if not res:
        return {
            "zero_yield_status": "FAIL",
            "zero_yield_reason": "ZERO_YIELD_HTTP_FAILED",
            "probe_html_bytes": 0,
            "probe_candidate_links": 0,
            "probe_dated_candidates": 0,
        }

    text = res.text or ""
    low = text.lower()
    if len(text.encode("utf-8", errors="ignore")) < 500 or any(x in low for x in (
        "access denied", "captcha", "cf-chl-", "enable javascript to continue", "robot check"
    )):
        return {
            "zero_yield_status": "FAIL",
            "zero_yield_reason": "ZERO_YIELD_BLOCKED_OR_EMPTY_HTML",
            "probe_html_bytes": len(text.encode("utf-8", errors="ignore")),
            "probe_candidate_links": 0,
            "probe_dated_candidates": 0,
        }

    soup = BeautifulSoup(text, "html.parser")
    candidates = []
    dated = 0
    for anchor in soup.find_all("a", href=True):
        title = clean_text(anchor.get_text(" ", strip=True))
        link = urljoin(url, anchor.get("href", ""))
        if not is_valid_title(title) or is_menu_or_category_link(title, link, url):
            continue
        candidates.append((title, link))
        parent_text = clean_text((anchor.parent or anchor).get_text(" ", strip=True))
        if extract_date_from_text(parent_text):
            dated += 1

    recent_hint = bool(site_recent_dt and is_recent(site_recent_dt))
    if len(candidates) >= 3 or dated > 0 or recent_hint:
        reason = "RECENT_HINT_BUT_ZERO" if recent_hint else "POST_LIKE_CONTENT_BUT_ZERO"
        status = "CHECK_REQUIRED"
    else:
        reason = "ACCESS_OK_NO_POST_SIGNAL"
        status = "NO_NEW"
    return {
        "zero_yield_status": status,
        "zero_yield_reason": reason,
        "probe_html_bytes": len(text.encode("utf-8", errors="ignore")),
        "probe_candidate_links": len(candidates),
        "probe_dated_candidates": dated,
    }


def fetch_selenium_html(url, wait_sec=4):
    if not SELENIUM_AVAILABLE:
        return None

    driver = None
    try:
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1800,1400")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(options=options)
        driver.get(url)
        time.sleep(wait_sec)
        return driver.page_source
    except Exception as e:
        print(f"   [DYNAMIC PAGE WARN] {e}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def soup_candidates(url, use_dynamic=True):
    seen = set()
    res = fetch_html(url)
    if res and res.text:
        seen.add(hash(res.text))
        yield BeautifulSoup(res.text, "html.parser")

    if use_dynamic:
        html = fetch_selenium_html(url)
        if html and hash(html) not in seen:
            yield BeautifulSoup(html, "html.parser")


def get_query_params(url):
    qs = parse_qs(urlparse(url).query)
    return {k: v[0] for k, v in qs.items()}


def find_best_anchor(container, base_url, href_keyword=None):
    best = None

    for a in container.find_all("a", href=True):
        title = clean_text(a.get_text(" ", strip=True))
        href = a.get("href", "")

        if href_keyword and href_keyword.lower() not in href.lower():
            continue
        if not is_valid_title(title):
            continue
        if str(href).lower().startswith(("javascript:", "#", "mailto:", "tel:")):
            continue

        link = urljoin(base_url, href)

        if "blog.naver.com" in link.lower():
            continue

        score = len(title)
        if re.search(r"[가-힣]", title):
            score += 10
        if any(k.lower() in title.lower() for k in TRADE_WORDS):
            score += 30

        # Several overseas boards (notably India DGFT) expose only a generic
        # Download label. Recover the actual document title from metadata,
        # the row text, or finally the PDF filename.
        if re.fullmatch(r"(?:download|view)(?:\s*\(type\s*:\s*pdf\))?", title, re.I):
            candidates = [
                clean_text(a.get("title", "")),
                clean_text(a.get("aria-label", "")),
                clean_text(container.get_text(" ", strip=True)),
            ]
            filename = unquote(Path(urlparse(link).path).name)
            filename = re.sub(r"\.(?:pdf|docx?|xlsx?)$", "", filename, flags=re.I)
            filename = re.sub(r"[_]+", " ", filename)
            candidates.append(clean_text(filename))
            for candidate in candidates:
                candidate = re.sub(
                    r"(?i)\bdownload\s*\(type\s*:\s*pdf\)\b|\bdownload\b",
                    " ", candidate,
                )
                candidate = re.sub(
                    r"\b(?:20\d{2}[-/.]\d{1,2}[-/.]\d{1,2}|\d{1,2}[-/.]\d{1,2}[-/.]20\d{2})\b",
                    " ", candidate,
                )
                candidate = clean_text(candidate)
                if len(candidate) >= 12 and not re.fullmatch(r"(?:pdf|view|notification)", candidate, re.I):
                    title = candidate
                    score = len(title) + 25
                    break

        item = (score, title, link)
        if best is None or item[0] > best[0]:
            best = item

    if best:
        return best[1], best[2]

    return None, None


def build_law_url(title, href="", onclick=""):
    title_clean = clean_text(title)
    title_clean = re.sub(r"\s+", "", title_clean)
    title_clean = re.sub(r"\.\.\.$", "", title_clean)
    title_clean = title_clean.replace("…", "")

    href = str(href or "").strip()
    onclick = str(onclick or "").strip()
    joined = href + " " + onclick

    m = re.search(r"lsiSeq\s*[:=]\s*['\"]?(\d+)", joined)
    if not m:
        m = re.search(r"lsInfoP\s*\(\s*['\"]?(\d+)", joined)
    if not m:
        m = re.search(r"lsiSeq=([0-9]+)", joined)
    if not m:
        m = re.search(r"LSI_SEQ\s*[:=]\s*['\"]?(\d+)", joined, re.I)

    if m:
        return f"https://www.law.go.kr/lsInfoP.do?lsiSeq={m.group(1)}&efYd="

    if href.startswith("http"):
        if "lsInfoP" in href and ".do" not in href:
            href = href.replace("lsInfoP", "lsInfoP.do")
        return href

    if href.startswith("/"):
        link = "https://www.law.go.kr" + href
        if "lsInfoP" in link and ".do" not in link:
            link = link.replace("lsInfoP", "lsInfoP.do")
        return link

    if title_clean:
        return "https://www.law.go.kr/법령/" + quote(title_clean)

    return "https://www.law.go.kr"


def crawl_fallback_links(source_url, agency, site_type, allow_keywords=None, block_keywords=None):
    before = len(results)
    res = fetch_html(source_url)

    if not res:
        return 0

    soup = BeautifulSoup(res.text, "html.parser")
    count = 0

    allow_keywords = allow_keywords or []
    block_keywords = block_keywords or BAD_TITLE_CONTAINS

    for a in soup.find_all("a", href=True):
        title = clean_text(a.get_text(" ", strip=True))
        href = a.get("href", "")
        link = urljoin(source_url, href)

        if not is_valid_title(title):
            continue
        if any(b.lower() in title.lower() for b in block_keywords):
            continue
        if any(b.lower() in link.lower() for b in [
            "facebook", "twitter", "youtube", "instagram",
            "linkedin", "blog.naver.com",
        ]):
            continue

        check_text = (title + " " + link).lower()
        if allow_keywords and not any(k.lower() in check_text for k in allow_keywords):
            continue

        post_date = find_date_near_anchor(a)

        if add_result(post_date, title, link, source_url, agency, site_type):
            count += 1

        if count >= MAX_PER_SITE:
            break

    return len(results) - before


def discover_feed_urls(source_url, soup):
    feeds = []
    for tag in soup.find_all("link", href=True):
        typ = clean_text(tag.get("type", "")).lower()
        rel = " ".join(tag.get("rel", [])).lower()
        if "rss" in typ or "atom" in typ or "alternate" in rel and "xml" in typ:
            feeds.append(urljoin(source_url, tag.get("href")))
    parsed = urlparse(source_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    feeds.extend([urljoin(root, x) for x in ["/rss", "/rss.xml", "/feed", "/feed.xml", "/atom.xml"]])
    return list(dict.fromkeys(x for x in feeds if x.startswith("http")))[:8]


def detail_page_date(url):
    """Read published date from an article detail page when the list has no date."""
    res = fetch_html(url)
    if not res:
        return None
    soup = BeautifulSoup(res.text, "html.parser")
    for key in [
        ("property", "article:published_time"), ("name", "date"),
        ("name", "pubdate"), ("name", "publishdate"),
        ("itemprop", "datePublished"),
    ]:
        tag = soup.find("meta", attrs={key[0]: key[1]})
        if tag and tag.get("content"):
            dt = normalize_date(tag.get("content"))
            if dt:
                return dt
    time_tag = soup.find("time")
    if time_tag:
        dt = normalize_date(time_tag.get("datetime")) or extract_date_from_text(time_tag.get_text(" ", strip=True))
        if dt:
            return dt
    # Navigation, footer and related-content dates must not become the article date.
    for selector in [
        "[class*='publish']", "[class*='date']", "[id*='publish']", "[id*='date']",
    ]:
        for tag in soup.select(selector)[:8]:
            dt = extract_date_from_text(tag.get_text(" ", strip=True))
            if dt:
                return dt
    return None


def crawl_resilient_new_posts(source_url, agency, site_type):
    """Final real-post probe: static HTML -> discovered feeds -> dynamic HTML -> detail dates."""
    before = len(results)
    strategies = []
    static = fetch_html(source_url)
    soups = []
    if static and static.text:
        soups.append(("static", BeautifulSoup(static.text, "html.parser")))
    if soups:
        for feed in discover_feed_urls(source_url, soups[0][1]):
            n0 = len(results)
            try:
                crawl_rss(feed, agency, site_type)
            except Exception:
                pass
            if len(results) > n0:
                strategies.append(f"feed:{feed}")
    dynamic = fetch_selenium_html(source_url, wait_sec=5)
    if dynamic:
        soups.append(("selenium", BeautifulSoup(dynamic, "html.parser")))

    probed = 0
    seen_links = set()
    for strategy, soup in soups:
        for a in soup.find_all("a", href=True):
            title = clean_text(a.get_text(" ", strip=True) or a.get("title", ""))
            href = clean_text(a.get("href", ""))
            if not is_valid_title(title) or href.lower().startswith(("javascript:", "#", "mailto:")):
                continue
            link = urljoin(source_url, href)
            if link in seen_links or urlparse(link).netloc != urlparse(source_url).netloc:
                continue
            seen_links.add(link)
            dt = find_date_near_anchor(a) or extract_date_from_tag(a.parent or a)
            if not dt and probed < 12:
                dt = detail_page_date(link)
                probed += 1
            if dt and is_recent(dt):
                if add_result(dt, title, link, source_url, agency, site_type):
                    strategies.append(strategy + ":detail_date")
            if len(results) - before >= MAX_PER_SITE:
                break
        if len(results) - before >= MAX_PER_SITE:
            break
    return len(results) - before, ";".join(dict.fromkeys(strategies)) or "NO_REAL_POST_FOUND"


def crawl_site_hint(row, source_url, agency, site_type):
    for col in ["최근게시일", "latest_date", "last_post_date"]:
        if col in row.index and str(row.get(col, "")).strip():
            hint_date = row.get(col, "")
            title = f"{agency} 최신 게시물 확인 필요"
            before = len(results)
            add_result(hint_date, title, source_url, source_url, agency, site_type)
            return len(results) - before
    return 0


KOREA_LIST_URL = "https://www.customs.go.kr/kcs/na/ntt/selectNttList.do"
KOREA_DETAIL_URL = "https://www.customs.go.kr/kcs/na/ntt/selectNttInfo.do"


@dataclass
class KoreaPost:
    number: str
    category: str
    title: str
    author: str
    posted_at: date
    views: str
    detail_url: str


def parse_korea_date(value: str):
    s = clean_text(value)
    if not re.search(r"20\d{2}\s*[-/.]\s*\d{1,2}\s*[-/.]\s*\d{1,2}", s):
        return None

    try:
        return datetime.strptime(s, "%Y.%m.%d").date()
    except Exception:
        pass

    dt = normalize_date(s.replace(".", "-"))
    if dt:
        return dt.date()

    return None


class CustomsBoardParser(HTMLParser):
    def __init__(self, base_params):
        super().__init__(convert_charrefs=True)
        self.base_params = base_params
        self.posts = []
        self._in_row = False
        self._in_cell = False
        self._cell_text = []
        self._cells = []
        self._link_attrs = {}
        self._anchor_title = ""

    def handle_starttag(self, tag, attrs):
        attr = {k: v or "" for k, v in attrs}

        if tag == "tr":
            self._in_row = True
            self._cells = []
            self._link_attrs = {}
            self._anchor_title = ""

        if self._in_row and tag == "td":
            self._in_cell = True
            self._cell_text = []

        if self._in_row and tag == "a" and "nttInfoBtn" in attr.get("class", ""):
            self._link_attrs = attr
            self._anchor_title = attr.get("title", "")

    def handle_data(self, data):
        if self._in_cell:
            self._cell_text.append(data)

    def handle_endtag(self, tag):
        if self._in_cell and tag == "td":
            self._cells.append(clean_text(" ".join(self._cell_text)))
            self._in_cell = False

        if self._in_row and tag == "tr":
            self._append_post_if_valid()
            self._in_row = False

    def _append_post_if_valid(self):
        if len(self._cells) < 6 or not self._link_attrs:
            return

        posted_at = parse_korea_date(self._cells[4])
        if posted_at is None:
            return

        title = clean_text(self._anchor_title or self._cells[2])
        ntt_sn = self._link_attrs.get("data-id", "")
        ntt_sn_url = self._link_attrs.get("data-url", "")

        detail_params = {
            **self.base_params,
            "nttSn": ntt_sn,
            "nttSnUrl": ntt_sn_url,
        }

        self.posts.append(
            KoreaPost(
                number=self._cells[0],
                category=self._cells[1],
                title=title,
                author=self._cells[3],
                posted_at=posted_at,
                views=self._cells[5],
                detail_url=f"{KOREA_DETAIL_URL}?{urlencode(detail_params)}",
            )
        )


def fetch_korea_html(source_url):
    params = get_query_params(source_url)

    if "mi" not in params or "bbsId" not in params:
        params = {"mi": "2891", "bbsId": "1362"}

    full_url = f"{KOREA_LIST_URL}?{urlencode(params)}"

    req = Request(
        full_url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "close",
        },
    )

    last_error = None

    for attempt in range(1, 4):
        try:
            with urlopen(req, timeout=20) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                html = response.read().decode(charset, errors="replace")
                return html, params, full_url
        except (ConnectionResetError, TimeoutError, URLError) as e:
            last_error = e
            time.sleep(attempt * 2)

    raise RuntimeError(f"Korea Customs 접속 실패: {last_error}")


def crawl_korea(source_url, agency, site_type):
    before = len(results)

    try:
        html, params, full_url = fetch_korea_html(source_url)
        parser = CustomsBoardParser(params)
        parser.feed(html)

        for post in parser.posts:
            add_result(
                date_value=post.posted_at,
                title=f"[{post.category}] {post.title}",
                url=post.detail_url,
                source=full_url,
                agency=agency,
                site_type=site_type,
            )

    except Exception as e:
        print("   ERROR:", e)

    if len(results) - before == 0:
        return crawl_fallback_links(
            source_url,
            agency,
            site_type,
            allow_keywords=["ntt", "notice", "공지", "공고", "보도", "행정", "fta", "customs"],
        )

    return len(results) - before


def crawl_rss(source_url, agency, site_type):
    before = len(results)
    res = fetch_html(source_url)

    if not res:
        return 0

    soup = BeautifulSoup(res.text, "xml")
    items = soup.find_all(["item", "entry"])

    for item in items[:MAX_PER_SITE]:
        title_tag = item.find("title")
        link_tag = item.find("link")
        date_tag = item.find("pubDate") or item.find("published") or item.find("updated") or item.find("dc:date")

        title = title_tag.get_text(strip=True) if title_tag else ""
        link = ""

        if link_tag:
            link = link_tag.get("href") or link_tag.get_text(strip=True)

        dt = date_tag.get_text(strip=True) if date_tag else ""

        add_result(dt, title, urljoin(source_url, link), source_url, agency, site_type)

    if len(results) - before == 0:
        return crawl_fallback_links(
            source_url,
            agency,
            site_type,
            allow_keywords=["news", "notice", "공지", "공고", "rss", "article"],
        )

    return len(results) - before


def _url_with_query_value(url, key, value):
    """Return *url* with one query-string value replaced safely."""
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query[key] = [str(value)]
    return parsed._replace(query=urlencode(query, doseq=True)).geturl()


def crawl_lawmaking_bills(source_url, agency, site_type):
    """Crawl National Assembly bill status until the freshness cutoff is reached.

    The board exposes only about 20 bills on its first page.  The former generic
    table parser therefore missed relevant bills as soon as newer, unrelated
    bills pushed them to page 2 or later.  This parser follows ``pageIndex`` and
    stops only after it has observed a bill date older than the inclusive cutoff.
    """
    before = len(results)
    normal_days = max(0, int(HOURS_BACK // 24))
    window_days = max(normal_days, LAWMAKING_BACKFILL_DAYS)
    cutoff = datetime.now().date() - timedelta(days=window_days)
    pages_scanned = 0
    cutoff_reached = False
    fetch_failed = False
    oldest_seen = None
    latest_seen = None
    seen_urls = set()
    page_row_counts = []

    for page_index in range(1, LAWMAKING_MAX_PAGES + 1):
        page_url = _url_with_query_value(source_url, "pageIndex", page_index)
        page_url = _url_with_query_value(page_url, "blockStartPage", ((page_index - 1) // 10) * 10 + 1)
        res = fetch_html(page_url)
        if not res:
            fetch_failed = True
            break

        pages_scanned += 1
        soup = BeautifulSoup(res.text, "html.parser")
        page_dates = []
        page_added = 0

        for row in soup.find_all("tr"):
            anchor = row.find(
                "a",
                href=re.compile(r"/gcom/nsmLmSts/out/\d+/detailRP(?:$|[?#])"),
            )
            if not anchor:
                continue

            title = clean_text(anchor.get_text(" ", strip=True))
            link = urljoin(source_url, anchor.get("href", ""))
            if not title or not link or link in seen_urls:
                continue

            row_text = clean_text(row.get_text(" ", strip=True))
            post_date = extract_date_from_tag(row) or extract_date_from_text(row_text)
            if post_date is None:
                continue

            seen_urls.add(link)
            page_dates.append(post_date.date())
            oldest_seen = post_date.date() if oldest_seen is None else min(oldest_seen, post_date.date())
            latest_seen = post_date.date() if latest_seen is None else max(latest_seen, post_date.date())

            if add_result(
                post_date, title, link, source_url, agency, site_type,
                freshness_days=window_days,
            ):
                page_added += 1

        page_row_counts.append(page_added)
        if not page_dates:
            break

        # The cutoff day itself is in scope. Continue while a page contains
        # only cutoff-day rows because more rows from the same day can spill
        # onto the next page.
        if min(page_dates) < cutoff:
            cutoff_reached = True
            break

        # Repeated/empty result pages indicate that pagination did not advance.
        if page_added == 0:
            break

    partial = fetch_failed or not cutoff_reached
    site_coverage_status[source_url] = {
        "coverage_status": "PARTIAL_COVERAGE" if partial else "COMPLETE_TO_CUTOFF",
        "collection_mode": "BACKFILL" if LAWMAKING_BACKFILL_DAYS > normal_days else "DAILY",
        "window_days": window_days,
        "pages_scanned": pages_scanned,
        "cutoff_date": cutoff.isoformat(),
        "cutoff_reached": "Y" if cutoff_reached else "N",
        "oldest_date_seen": oldest_seen.isoformat() if oldest_seen else "",
        "latest_date_seen": latest_seen.isoformat() if latest_seen else "",
        "page_row_counts": ",".join(str(x) for x in page_row_counts),
        "coverage_error": "PAGE_FETCH_FAILED" if fetch_failed else (
            "MAX_PAGES_BEFORE_CUTOFF" if not cutoff_reached and pages_scanned >= LAWMAKING_MAX_PAGES
            else "CUTOFF_NOT_REACHED"
        ) if partial else "",
    }
    return len(results) - before


def crawl_table(source_url, agency, site_type):
    before = len(results)
    res = fetch_html(source_url)

    if not res:
        return 0

    soup = BeautifulSoup(res.text, "html.parser")
    count = 0

    for row in soup.find_all("tr"):
        row_text = clean_text(row.get_text(" ", strip=True))
        if len(row_text) < 15:
            continue

        post_date = extract_date_from_tag(row) or extract_date_from_text(row_text)
        title, link = find_best_anchor(row, source_url)

        if not title or not link:
            continue

        if add_result(post_date, title, link, source_url, agency, site_type):
            count += 1

        if count >= MAX_PER_SITE:
            break

    if count < 5:
        for tag in soup.find_all(["li", "dt", "dd", "article", "section", "div"]):
            text = clean_text(tag.get_text(" ", strip=True))
            if len(text) < 20:
                continue

            post_date = extract_date_from_tag(tag) or extract_date_from_text(text)
            title, link = find_best_anchor(tag, source_url)

            if not title or not link:
                continue

            if add_result(post_date, title, link, source_url, agency, site_type):
                count += 1

            if count >= MAX_PER_SITE:
                break

    if len(results) - before == 0:
        return crawl_fallback_links(
            source_url,
            agency,
            site_type,
            allow_keywords=["법령", "고시", "공고", "시행", "개정", "law", "notice", "news", "article", "research", "kctdi"],
        )

    return len(results) - before


def crawl_card(source_url, agency, site_type):
    before = len(results)
    res = fetch_html(source_url)

    if not res:
        return 0

    soup = BeautifulSoup(res.text, "html.parser")
    count = 0

    for c in soup.find_all(["article", "li", "div", "section", "tr", "dt", "dd"]):
        text = clean_text(c.get_text(" ", strip=True))
        if len(text) < 20:
            continue

        post_date = extract_date_from_tag(c) or extract_date_from_text(text)
        title, link = find_best_anchor(c, source_url)

        if not title or not link:
            continue

        if add_result(post_date, title, link, source_url, agency, site_type):
            count += 1

        if count >= MAX_PER_SITE:
            break

    if len(results) - before == 0:
        return crawl_fallback_links(
            source_url,
            agency,
            site_type,
            allow_keywords=["news", "notice", "customs", "announcement", "trade", "tariff", "article", "press", "release"],
        )

    return len(results) - before


def crawl_cbp(source_url, agency, site_type):
    before = len(results)
    res = fetch_html(source_url)

    if not res:
        return 0

    soup = BeautifulSoup(res.text, "html.parser")
    count = 0
    seen = set()

    for a in soup.find_all("a", href=True):
        title = clean_text(a.get_text(" ", strip=True))
        href = a.get("href", "")
        link = urljoin(source_url, href)
        link_l = link.lower()

        if not is_valid_title(title):
            continue

        if "/newsroom/" not in link_l:
            continue

        if not (
            "/national-media-release/" in link_l
            or "/local-media-release/" in link_l
            or "/media-release/" in link_l
            or "/frontline/" in link_l
            or "/trade/" in link_l
        ):
            continue

        if any(b.lower() in title.lower() for b in BAD_TITLE_CONTAINS):
            continue

        post_date = find_date_near_anchor(a)

        key = title + link
        if key in seen:
            continue
        seen.add(key)

        if add_result(post_date, title, link, source_url, agency, site_type):
            count += 1

        if count >= MAX_PER_SITE:
            break

    return len(results) - before


def crawl_cbp_ruling(source_url, agency, site_type):
    before = len(results)
    res = fetch_html(source_url)

    if not res:
        return 0

    soup = BeautifulSoup(res.text, "html.parser")
    count = 0
    seen = set()

    blocked_paths = [
        "/travel/",
        "/about/",
        "/careers/",
        "/border-security/",
        "/newsroom/",
        "/contact/",
    ]

    for a in soup.find_all("a", href=True):
        title = clean_text(a.get_text(" ", strip=True))
        href = a.get("href", "")
        link = urljoin(source_url, href)
        link_l = link.lower()
        check_text = f"{title} {link}".lower()

        if not is_valid_title(title):
            continue
        if any(path in link_l for path in blocked_paths):
            continue
        if not (
            "federalregister.gov/documents" in link_l
            or "federal-register-notices" in check_text
            or "federal register notice" in check_text
        ):
            continue

        container = a.find_parent(["tr", "li", "article", "section", "div"]) or a
        row_text = clean_text(container.get_text(" ", strip=True))
        post_date = find_date_near_anchor(a) or extract_date_from_text(row_text)

        key = normalize_url_for_compare(link) or title
        if key in seen:
            continue
        seen.add(key)

        if add_result(post_date, title, link, source_url, agency, site_type):
            count += 1

        if count >= MAX_PER_SITE:
            break

    if len(results) - before == 0:
        return crawl_fallback_links(
            source_url,
            agency,
            site_type,
            allow_keywords=["federalregister.gov/documents", "federal-register", "federal register notice"],
        )

    return len(results) - before


def crawl_ustr(source_url, agency, site_type):
    before = len(results)
    res = fetch_html(source_url)

    if not res:
        return 0

    soup = BeautifulSoup(res.text, "html.parser")
    count = 0

    for c in soup.find_all(["article", "li", "div", "tr"]):
        post_date = extract_date_from_tag(c)
        title, link = find_best_anchor(c, source_url, "/press-releases/")

        if not title:
            continue

        if add_result(post_date, title, link, source_url, agency, site_type):
            count += 1

        if count >= MAX_PER_SITE:
            break

    if len(results) - before == 0:
        return crawl_fallback_links(
            source_url,
            agency,
            site_type,
            allow_keywords=["press-releases", "trade", "tariff", "ustr", "news"],
        )

    return len(results) - before


def extract_wto_url_date(url):
    """Read the publication date encoded in WTO news-detail filenames."""
    match = re.search(
        r"_(\d{1,2})(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(\d{2})_",
        str(url or ""),
        re.I,
    )
    if not match:
        return None
    months = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    try:
        return datetime(2000 + int(match.group(3)), months[match.group(2).lower()], int(match.group(1)))
    except ValueError:
        return None


def parse_wto_news_page(html, source_url, agency, site_type):
    """Extract concrete WTO news detail links, excluding navigation labels."""
    before = len(results)
    soup = BeautifulSoup(html or "", "html.parser")
    seen = set()
    for anchor in soup.find_all("a", href=True):
        title = clean_text(anchor.get_text(" ", strip=True))
        link = urljoin(source_url, anchor.get("href", ""))
        link_l = link.lower()
        if "/english/news_e/news" not in link_l:
            continue
        if not re.search(r"/news\d{2}_e/[^/]+\.htm(?:l)?(?:[?#].*)?$", link_l):
            continue
        if link_l.endswith(("/news26_e.htm", "/news26_e.html")):
            continue
        key = normalize_url_for_compare(link)
        if not key or key in seen or is_menu_or_category_link(title, link, source_url):
            continue
        seen.add(key)
        post_date = extract_wto_url_date(link) or find_date_near_anchor(anchor)
        add_result(post_date, title, link, source_url, agency, site_type)
        if len(results) - before >= MAX_PER_SITE:
            break
    return len(results) - before


def crawl_wto(source_url, agency, site_type):
    if "rss" in source_url.lower() or source_url.lower().endswith(".xml"):
        return crawl_rss(source_url, agency, site_type)

    response = fetch_html(source_url)
    count = parse_wto_news_page(
        response.text if response else "", source_url, agency, site_type
    )
    if count == 0:
        # The static response can contain only the WTO navigation shell.
        # Render once and apply the same strict detail-link contract.  Never
        # fall back to generic anchors because that converts six menu labels
        # into six false "posts" and hides the parser failure.
        dynamic_html = fetch_selenium_html(source_url, wait_sec=6)
        count = parse_wto_news_page(dynamic_html, source_url, agency, site_type)
    return count


def crawl_eu(source_url, agency, site_type):
    if "rss" in source_url.lower() or source_url.lower().endswith(".xml"):
        return crawl_rss(source_url, agency, site_type)

    count = crawl_card(source_url, agency, site_type)
    if count == 0:
        return crawl_fallback_links(
            source_url,
            agency,
            site_type,
            allow_keywords=["news", "taxation", "customs", "trade", "article"],
        )
    return count


def parse_gwanbo_text(text, source_url, agency, site_type, page_date):
    before = len(results)

    bad_words = [
        "최초 사용자", "가이드", "공직자 재산공개", "병역사항 공개",
        "관인", "마이페이지", "관보보기", "관보분석", "관심 관보",
        "일자별", "기간별", "호수별", "인기관보", "정정관보",
        "취소관보", "목차 다운로드", "전체 다운로드", "타임스탬프",
    ]

    patterns = [
        r"(법률제\d+호\s*\([^)]+\))",
        r"(대통령령제\d+호\s*\([^)]+\))",
        r"(총리령제\d+호\s*\([^)]+\))",
        r"([가-힣]+령제\d+호\s*\([^)]+\))",
        r"([가-힣]+부령제\d+호\s*\([^)]+\))",
        r"([가-힣]+고시제[\d\-]+호\s*\([^)]+\))",
        r"([가-힣]+고시제[\d\-]+호\s*[^\n]{0,160})",
        r"([가-힣]+공고제[\d\-]+호\s*\([^)]+\))",
        r"([가-힣]+공고제[\d\-]+호\s*[^\n]{0,160})",
        r"([가-힣]+훈령제\d+호\s*\([^)]+\))",
        r"([가-힣]+훈령제\d+호\s*[^\n]{0,160})",
        r"([가-힣]+예규제\d+호\s*\([^)]+\))",
        r"([가-힣]+예규제\d+호\s*[^\n]{0,160})",
        r"(법인해산명령신청공고\s*\([^)]+\))",
        r"(형사보상결정공시\s*\([^)]+\))",
        r"(압수물환부공고\s*\([^)]+\))",
        r"([가-힣]+공고제\d+호\s*\([^)]+\))",
    ]

    seen = set()

    for p in patterns:
        for m in re.finditer(p, text):
            title = clean_text(m.group(1))

            if any(b in title for b in bad_words):
                continue
            if len(title) < 10:
                continue

            title = re.split(
                r"(헌법|법률|조약|대통령령|총리령|부령|고시|공고|국회|법원|상훈|기타)\s*총\s*\d+건",
                title,
            )[0].strip()

            title = clean_text(title)

            if title in seen:
                continue
            seen.add(title)

            item_url = f"{source_url}?gwanboDate={page_date:%Y%m%d}&gwanboTitle={quote(title[:120])}"
            add_result(page_date, title, item_url, source_url, agency, site_type)

            if len(seen) >= MAX_GWANBO_ITEMS:
                break

    return len(results) - before


GWANBO_NOTICE_START_RE = re.compile(
    r"(?:법률|대통령령|총리령)제\s*\d+호|"
    r"[가-힣]{2,30}(?:부령|고시|공고|훈령|예규)제?\s*\d{4}(?:[-–]\d+)?호"
)


def split_joined_notices(title):
    """Split a gazette row containing two or more independent legal notices."""
    value = clean_text(title)
    starts = [m.start() for m in GWANBO_NOTICE_START_RE.finditer(value)]
    if len(starts) < 2:
        return [value] if value else []
    parts = []
    for pos, end in zip(starts, starts[1:] + [len(value)]):
        part = clean_text(value[pos:end]).strip(" ·,;/")
        if len(part) >= 10 and part not in parts:
            parts.append(part)
    return parts or [value]


def explode_joined_gwanbo_rows(df):
    """Final safety net: rescue parsers must not reintroduce joined gazette titles."""
    if df.empty:
        return df
    out = []
    for _, row in df.iterrows():
        is_gwanbo = "관보" in clean_text(row.get("agency", ""))
        parts = split_joined_notices(row.get("title", "")) if is_gwanbo else [row.get("title", "")]
        for part in parts:
            copied = row.copy()
            copied["title"] = part
            if is_gwanbo:
                base_url = clean_text(row.get("source", "")) or clean_text(row.get("url", "")).split("?", 1)[0]
                dt = pd.to_datetime(row.get("date"), errors="coerce")
                day = dt.strftime("%Y%m%d") if pd.notna(dt) else datetime.now().strftime("%Y%m%d")
                copied["url"] = f"{base_url}?gwanboDate={day}&gwanboTitle={quote(part[:120])}"
            out.append(copied)
    return pd.DataFrame(out, columns=df.columns)


def crawl_gwanbo_requests(source_url, agency, site_type):
    before = len(results)
    res = fetch_html(source_url)

    if not res:
        return 0

    soup = BeautifulSoup(res.text, "html.parser")
    text = clean_text(soup.get_text("\n", strip=True))
    page_date = extract_date_from_text(text) or datetime.now()

    parse_gwanbo_text(text, source_url, agency, site_type, page_date)

    return len(results) - before


def crawl_gwanbo_selenium(source_url, agency, site_type):
    before = len(results)

    if not SELENIUM_AVAILABLE:
        print("   [WARN] selenium 미설치: pip install selenium 필요")
        return crawl_gwanbo_requests(source_url, agency, site_type)

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1800,1400")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = None

    try:
        driver = webdriver.Chrome(options=options)
        driver.get(source_url)
        time.sleep(5)

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        text = clean_text(soup.get_text("\n", strip=True))
        page_date = extract_date_from_text(text) or datetime.now()

        parse_gwanbo_text(text, source_url, agency, site_type, page_date)

    except Exception as e:
        print(f"   [GWANBO SELENIUM ERROR] {e}")
        return crawl_gwanbo_requests(source_url, agency, site_type)

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    return len(results) - before


def crawl_gwanbo(source_url, agency, site_type):
    return crawl_gwanbo_selenium(source_url, agency, site_type)


def crawl_motie(source_url, agency, site_type):
    before = len(results)
    res = fetch_html(source_url)

    if not res:
        return 0

    soup = BeautifulSoup(res.text, "html.parser")
    count = 0

    category_words = [
        "훈령·예규·지침", "입법 예고", "행정 예고", "고시·공고",
        "보도자료", "예산·법령", "산업통상부 네이버 블로그",
    ]

    for row in soup.find_all(["tr", "li", "div", "article"]):
        post_date = extract_date_from_tag(row) or extract_date_from_text(row.get_text(" ", strip=True))

        for a in row.find_all("a", href=True):
            title = clean_text(a.get_text(" ", strip=True))
            href = a.get("href", "")
            link = urljoin(source_url, href)

            if not title or len(title) < 10:
                continue
            if any(w in title for w in category_words):
                continue
            if "blog.naver.com" in link.lower():
                continue
            if "/kor/article/" not in link:
                continue

            if add_result(post_date, title, link, source_url, agency, site_type):
                count += 1
                break

        if count >= MAX_PER_SITE:
            break

    if len(results) - before == 0:
        return crawl_fallback_links(
            source_url,
            agency,
            site_type,
            allow_keywords=["/kor/article/", "article", "보도", "고시", "공고", "입법", "행정", "훈령", "예규"],
        )

    return len(results) - before


def crawl_custra(source_url, agency, site_type):
    count = crawl_table(source_url, agency, site_type)
    if count == 0:
        return crawl_fallback_links(
            source_url,
            agency,
            site_type,
            allow_keywords=["research", "kctdi", "관세", "무역", "통상", "보고서", "동향", "customs", "trade"],
        )
    return count


def crawl_oecd(source_url, agency, site_type):
    count = crawl_card(source_url, agency, site_type)
    if count == 0:
        return crawl_fallback_links(
            source_url,
            agency,
            site_type,
            allow_keywords=["news", "press", "trade", "oecd", "tax", "policy"],
        )
    return count


def build_nsp_url(source_url, href="", onclick=""):
    href = str(href or "").strip()
    onclick = str(onclick or "").strip()
    joined = f"{href} {onclick}"

    if href and not href.lower().startswith(("javascript:", "#")):
        return urljoin(source_url, href)

    for pattern in [
        r"latestSn\s*[:=]\s*['\"]?(\d+)",
        r"bbscttSn\s*[:=]\s*['\"]?(\d+)",
        r"nttSn\s*[:=]\s*['\"]?(\d+)",
        r"\(\s*['\"]?(\d{3,})['\"]?",
    ]:
        match = re.search(pattern, joined, re.I)
        if match:
            sep = "&" if "?" in source_url else "?"
            return f"{source_url}{sep}latestSn={match.group(1)}"

    return ""


def crawl_nsp(source_url, agency, site_type):
    before = len(results)
    seen = set()

    for soup in soup_candidates(source_url, use_dynamic=True):
        for row in soup.find_all(["tr", "li", "article", "section", "div"]):
            row_text = clean_text(row.get_text(" ", strip=True))
            if len(row_text) < 12:
                continue

            post_date = extract_date_from_tag(row) or extract_date_from_text(row_text)

            for a in row.find_all("a"):
                title = clean_text(a.get_text(" ", strip=True) or a.get("title", ""))
                href = a.get("href", "")
                onclick = a.get("onclick", "")
                link = build_nsp_url(source_url, href, onclick)

                if not title or not link:
                    continue
                if not is_valid_title(title):
                    continue
                if is_menu_or_category_link(title, link, source_url):
                    continue

                check = f"{title} {link}".lower()
                if not (
                    "/trend/latest/" in check
                    or "latest" in check
                    or "detail.do" in check
                    or "nsp.nanet.go.kr" in check
                ):
                    continue

                key = normalize_url_for_compare(link) or title
                if key in seen:
                    continue
                seen.add(key)

                add_result(post_date, title, link, source_url, agency, site_type)

                if len(results) - before >= MAX_PER_SITE:
                    break

            if len(results) - before >= MAX_PER_SITE:
                break

        if len(results) - before > 0:
            break

    return len(results) - before


def parse_cbic_rendered_html(html, source_url):
    """Return defensible CBIC rows from the Angular-rendered News Updates page."""
    soup = BeautifulSoup(html or "", "html.parser")
    rows = []
    seen = set()
    containers = soup.select("ul.all-new-list li") or soup.select("#news-marquee li, article, .news-item")
    for item in containers:
        anchors = item.find_all("a")
        title = ""
        link = source_url
        for anchor in anchors:
            candidate = clean_text(anchor.get_text(" ", strip=True) or anchor.get("title", ""))
            if len(candidate) > len(title) and is_valid_title(candidate):
                title = candidate
                href = clean_text(anchor.get("href", ""))
                if href and not href.lower().startswith(("javascript:", "#")):
                    link = urljoin(source_url, href)
        if not title:
            title = clean_text(item.get_text(" ", strip=True))
        if not is_valid_title(title):
            continue
        item_text = clean_text(item.get_text(" ", strip=True))
        post_date = extract_date_from_text(item_text) or extract_date_from_text(title)
        key = (title.lower(), normalize_url_for_compare(link))
        if key in seen:
            continue
        seen.add(key)
        rows.append((post_date, title, link))
    return rows


def crawl_cbic(source_url, agency, site_type):
    """CBIC is Angular-rendered; static shell success is not collection success."""
    before = len(results)
    rendered = fetch_selenium_html(source_url, wait_sec=7)
    if not rendered:
        return 0
    for post_date, title, link in parse_cbic_rendered_html(rendered, source_url):
        # Undated candidates stay in audit but cannot prove NO_NEW or enter the
        # recent regulation feed. This prevents the page update date from being
        # copied to every post.
        add_result(post_date, title, link, source_url, agency, site_type)
        if len(results) - before >= MAX_PER_SITE:
            break
    return len(results) - before


def federal_register_usitc_api_url(per_page=50):
    params = {
        "per_page": max(1, min(int(per_page), 100)),
        "order": "newest",
        "conditions[agencies][]": "international-trade-commission",
    }
    return "https://www.federalregister.gov/api/v1/documents.json?" + urlencode(params)


def parse_federal_register_api(payload):
    rows = []
    for item in (payload or {}).get("results", []):
        title = clean_text(item.get("title", ""))
        link = clean_text(item.get("html_url", "") or item.get("pdf_url", ""))
        post_date = item.get("publication_date", "")
        agencies = item.get("agencies") or []
        agency_names = " ".join(clean_text(x.get("name", "")) for x in agencies if isinstance(x, dict))
        if not is_valid_title(title) or not link.startswith("http"):
            continue
        if agencies and "international trade commission" not in agency_names.lower():
            continue
        rows.append((post_date, title, link))
    return rows


def crawl_usitc_federal_register(agency, site_type):
    """Official failover for USITC pages blocked by anti-automation controls."""
    before = len(results)
    api_url = federal_register_usitc_api_url()
    try:
        response = requests.get(
            api_url,
            headers={"User-Agent": "GTI-Radar/2.3 official-source-monitor"},
            timeout=(12, 35),
        )
        if response.status_code >= 400:
            return 0
        payload = response.json()
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return 0
    for post_date, title, link in parse_federal_register_api(payload):
        add_result(post_date, title, link, api_url, agency, site_type)
        if len(results) - before >= MAX_PER_SITE:
            break
    return len(results) - before


def crawl_usitc(source_url, agency, site_type):
    count = crawl_card(source_url, agency, site_type)
    if count == 0:
        count = crawl_fallback_links(
            source_url,
            agency,
            site_type,
            allow_keywords=["news", "release", "commission", "notice", "usitc", "investigation"],
        )
    if count == 0:
        count = crawl_usitc_federal_register(agency, site_type)
    return count


def crawl_fta_trend(source_url, agency, site_type):
    before = len(results)

    for soup in soup_candidates(source_url, use_dynamic=True):
        lines = [clean_text(x) for x in soup.get_text("\n", strip=True).splitlines()]
        lines = [x for x in lines if x]

        for i, line in enumerate(lines):
            if not re.fullmatch(r"\d{3,6}", line):
                continue

            title = ""
            post_date = None

            for j in range(i + 1, min(i + 8, len(lines))):
                candidate = clean_text(lines[j])
                if not candidate or re.fullmatch(r"\d+", candidate):
                    continue
                if extract_date_from_text(candidate):
                    post_date = extract_date_from_text(candidate)
                    break
                if candidate.lower() in {"image: 새글", "새글"}:
                    continue
                if not title and is_valid_title(candidate):
                    title = candidate

            if not title:
                continue

            if not post_date:
                for j in range(i + 1, min(i + 10, len(lines))):
                    post_date = extract_date_from_text(lines[j])
                    if post_date:
                        break

            if not post_date:
                continue

            link = f"{source_url}?trendTitle={quote(title[:120])}"
            add_result(post_date, title, link, source_url, agency, site_type)

            if len(results) - before >= MAX_PER_SITE:
                break

        if len(results) - before > 0:
            break

    return len(results) - before


def crawl_law(source_url, agency, site_type):
    before = len(results)
    res = fetch_html(source_url)

    if not res:
        return 0

    soup = BeautifulSoup(res.text, "html.parser")
    count = 0

    law_keywords = [
        "법률", "시행령", "시행규칙", "고시", "훈령", "예규",
        "행정규칙", "공포", "개정", "폐지", "제정",
    ]

    rows = soup.find_all(["tr", "li", "div"])

    for row in rows:
        row_text = clean_text(row.get_text(" ", strip=True))

        if len(row_text) < 15:
            continue
        if not any(k in row_text for k in law_keywords):
            continue

        post_date = extract_date_from_tag(row) or extract_date_from_text(row_text)

        a = row.find("a")
        if not a:
            continue

        title = clean_text(a.get_text(" ", strip=True))
        if not title:
            title = row_text

        title = re.sub(r"\s*상세보기\s*$", "", title)
        title = re.sub(r"\s*\.\.\.$", "", title)
        title = clean_text(title)

        href = a.get("href", "")
        onclick = a.get("onclick", "")

        link = build_law_url(title, href, onclick)

        if not title or not link:
            continue

        if add_result(post_date, title, link, source_url, agency, site_type):
            count += 1

        if count >= MAX_PER_SITE:
            break

    if len(results) - before == 0:
        return crawl_fallback_links(
            source_url,
            agency,
            site_type,
            allow_keywords=["law", "법률", "시행령", "시행규칙", "고시", "훈령", "예규", "행정규칙", "공포", "개정"],
        )

    return len(results) - before


def extract_law_decision_title_date(text):
    """31.국가법령정보센터(행정심판): date is inside title brackets."""
    text = clean_text(text)

    patterns = [
        r"(.+?)\s*\[\s*[^\]]*?(20\d{2}\.\s*\d{1,2}\.\s*\d{1,2}\.?)\s*[^\]]*?\]",
        r"(.+?)\s*\(\s*20\d{2}[^)]*?(20\d{2}\.\s*\d{1,2}\.\s*\d{1,2}\.?)\s*\)",
        r"(.+?)\s*\[\s*[^\]]*?(20\d{2}[-/]\s*\d{1,2}[-/]\s*\d{1,2})\s*[^\]]*?\]",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        title = clean_text(match.group(1))
        date_text = match.group(2)
        dt = normalize_date(date_text.replace(".", "-").replace("/", "-"))
        if title and dt:
            return title, dt

    return "", None


def crawl_law_decision(source_url, agency, site_type):
    before = len(results)

    for soup in soup_candidates(source_url):
        for row in soup.find_all(["tr", "li", "div"]):
            row_text = clean_text(row.get_text(" ", strip=True))
            if len(row_text) < 20:
                continue

            title_from_text, post_date = extract_law_decision_title_date(row_text)
            if not post_date:
                continue

            title, link = find_best_anchor(row, source_url)
            if not title:
                title = title_from_text
            else:
                title = clean_text(re.sub(r"\s*\[.*?20\d{2}.*?\]\s*", "", title))
                if len(title) < 4:
                    title = title_from_text

            if not link:
                link = _row_link_with_title(source_url, row, title)

            add_result(post_date, title, link, source_url, agency, site_type)

            if len(results) - before >= MAX_PER_SITE:
                break

        if len(results) - before > 0:
            break

    return len(results) - before


def crawl_krcaa(source_url, agency, site_type):
    before = len(results)
    res = fetch_html(source_url)

    if not res:
        return 0

    soup = BeautifulSoup(res.text, "html.parser")
    count = 0

    for row in soup.find_all("tr"):
        cells = [clean_text(cell.get_text(" ", strip=True)) for cell in row.find_all(["td", "th"])]
        if len(cells) < 2:
            continue

        row_text = clean_text(" ".join(cells))
        post_date = extract_date_from_text(row_text)
        if not post_date:
            continue

        title = ""
        for cell in cells:
            if extract_date_from_text(cell):
                continue
            if re.fullmatch(r"[\d,]+", cell or ""):
                continue
            if len(cell) >= 8:
                title = cell
                break

        anchor = row.find("a", href=True) or row.find("a", onclick=True) or row.find("a")
        href = anchor.get("href", "") if anchor else ""
        onclick = anchor.get("onclick", "") if anchor else ""
        link = _row_link_with_title(source_url, row, title)

        if not title or not link:
            continue
        if is_menu_or_category_link(title, link, source_url):
            continue
        if "Notify" not in link and "notify" not in link:
            continue

        if add_result(post_date, title, link, source_url, agency, site_type):
            count += 1

        if count >= MAX_PER_SITE:
            break

    if len(results) - before > 0:
        return len(results) - before

    menu_words = [
        "관세사 · 법인 징계현황", "등록·채용 신고", "로그인",
        "회원가입", "사이트맵", "개인정보처리방침", "오시는길", "조직도",
    ]

    for row in soup.find_all(["tr", "li", "div"]):
        post_date = extract_date_from_tag(row) or extract_date_from_text(row.get_text(" ", strip=True))
        title, link = find_best_anchor(row, source_url)

        if not title or not link:
            continue
        if any(w in title for w in menu_words):
            continue
        if "Notify" not in link and "notify" not in link:
            continue

        if add_result(post_date, title, link, source_url, agency, site_type):
            count += 1

        if count >= MAX_PER_SITE:
            break

    if len(results) - before == 0:
        return crawl_fallback_links(
            source_url,
            agency,
            site_type,
            allow_keywords=["notify", "공지", "뉴스", "관세", "세관", "customs"],
        )

    return len(results) - before


def crawl_generic(source_url, agency, site_type):
    return crawl_card(source_url, agency, site_type)


def _row_link_with_title(source_url, row, title, builder=None):
    anchor = row.find("a", href=True) or row.find("a", onclick=True) or row.find("a")
    href = anchor.get("href", "") if anchor else ""
    onclick = anchor.get("onclick", "") if anchor else ""

    if builder:
        return builder(title, href, onclick)

    if href and not str(href).lower().startswith(("javascript:", "#")):
        return urljoin(source_url, href)

    joined = f"{href} {onclick}"
    match = re.search(r"(\d{4,})", joined)
    if match:
        sep = "&" if "?" in source_url else "?"
        return f"{source_url}{sep}seq={match.group(1)}"

    sep = "&" if "?" in source_url else "?"
    return f"{source_url}{sep}rowTitle={quote(clean_text(title)[:80])}"


def build_admin_rule_url(title, href="", onclick=""):
    title_clean = clean_text(title)
    title_no_space = re.sub(r"\s+", "", title_clean)
    href = str(href or "").strip()
    onclick = str(onclick or "").strip()
    joined = href + " " + onclick

    for pattern in [
        r"admRulSeq\s*[:=]\s*['\"]?(\d+)",
        r"admRulInfoP\s*\(\s*['\"]?(\d+)",
        r"admRulSeq=([0-9]+)",
        r"ADMRULSEQ\s*[:=]\s*['\"]?(\d+)",
    ]:
        match = re.search(pattern, joined, re.I)
        if match:
            return f"https://www.law.go.kr/admRulInfoP.do?admRulSeq={match.group(1)}"

    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return "https://www.law.go.kr" + href
    if href and not href.lower().startswith(("javascript:", "#")):
        return urljoin("https://www.law.go.kr/", href)
    if title_no_space:
        return "https://www.law.go.kr/admRulSc.do?query=" + quote(title_no_space)
    return "https://www.law.go.kr/admRulSc.do"


def _crawl_structured_rows(source_url, agency, site_type, builder=None, min_cells=4):
    before = len(results)

    for soup in soup_candidates(source_url):
        for row in soup.find_all("tr"):
            cells = [clean_text(cell.get_text(" ", strip=True)) for cell in row.find_all(["td", "th"])]
            if len(cells) < min_cells:
                continue
            if not re.fullmatch(r"\d+", cells[0] or ""):
                continue

            title = cells[1] if len(cells) > 1 else ""
            if not title or title.lower() in {"title", "name"}:
                continue
            if title in {"법령명", "행정규칙명", "제목", "규칙명", "사건명"}:
                continue

            post_date = None
            for cell in reversed(cells):
                post_date = normalize_date(cell) or extract_date_from_text(cell)
                if post_date:
                    break
            if not post_date:
                continue

            meta = []
            for value in cells[2:6]:
                if value and not extract_date_from_text(value) and len(value) <= 50:
                    meta.append(value)
            display_title = title if not meta else f"{title} ({', '.join(meta[:3])})"
            link = _row_link_with_title(source_url, row, title, builder)

            if add_result(post_date, display_title, link, source_url, agency, site_type):
                pass

            if len(results) - before >= MAX_PER_SITE:
                break

        if len(results) - before > 0:
            break

    return len(results) - before


def crawl_unipass_admin_notice(source_url, agency, site_type):
    """91.관세법령(행정예고): use 작성일자 column as the article date."""
    before = len(results)

    for soup in soup_candidates(source_url):
        for row in soup.find_all("tr"):
            cells = [clean_text(cell.get_text(" ", strip=True)) for cell in row.find_all(["td", "th"])]
            # 번호 / 제목 / 담당부서 / 작성일자
            if len(cells) < 4:
                continue
            if not re.fullmatch(r"\d+", cells[0] or ""):
                continue

            title = cells[1]
            department = cells[2] if len(cells) > 2 else ""
            written_date = cells[3]

            post_date = normalize_date(written_date) or extract_date_from_text(written_date)
            if not post_date:
                continue

            display_title = title
            if department and department not in display_title:
                display_title = f"{display_title} ({department})"

            link = _row_link_with_title(source_url, row, title)
            add_result(post_date, display_title, link, source_url, agency, site_type)

            if len(results) - before >= MAX_PER_SITE:
                break

        if len(results) - before > 0:
            break

    return len(results) - before


def crawl_unipass_latest_law(source_url, agency, site_type):
    """92.관세법령(최신법령): use 공포일자 column, not 시행일자."""
    before = len(results)

    for soup in soup_candidates(source_url):
        for row in soup.find_all("tr"):
            cells = [clean_text(cell.get_text(" ", strip=True)) for cell in row.find_all(["td", "th"])]
            # 번호 / 법령명 / 분야 / 종류 / 소관부처명 / 시행일자 / 공포일자 / 제개정구분
            if len(cells) < 7:
                continue
            if not re.fullmatch(r"\d+", cells[0] or ""):
                continue

            law_name = cells[1]
            field = cells[2] if len(cells) > 2 else ""
            law_kind = cells[3] if len(cells) > 3 else ""
            ministry = cells[4] if len(cells) > 4 else ""
            announce_date = cells[6] if len(cells) > 6 else ""
            change_type = cells[7] if len(cells) > 7 else ""

            post_date = normalize_date(announce_date) or extract_date_from_text(announce_date)
            if not post_date:
                continue

            meta = [value for value in [field, law_kind, ministry, change_type] if value]
            display_title = law_name if not meta else f"{law_name} ({', '.join(meta[:4])})"

            link = _row_link_with_title(source_url, row, law_name)
            add_result(post_date, display_title, link, source_url, agency, site_type)

            if len(results) - before >= MAX_PER_SITE:
                break

        if len(results) - before > 0:
            break

    return len(results) - before


def crawl_unipass(source_url, agency, site_type):
    lower_url = source_url.lower()
    if "admnrulmkplpnot" in lower_url or "openuls0502001q" in lower_url:
        return crawl_unipass_admin_notice(source_url, agency, site_type)
    if "openuls0101047q" in lower_url:
        return crawl_unipass_latest_law(source_url, agency, site_type)
    if any(key in lower_url for key in [
        "openuls0102001q",
        "openuls0105001q",
    ]):
        return _crawl_structured_rows(source_url, agency, site_type, builder=None, min_cells=4)

    before = len(results)
    for soup in soup_candidates(source_url):
        for row in soup.find_all(["tr", "li", "div", "article", "section"]):
            row_text = clean_text(row.get_text(" ", strip=True))
            if len(row_text) < 12:
                continue
            post_date = extract_date_from_tag(row) or extract_date_from_text(row_text)
            if not post_date:
                continue
            title, link = find_best_anchor(row, source_url)
            if not title:
                cells = [clean_text(cell.get_text(" ", strip=True)) for cell in row.find_all(["td", "th"])]
                title_candidates = [c for c in cells if c and not extract_date_from_text(c) and not re.fullmatch(r"[\d,.\-\s]+", c)]
                title = max(title_candidates, key=len) if title_candidates else ""
                link = _row_link_with_title(source_url, row, title)
            if add_result(post_date, title, link, source_url, agency, site_type):
                pass
            if len(results) - before >= MAX_PER_SITE:
                break
        if len(results) - before > 0:
            break
    return len(results) - before


def crawl_law_admin_rules(source_url, agency, site_type):
    return _crawl_structured_rows(source_url, agency, site_type, builder=build_admin_rule_url, min_cells=6)


def crawl_law_structured_table(source_url, agency, site_type):
    return _crawl_structured_rows(source_url, agency, site_type, builder=build_law_url, min_cells=5)


def crawl_law(source_url, agency, site_type):
    lower_url = source_url.lower()
    if "admrulsc.do" in lower_url:
        return crawl_law_admin_rules(source_url, agency, site_type)
    if "lssc.do" in lower_url or "eflspop.do" in lower_url or "nw" in lower_url:
        count = crawl_law_structured_table(source_url, agency, site_type)
        if count:
            return count

    before = len(results)
    for soup in soup_candidates(source_url):
        for row in soup.find_all(["tr", "li", "div"]):
            row_text = clean_text(row.get_text(" ", strip=True))
            if len(row_text) < 15:
                continue
            post_date = extract_date_from_tag(row) or extract_date_from_text(row_text)
            title, link = find_best_anchor(row, source_url)
            if not title:
                continue
            if not post_date:
                continue
            link = link or _row_link_with_title(source_url, row, title, build_law_url)
            add_result(post_date, title, link, source_url, agency, site_type)
            if len(results) - before >= MAX_PER_SITE:
                break
        if len(results) - before > 0:
            break
    return len(results) - before


def crawl_korea(source_url, agency, site_type):
    if "unipass.customs.go.kr" in source_url.lower():
        return crawl_unipass(source_url, agency, site_type)

    before = len(results)
    html = ""
    params = get_query_params(source_url)
    full_url = source_url

    try:
        html, params, full_url = fetch_korea_html(source_url)
        parser = CustomsBoardParser(params)
        parser.feed(html)
        for post in parser.posts:
            add_result(
                date_value=post.posted_at,
                title=f"[{post.category}] {post.title}" if post.category else post.title,
                url=post.detail_url,
                source=full_url,
                agency=agency,
                site_type=site_type,
            )
    except Exception as e:
        print("   ERROR:", e)
        res = fetch_html(source_url)
        if res:
            html = res.text
            params = get_query_params(source_url)

    if len(results) - before > 0:
        return len(results) - before

    if not html:
        return 0

    soup = BeautifulSoup(html, "html.parser")
    for row in soup.find_all("tr"):
        cells = [clean_text(td.get_text(" ", strip=True)) for td in row.find_all(["td", "th"])]
        if len(cells) < 3:
            continue

        post_date = None
        for cell in reversed(cells):
            post_date = parse_korea_date(cell)
            if post_date:
                break
        if not post_date:
            continue

        anchor = row.find("a", class_=lambda c: c and "nttInfoBtn" in c) or row.find("a")
        title = clean_text(anchor.get("title") or anchor.get_text(" ", strip=True)) if anchor else cells[1]
        if not is_valid_title(title):
            continue

        href = anchor.get("href", "") if anchor else ""
        if href and not str(href).lower().startswith(("javascript:", "#")):
            link = urljoin(source_url, href)
        else:
            ntt_sn = anchor.get("data-id", "") if anchor else ""
            ntt_sn_url = anchor.get("data-url", "") if anchor else ""
            detail_params = dict(params)
            if ntt_sn:
                detail_params["nttSn"] = ntt_sn
            if ntt_sn_url:
                detail_params["nttSnUrl"] = ntt_sn_url
            link = f"{KOREA_DETAIL_URL}?{urlencode(detail_params)}"

        add_result(post_date, title, link, full_url, agency, site_type)
        if len(results) - before >= MAX_PER_SITE:
            break

    return len(results) - before


def infer_parser(type_value, parser):
    p = str(parser or "").strip().lower()
    t = str(type_value or "").strip().lower()

    if p and p != "nan":
        return p

    if t in ["korea", "korea_press", "korea_fta", "korea_notice"]:
        return "customs_board"
    if t == "ustr":
        return "ustr_parser"
    if t == "cbp":
        return "cbp_parser"
    if t in ["eu", "eu_taxud"]:
        return "eu_parser"
    if t == "wto":
        return "wto_parser"
    if t == "rss":
        return "rss_parser"
    if t == "gwanbo":
        return "gwanbo_selenium_parser"
    if t == "motie":
        return "motie_parser"
    if t == "law":
        return "law_parser"
    if t == "law_decision":
        return "law_decision_parser"
    if t == "krcaa":
        return "krcaa_parser"
    if t == "custra":
        return "custra_parser"
    if t == "unipass":
        return "unipass_parser"
    if t == "oecd":
        return "oecd_parser"
    if t == "usitc":
        return "usitc_parser"
    if t == "nsp":
        return "nsp_parser"
    if t == "table":
        return "table_date"

    return "generic_html"


def normalize_source_config(source_url, type_value, parser, site_type):
    url_l = str(source_url or "").strip().lower()
    t = str(type_value or "").strip().lower()
    p = str(parser or "").strip().lower()
    st = normalize_site_type(site_type)

    if "unipass.customs.go.kr" in url_l:
        return "unipass", "unipass_parser", "regulation"

    if "law.go.kr/alldeccsc.do" in url_l:
        return "law_decision", "law_decision_parser", "regulation"

    if "customs.go.kr" in url_l and "unipass.customs.go.kr" not in url_l:
        if "selectnttlist.do" in url_l or "/na/ntt/" in url_l:
            return "korea", "customs_board", st

    if "cbp.gov/newsroom" in url_l:
        return "cbp", "cbp_parser", "news"

    if (
        "cbp.gov/trade/rulings" in url_l
        or "federal-register-notices" in url_l
        or t == "cbp_ruling"
        or p == "cbp_ruling_parser"
    ):
        return "cbp_ruling", "cbp_ruling_parser", "regulation"

    return t, p, st


def get_status(count):
    if count == 0:
        return "NO_NEW"
    if count <= 2:
        return "CHECK"
    return "OK"


def save_split_files(df):
    """STEP1 is regulation-only. Never create 1-2.site_news_raw.xlsx."""
    regulation_df = df[df["site_type"] == "regulation"].copy()
    regulation_df.to_excel(OUT_ALL_FILE, index=False)
    regulation_df.to_excel(OUT_REG_FILE, index=False)


def get_recent_post_col(df):
    for col in df.columns:
        name = str(col)
        if "최근게시일" in name or ("recent" in name.lower() and "post" in name.lower()):
            return col
    return None


def apply_site_recent_date(start_idx, agency, site_type, site_recent_dt):
    if site_type != "news" or not site_recent_dt or not is_recent(site_recent_dt):
        return

    for item in results[start_idx:]:
        if item.get("agency") != agency:
            continue
        if item.get("site_type") != "news":
            continue
        if item.get("date_status") != "no_date":
            continue
        if is_menu_or_category_link(item.get("title", ""), item.get("url", ""), item.get("source", "")):
            continue

        item["date"] = site_recent_dt.strftime("%Y-%m-%d %H:%M:%S")
        item["date_status"] = "recent"


def split_final_rows(df):
    """Keep only final article rows for raw output; move diagnostics/no-date/old rows aside."""
    if df.empty:
        return df.copy(), df.copy()

    work = df.copy()
    reasons = []

    for _, row in work.iterrows():
        row_reasons = []
        title = clean_text(row.get("title", ""))
        url = normalize_url_for_compare(row.get("url", ""))
        source = normalize_url_for_compare(row.get("source", ""))
        date_status = clean_text(row.get("date_status", ""))

        if date_status != "recent":
            row_reasons.append(f"date_status={date_status or 'blank'}")
        if "최신 게시물 확인 필요" in title:
            row_reasons.append("diagnostic_hint_title")
        if is_menu_or_category_link(title, row.get("url", ""), row.get("source", "")):
            row_reasons.append("menu_or_category_link")
        if clean_text(row.get("site_type", "")) == "regulation" and not is_policy_content_candidate(title):
            row_reasons.append("non_policy_content")
        parsed_dt = normalize_date(row.get("date", ""))
        if has_url_date_conflict(row.get("url", ""), parsed_dt):
            row_reasons.append("url_date_conflict")
        if url and source and url == source:
            row_reasons.append("url_equals_source")
        if "#" in str(row.get("url", "")):
            row_reasons.append("fragment_or_menu_url")

        reasons.append("; ".join(row_reasons))

    work["_final_exclude_reason"] = reasons
    excluded = work[work["_final_exclude_reason"] != ""].copy()
    final = work[work["_final_exclude_reason"] == ""].copy()

    final.drop(columns=["_final_exclude_reason"], inplace=True)

    return final, excluded


def build_policy_candidates(excluded_df):
    """Preserve official policy signals that lack date/detail proof without promoting them."""
    columns = [
        "date", "title", "url", "source", "collected_at", "agency",
        "site_type", "date_status", "CandidateStatus", "CandidateReason",
        "PolicySignal", "EligibleForFinalReport",
    ]
    if excluded_df is None or excluded_df.empty:
        return pd.DataFrame(columns=columns)

    policy_terms = re.compile(
        r"관세|통관|수입|수출|원산지|고시|공고|규정|개정|입법|행정예고|"
        r"customs|tariff|trade|import|export|origin|notification|notice|"
        r"regulation|rules?|order|circular|anti[- ]?dumping|countervailing|"
        r"safeguard|warehous",
        re.I,
    )
    non_policy_terms = re.compile(
        r"recruit|examination|fellowship|training|vacancy|promotion of|seniority|"
        r"sports|cultural|tender|procurement|holiday|employee|"
        r"call for contributions?|call for papers?|newsletter|customs news|"
        r"issue no\.?|journal|conference|seminar|workshop|webinar|nomination|"
        r"^federal register notices?$",
        re.I,
    )
    rows = []
    for _, row in excluded_df.iterrows():
        if clean_text(row.get("site_type", "")) != "regulation":
            continue
        date_status = clean_text(row.get("date_status", ""))
        title = clean_text(row.get("title", ""))
        agency = clean_text(row.get("agency", ""))

        # Recent USTR releases may be meaningful official trade-policy signals
        # even when they do not announce a legally effective measure.  Preserve
        # them for Global Context review, but never promote them into the final
        # regulation feed or Samsung Action Queue at STEP1.
        ustr_context_terms = re.compile(
            r"tariff|tariff.rate quota|\btrq\b|quota allocation|section 301|"
            r"public comment|joint (?:ministerial )?statement|excess capacity|"
            r"coercive trade actions?|forced labor import prohibitions?|"
            r"trade agreement|usmca|wto|trade barriers?",
            re.I,
        )
        if (
            agency.startswith("53.USTR")
            and date_status == "recent"
            and ustr_context_terms.search(title)
            and not non_policy_terms.search(title)
        ):
            item = {key: row.get(key, "") for key in columns[:8]}
            item.update({
                "CandidateStatus": "GLOBAL_CONTEXT_REVIEW",
                "CandidateReason": "OFFICIAL_POLICY_SIGNAL_REQUIRES_CHANGE_VERIFICATION",
                "PolicySignal": "Y",
                "EligibleForFinalReport": "N",
            })
            rows.append(item)
            continue

        if date_status != "no_date":
            continue
        item_url = normalize_url_for_compare(row.get("url", ""))
        source_url = normalize_url_for_compare(row.get("source", ""))
        same_official_landing = bool(item_url and source_url and item_url == source_url)
        if is_menu_or_category_link(title, row.get("url", ""), row.get("source", "")) and not same_official_landing:
            continue
        if has_stale_explicit_title_year(title):
            continue
        if has_stale_official_url_date(row.get("url", "")):
            continue
        if not is_policy_content_candidate(title):
            continue
        if not policy_terms.search(title) or non_policy_terms.search(title):
            continue
        item = {key: row.get(key, "") for key in columns[:8]}
        item.update({
            "CandidateStatus": "URGENT_VERIFICATION",
            "CandidateReason": "OFFICIAL_POST_DATE_OR_DETAIL_URL_PENDING",
            "PolicySignal": "Y",
            "EligibleForFinalReport": "N",
        })
        rows.append(item)
    return pd.DataFrame(rows, columns=columns)


def is_menu_only_extraction(excluded_rows, final_valid_count=0):
    """True when a parser returned navigation labels but no real post."""
    if final_valid_count or excluded_rows is None or excluded_rows.empty:
        return False
    if "_final_exclude_reason" not in excluded_rows.columns:
        return False
    return excluded_rows["_final_exclude_reason"].fillna("").str.contains(
        "menu_or_category_link", regex=False
    ).all()



def update_sites_status(sites, idx, count, site_recent_dt=None):
    """Update sites.xlsx monitoring fields for the current site."""
    prev_total = pd.to_numeric(sites.at[idx, "total_collected"], errors="coerce")
    if pd.isna(prev_total):
        prev_total = 0
    sites.at[idx, "collected_count"] = int(count)
    sites.at[idx, "total_collected"] = int(prev_total) + int(count)
    sites.at[idx, "last_checked"] = now_str()

    if site_recent_dt is not None:
        col = get_recent_post_col(sites)
        if col:
            sites.at[idx, col] = site_recent_dt.strftime("%Y-%m-%d")

    sites.at[idx, "status"] = get_status(count)


def save_sites_status(sites):
    try:
        sites.to_excel(SITE_FILE, index=False)
        print(f"💾 sites.xlsx UPDATE OK : {SITE_FILE}")
    except PermissionError:
        alt_sites = BASE_DIR / "sites_updated.xlsx"
        sites.to_excel(alt_sites, index=False)
        print("❌ sites.xlsx가 열려 있어 원본 저장 실패")
        print(f"💾 대체 저장 : {alt_sites}")
        raise




def is_step1_regulation_site(row):
    """Only STEP1 regulation sites are crawled by 1.site_crawler."""
    owner = clean_text(row.get("owner_step", "")).upper()
    track = clean_text(row.get("track", "")).lower()
    official = clean_text(row.get("official_flag", "")).upper()
    site_type = clean_text(row.get("site_type", "")).lower()

    if owner:
        return owner == "STEP1"
    if track:
        return track == "regulation"
    if official:
        return official in {"Y", "YES", "1", "TRUE"}
    return site_type in {"regulation", "reg", "law", "official", "official_government"}


def calc_site_final_status(final_df, site_name, source_url):
    """Return final-valid count and latest actual post date for one STEP1 site."""
    if final_df is None or final_df.empty:
        return 0, None

    agency = clean_text(site_name)
    src_key = normalize_url_for_compare(source_url)
    mask = final_df["agency"].astype(str).map(clean_text).eq(agency)

    if "source" in final_df.columns and src_key:
        source_match = final_df["source"].astype(str).map(normalize_url_for_compare).eq(src_key)
        if source_match.any():
            mask = mask & source_match

    rows = final_df[mask].copy()
    if rows.empty:
        return 0, None

    dt = pd.to_datetime(rows["date"], errors="coerce")
    latest = dt.max()
    if pd.isna(latest):
        latest = None
    elif hasattr(latest, "to_pydatetime"):
        latest = latest.to_pydatetime()

    return len(rows), latest


def apply_final_sites_status(sites, final_df, checked_indices):
    """
    Update sites.xlsx from FINAL valid regulation rows, not RAW crawl rows.
    collected_count = final valid rows this run
    total_collected = previous total + final valid rows
    last_checked = actual crawl execution time
    최근게시일 = latest actual collected post date
    """
    recent_col = get_recent_post_col(sites)

    for idx in checked_indices:
        row = sites.loc[idx]
        count, latest_dt = calc_site_final_status(
            final_df,
            clean_text(row.get("site_name", "")),
            clean_text(row.get("url", "")),
        )

        prev_total = pd.to_numeric(sites.at[idx, "total_collected"], errors="coerce")
        if pd.isna(prev_total):
            prev_total = 0

        sites.at[idx, "collected_count"] = int(count)
        sites.at[idx, "total_collected"] = int(prev_total) + int(count)
        sites.at[idx, "last_checked"] = now_str()
        run_state = site_run_status.get(idx, {})
        # A successful fetch with zero new/valid posts is normal. FAIL is
        # reserved for transport/parser failure after rescue also failed.
        sites.at[idx, "status"] = (
            "FAIL" if run_state.get("failed") else (
                "CHECK_REQUIRED" if run_state.get("check_required") else (
                    "PARTIAL_COVERAGE" if run_state.get("partial") else get_status(count)
                )
            )
        )
        if "status_detail" not in sites.columns:
            sites["status_detail"] = ""
        sites.at[idx, "status_detail"] = run_state.get("detail", "")

        if recent_col and latest_dt is not None:
            sites.at[idx, recent_col] = latest_dt.strftime("%Y-%m-%d %H:%M:%S")


def main():
    print("🚀 GTI STEP1 SITES MODE START")

    BASE_DIR.mkdir(parents=True, exist_ok=True)

    if not SITE_FILE.exists():
        print(f"❌ sites.xlsx 없음: {SITE_FILE}")
        return

    sites = pd.read_excel(SITE_FILE)
    sites = sites.loc[:, ~sites.columns.astype(str).str.contains("^Unnamed")]
    sites = sites.loc[:, ~sites.columns.astype(str).str.endswith(".1")]

    for col in ["parser", "site_type", "collected_count", "total_collected", "last_checked", "status"]:
        if col not in sites.columns:
            sites[col] = ""

    recent_post_col = get_recent_post_col(sites)

    total_before = len(results)
    active_count = 0
    skipped_count = 0
    routed_skip_count = 0
    checked_indices = []

    for idx, row in sites.iterrows():
        active = row.get("active", 1)

        if not is_active_value(active):
            skipped_count += 1
            print(f"[SKIP] inactive: {row.get('site_name', '')}")
            continue

        if not is_step1_regulation_site(row):
            routed_skip_count += 1
            print(
                f"[SKIP] non-STEP1: {row.get('site_name', '')} "
                f"/ owner_step={row.get('owner_step', '')} "
                f"/ track={row.get('track', '')}"
            )
            continue

        active_count += 1
        checked_indices.append(idx)

        site_name = clean_text(row.get("site_name", ""))
        source_url = str(row.get("url", "")).strip()

        type_value = str(row.get("type", "generic")).strip().lower()
        type_value, fixed_parser, site_type = normalize_source_config(
            source_url,
            type_value,
            row.get("parser", ""),
            "regulation",
        )
        site_type = "regulation"
        parser = infer_parser(type_value, fixed_parser)
        agency = site_name

        sites.at[idx, "type"] = type_value
        sites.at[idx, "parser"] = parser
        sites.at[idx, "site_type"] = site_type

        if not source_url.startswith("http"):
            count = 0
            continue

        print(f"[CRAWL] {site_name} / type={type_value} / parser={parser} / site_type={site_type}")

        crawl_start_idx = len(results)
        site_recent_dt = parse_site_recent_date(row.get(recent_post_col, "")) if recent_post_col else None

        primary_failed = False
        primary_error = ""
        try:
            if parser == "customs_board":
                count = crawl_korea(source_url, agency, site_type)
            elif parser == "ustr_parser":
                count = crawl_ustr(source_url, agency, site_type)
            elif parser == "cbp_parser":
                count = crawl_cbp(source_url, agency, site_type)
            elif parser == "cbp_ruling_parser":
                count = crawl_cbp_ruling(source_url, agency, site_type)
            elif parser == "eu_parser":
                count = crawl_eu(source_url, agency, site_type)
            elif parser == "wto_parser":
                count = crawl_wto(source_url, agency, site_type)
            elif parser == "rss_parser":
                count = crawl_rss(source_url, agency, site_type)
            elif parser == "card_parser":
                if "cbic" in agency.lower() or "cbic.gov.in" in source_url.lower():
                    count = crawl_cbic(source_url, agency, site_type)
                else:
                    count = crawl_card(source_url, agency, site_type)
            elif parser == "gwanbo_parser":
                count = crawl_gwanbo(source_url, agency, site_type)
            elif parser == "gwanbo_selenium_parser":
                count = crawl_gwanbo_selenium(source_url, agency, site_type)
            elif parser == "motie_parser":
                count = crawl_motie(source_url, agency, site_type)
            elif parser == "law_parser":
                count = crawl_law(source_url, agency, site_type)
            elif parser == "law_decision_parser":
                count = crawl_law_decision(source_url, agency, site_type)
            elif parser == "krcaa_parser":
                count = crawl_krcaa(source_url, agency, site_type)
            elif parser == "custra_parser":
                count = crawl_custra(source_url, agency, site_type)
            elif parser == "unipass_parser":
                count = crawl_unipass(source_url, agency, site_type)
            elif parser == "oecd_parser":
                count = crawl_oecd(source_url, agency, site_type)
            elif parser == "usitc_parser":
                count = crawl_usitc(source_url, agency, site_type)
            elif parser == "nsp_parser":
                count = crawl_nsp(source_url, agency, site_type)
            elif parser == "table_date":
                if "opinion.lawmaking.go.kr/gcom/nsmlmsts/out" in source_url.lower():
                    count = crawl_lawmaking_bills(source_url, agency, site_type)
                elif "fta.motir.go.kr/ftamain/promo/news/trend" in source_url.lower():
                    count = crawl_fta_trend(source_url, agency, site_type)
                else:
                    count = crawl_table(source_url, agency, site_type)
            else:
                count = crawl_generic(source_url, agency, site_type)

        except Exception as e:
            print(f"   ERROR: {e}")
            count = 0
            primary_failed = True
            primary_error = f"{type(e).__name__}: {e}"

        rescue_strategy = "PRIMARY_PARSER"
        if count == 0:
            try:
                count, rescue_strategy = crawl_resilient_new_posts(source_url, agency, site_type)
                print(f"   [NEW POST RESCUE] {rescue_strategy} / {count}건")
            except Exception as rescue_exc:
                count = 0
                rescue_strategy = "RESCUE_FAILED"
                primary_failed = True
                primary_error = (primary_error + " | " if primary_error else "") + f"RESCUE:{type(rescue_exc).__name__}: {rescue_exc}"

        coverage = site_coverage_status.get(source_url, {})
        partial_coverage = coverage.get("coverage_status") == "PARTIAL_COVERAGE"
        zero_diagnostic = {}
        if count == 0 and rescue_strategy != "RESCUE_FAILED":
            zero_diagnostic = diagnose_zero_yield(source_url, site_recent_dt)
        zero_status = zero_diagnostic.get("zero_yield_status", "")
        silent_failure = zero_status == "CHECK_REQUIRED"
        run_failed = (
            primary_failed and count == 0 and rescue_strategy == "RESCUE_FAILED"
        ) or zero_status == "FAIL"
        site_run_status[idx] = {
            "failed": run_failed,
            "partial": partial_coverage or silent_failure,
            "check_required": silent_failure,
            "detail": primary_error if run_failed else (
                coverage.get("coverage_error", "PARTIAL_COVERAGE")
                if partial_coverage else (
                    zero_diagnostic.get("zero_yield_reason", "CHECK_REQUIRED")
                    if silent_failure else ("NO_NEW" if count == 0 else rescue_strategy)
                )
            ),
        }

        health_row = {
            "checked_at": now_str(), "site": site_name, "url": source_url,
            "parser": parser, "primary_or_rescue": rescue_strategy,
            "real_posts_found": count,
            "status": "FAIL" if run_failed else (
                "PARTIAL_COVERAGE" if partial_coverage else (
                    "CHECK_REQUIRED" if silent_failure else ("OK" if count > 0 else "NO_NEW")
                )
            ),
            "error": primary_error or zero_diagnostic.get("zero_yield_reason", "") if run_failed else "",
            "latest_date_hint": clean_text(row.get(recent_post_col, "")) if recent_post_col else "",
        }
        health_row.update(coverage)
        health_row.update(zero_diagnostic)
        crawl_health.append(health_row)

        print(f" → {count}건")

        apply_site_recent_date(crawl_start_idx, agency, site_type, site_recent_dt)

        # sites.xlsx status is updated after final filtering.
        time.sleep(SLEEP_SEC)

    df = pd.DataFrame(
        results,
        columns=[
            "date",
            "title",
            "url",
            "source",
            "collected_at",
            "agency",
            "site_type",
            "date_status",
        ],
    )

    raw_count = len(df)

    if df.empty:
        print("❌ No data")
        print(f"📌 active 대상: {active_count}")
        print(f"📌 inactive skip: {skipped_count}")
        apply_final_sites_status(
            sites,
            pd.DataFrame(columns=[
                "date","title","url","source","collected_at",
                "agency","site_type","date_status"
            ]),
            checked_indices,
        )
        save_sites_status(sites)

        if rejects:
            pd.DataFrame(rejects).to_excel(REJECT_FILE, index=False)
            print(f"🧾 제외 로그: {REJECT_FILE}")

        pd.DataFrame(columns=[
            "date", "title", "url", "source", "collected_at", "agency",
            "site_type", "date_status", "CandidateStatus", "CandidateReason",
            "PolicySignal", "EligibleForFinalReport",
        ]).to_excel(POLICY_CANDIDATES_FILE, index=False)
        if crawl_health:
            for health in crawl_health:
                health["final_valid_count"] = 0
                health["final_excluded_count"] = 0
                health["final_exclude_reasons"] = ""
                health["final_status"] = "FAIL" if health.get("status") == "FAIL" else "NO_NEW"
            pd.DataFrame(crawl_health).to_excel(CRAWL_HEALTH_FILE, index=False)
        print(f"📁 공식 정책 후보: {POLICY_CANDIDATES_FILE} / 0건")
        print(f"🩺 사이트 탐지 진단: {CRAWL_HEALTH_FILE}")

        return

    df = explode_joined_gwanbo_rows(df)
    df = df.drop_duplicates(subset=["url", "title"])
    dedup_count = len(df)
    dup_removed = raw_count - dedup_count

    df = df[
        [
            "date",
            "title",
            "url",
            "source",
            "collected_at",
            "agency",
            "site_type",
            "date_status",
        ]
    ]

    audit_df = df.copy()
    final_df, excluded_df = split_final_rows(df)
    candidate_df = build_policy_candidates(excluded_df)
    final_count = len(final_df)
    final_filter_removed = len(excluded_df)

    if crawl_health:
        final_by_agency = final_df.groupby("agency").size().to_dict() if not final_df.empty else {}
        excluded_by_agency = excluded_df.groupby("agency").size().to_dict() if not excluded_df.empty else {}
        excluded_reason_by_agency = {}
        if not excluded_df.empty and "_final_exclude_reason" in excluded_df.columns:
            for agency_name, group in excluded_df.groupby("agency"):
                counts = group["_final_exclude_reason"].fillna("UNKNOWN").value_counts()
                excluded_reason_by_agency[agency_name] = " | ".join(
                    f"{reason}:{int(count)}" for reason, count in counts.items()
                )
        for health in crawl_health:
            agency_name = health.get("site", "")
            agency_audit = audit_df[audit_df["agency"].astype(str).map(clean_text).eq(clean_text(agency_name))]
            if not agency_audit.empty:
                actual_dates = pd.to_datetime(agency_audit["date"], errors="coerce").dropna()
                if not actual_dates.empty:
                    health["latest_date_hint"] = actual_dates.max().strftime("%Y-%m-%d %H:%M:%S")
            valid_n = int(final_by_agency.get(agency_name, 0))
            excluded_n = int(excluded_by_agency.get(agency_name, 0))
            health["final_valid_count"] = valid_n
            health["final_excluded_count"] = excluded_n
            health["final_exclude_reasons"] = excluded_reason_by_agency.get(agency_name, "")
            agency_excluded = excluded_df[
                excluded_df["agency"].astype(str).map(clean_text).eq(clean_text(agency_name))
            ] if not excluded_df.empty else pd.DataFrame()
            menu_only_extraction = is_menu_only_extraction(agency_excluded, valid_n)
            if health.get("status") == "FAIL":
                health["final_status"] = "FAIL"
            elif health.get("status") == "CHECK_REQUIRED":
                health["final_status"] = "CHECK_REQUIRED"
            elif health.get("status") == "PARTIAL_COVERAGE":
                health["final_status"] = "PARTIAL_COVERAGE"
            elif valid_n > 0:
                health["final_status"] = "VALID_REGULATION"
            elif menu_only_extraction:
                health["status"] = "PARSE_ZERO"
                health["final_status"] = "PARSE_ZERO"
                health["zero_yield_reason"] = "PARSER_RETURNED_MENU_LINKS_ONLY"
            elif int(health.get("real_posts_found", 0) or 0) > 0:
                health["final_status"] = "ALL_FILTERED"
            else:
                health["final_status"] = "NO_NEW"

    try:
        audit_df.to_excel(OUT_AUDIT_FILE, index=False)
        save_split_files(final_df)
        if not excluded_df.empty:
            excluded_df.to_excel(FINAL_EXCLUDED_FILE, index=False)
        candidate_df.to_excel(POLICY_CANDIDATES_FILE, index=False)
    except PermissionError:
        alt = BASE_DIR / "1.site_news_raw_new.xlsx"
        final_df.to_excel(alt, index=False)
        print(f"⚠ 결과 파일 열림 → 대체 저장: {alt}")

    if rejects:
        try:
            pd.DataFrame(rejects).to_excel(REJECT_FILE, index=False)
        except PermissionError:
            pass

    try:
        pd.DataFrame(crawl_health).to_excel(CRAWL_HEALTH_FILE, index=False)
    except PermissionError:
        print(f"⚠ crawl health 파일 열림: {CRAWL_HEALTH_FILE}")

    apply_final_sites_status(sites, final_df, checked_indices)
    save_sites_status(sites)

    print(f"📌 STEP1 regulation 대상: {active_count}")
    print(f"📌 inactive skip: {skipped_count}")
    print(f"📌 STEP2-3/news routing skip: {routed_skip_count}")
    print(f"📊 RAW 수집: {raw_count}")
    print(f"🧹 중복 제거: {dup_removed}")
    print(f"🧹 최종 필터 제외: {final_filter_removed}")
    print(f"✅ 최종 저장: {final_count}")
    if not final_df.empty:
        overseas_final = final_df[final_df["agency"].map(is_overseas_agency)]
        print(f"🌏 해외 법규 최종 저장: {len(overseas_final)}건 / {overseas_final['agency'].nunique()}개 기관")
        if not overseas_final.empty:
            overseas_breakdown = overseas_final.groupby("agency").size().sort_values(ascending=False)
            print("🌏 해외 기관별: " + " | ".join(f"{agency}={int(count)}" for agency, count in overseas_breakdown.items()))
    print(f"📁 전체 파일: {OUT_ALL_FILE}")
    print(f"📁 감사 파일: {OUT_AUDIT_FILE}")
    print(f"📁 법규 파일: {OUT_REG_FILE}")
    print(f"📁 최종 제외 파일: {FINAL_EXCLUDED_FILE}")
    print(f"📁 공식 정책 후보: {POLICY_CANDIDATES_FILE} / {len(candidate_df)}건")
    print("📌 sites.xlsx 업데이트 기준 = 최종 유효 법규: collected_count / total_collected / last_checked / 최근게시일")
    print(f"🧾 제외 로그: {REJECT_FILE}")
    print(f"🩺 사이트 탐지 진단: {CRAWL_HEALTH_FILE}")
    print(f"📊 RAW 증가: {len(results) - total_before}")


if __name__ == "__main__":
    main()
