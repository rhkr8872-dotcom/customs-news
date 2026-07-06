# -*- coding: utf-8 -*-
# GTI FINAL CORE v6 - LAW1 official regulation, rest news
r"""
GTI STEP1 LAW1-ONLY + NEWS-REST CLEAN v6 - official regulation sensing

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

Search period:
- HOURS_BACK = 72

Step design:
- STEP1 collects broadly and removes only obvious noise/menu/old/cumulative URL duplicates.
- STEP1 internal deduplication is URL-only.
- STEP3/STEP4 should perform stricter similar-title clustering, Top30 selection, and AI impact analysis.
"""

import re
import time
import warnings
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import URLError
from urllib.parse import parse_qs, quote, urlencode, urljoin, urlparse
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


BASE_DIR = Path(r"C:\temp")
SITE_FILE = BASE_DIR / "sites.xlsx"
KEYWORD_FILE_CANDIDATES = [
    BASE_DIR / "keyword.xlsx",
    BASE_DIR / "KEYWORD.xlsx",
    BASE_DIR / "custom_queries.xlsx",
]
CUMULATIVE_FILE = BASE_DIR / "1.site_news_cumulative.xlsx"

OUT_ALL_FILE = BASE_DIR / "1.site_news_raw.xlsx"
OUT_AUDIT_FILE = BASE_DIR / "1.site_news_audit.xlsx"
OUT_REG_FILE = BASE_DIR / "1-1.regulation_raw.xlsx"
OUT_REG_NEW_FILE = BASE_DIR / "1-1.regulation_new_raw.xlsx"
OUT_NEWS_FILE = BASE_DIR / "1-2.site_news_raw.xlsx"  # non-LAW1 rows are news/reference candidates
REJECT_FILE = BASE_DIR / "1.site_news_reject_debug.xlsx"
FINAL_EXCLUDED_FILE = BASE_DIR / "1.site_news_final_excluded.xlsx"

HOURS_BACK = int(os.getenv("GTI_STEP1_HOURS_BACK", "72"))
MAX_PER_SITE = 30
MAX_GWANBO_ITEMS = 250
SLEEP_SEC = 0.5

results = []
rejects = []

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
}

TRADE_WORDS = [
    "관세", "통관", "수입", "수출", "무역", "통상", "고시", "공고",
    "훈령", "예규", "입법예고", "행정예고", "FTA", "원산지",
    "customs", "tariff", "trade", "import", "export", "notice",
    "regulation", "announcement", "directive", "policy",
    "news", "press", "release", "commission", "investigation",
]

# GTI Step1 final relevance controls
# - keyword.xlsx/custom_queries.xlsx is loaded in main() and combined with these defaults.
# - CBP and generic agency news are filtered more strictly to prevent enforcement/crime/travel noise.
POLICY_CORE_KEYWORDS = [
    "customs", "tariff", "duty", "duties", "trade", "import", "export", "origin",
    "rules of origin", "fta", "free trade agreement", "hs", "hscode", "hs code",
    "classification", "ruling", "federal register", "notice", "regulation", "directive",
    "anti-dumping", "antidumping", "countervailing", "safeguard", "section 301",
    "section 232", "forced labor", "uflpa", "entity list", "export control", "sanction",
    "관세", "통관", "수입", "수출", "무역", "통상", "원산지", "품목분류", "HS", "FTA",
    "고시", "공고", "훈령", "예규", "입법예고", "행정예고", "덤핑", "상계관세", "세이프가드",
]

STRICT_AGENCY_HOST_HINTS = [
    "cbp.gov/newsroom", "cbp.gov/news", "cbp.gov/frontline", "cbp.gov/travel",
]

NOISE_NEWS_KEYWORDS = [
    "cocaine", "methamphetamine", "fentanyl", "marijuana", "drug", "narcotics", "arrest", "seized",
    "seizure", "sentenced", "prison", "wanted", "missing", "migrant", "border patrol", "checkpoint",
    "black hawk", "firearms", "airport wait times", "travelers", "insect", "leafy greens", "pest",
    "코카인", "마약", "체포", "검거", "압수", "밀수", "여행객", "해충", "병해충",
]

MENU_TEXT_PATTERNS = [
    "전체 관세청 유관기관", "--전체--", "유관기관", "관련사이트", "정부기관", "패밀리사이트",
    "본문 바로가기", "주메뉴", "누리집", "알림판", "전체메뉴", "검색어를 입력",
    "home >", "breadcrumb", "skip navigation", "all rights reserved",
]

MAX_TITLE_LEN = 180

# Step1 운영 원칙: 수집단계는 러프하게 유지한다.
# - 명백한 메뉴/오래된 날짜/기관 잡뉴스/누적 중복만 제거
# - 유사뉴스 클러스터링, Top30 선정, 삼성 영향도 분석은 STEP3/STEP4에서 수행
STEP1_STRICT_POLICY_FILTER = False
MIN_POLICY_SCORE_STRICT_AGENCY = 1
# News old-date handling: keep old news if it still has meaningful trade-policy signal.
# Exact similar-topic consolidation belongs to STEP3/STEP4.
NEWS_OLD_DATE_KEEP_MIN_POLICY_SCORE = 2

HIGH_VALUE_POLICY_TERMS = [
    "entity list", "export control", "uflpa", "forced labor", "section 301", "section 232",
    "anti-dumping", "antidumping", "countervailing", "safeguard", "ruling", "federal register",
    "classification", "hs code", "tariff", "customs valuation", "origin", "fta",
    "관세율", "품목분류", "원산지", "수출통제", "환급", "덤핑", "상계관세", "세이프가드",
]

MEDIUM_VALUE_POLICY_TERMS = [
    "customs", "trade", "import", "export", "duty", "duties", "notice", "regulation", "policy",
    "관세", "통관", "수입", "수출", "무역", "통상", "고시", "공고", "행정예고", "입법예고",
]


def clean_text(value: str, max_len: int | None = None) -> str:
    if value is None:
        return ""
    value = unescape(str(value))
    value = value.replace("\xa0", " ").replace("\u200b", "")
    value = re.sub(r"\s+", " ", value).strip()
    if max_len is not None and max_len > 0 and len(value) > max_len:
        return value[:max_len].rstrip()
    return value


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


def is_recent(dt):
    if dt is None:
        return False
    if isinstance(dt, date) and not isinstance(dt, datetime):
        dt = datetime.combine(dt, datetime.min.time())
    now = datetime.now()
    # Most government boards expose only a calendar date, not a posting time.
    # Treat HOURS_BACK as an inclusive day window so a 72-hour setting includes
    # the whole day 3 days ago, e.g. May 29 run includes all of May 26.
    days_back = max(0, int(HOURS_BACK // 24))
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
    ]
    if any(x in url_l for x in menu_url_parts):
        return True

    if url_l == source_l:
        return True

    return False


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


def add_result(date_value, title, url, source, agency, site_type):
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

    if dt is None:
        date_out = ""
        date_status = "no_date"
    elif dt.year < 2000 or dt.year > datetime.now().year + 2:
        add_reject("implausible_date", date_value, title, url, source, agency, site_type)
        date_out = ""
        date_status = "no_date"
    else:
        date_out = dt.strftime("%Y-%m-%d %H:%M:%S")
        date_status = "recent" if is_recent(dt) else "old_date"

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


def fetch_html(url):
    try:
        r = requests.get(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
                ),
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=25,
        )
        if r.status_code >= 400:
            return None
        r.encoding = r.apparent_encoding or r.encoding
        return r
    except Exception:
        return None


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


def crawl_wto(source_url, agency, site_type):
    if "rss" in source_url.lower() or source_url.lower().endswith(".xml"):
        return crawl_rss(source_url, agency, site_type)

    count = crawl_table(source_url, agency, site_type)
    if count == 0:
        return crawl_fallback_links(
            source_url,
            agency,
            site_type,
            allow_keywords=["news", "news_e", "trade", "wto"],
        )
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


def crawl_usitc(source_url, agency, site_type):
    count = crawl_card(source_url, agency, site_type)
    if count == 0:
        return crawl_fallback_links(
            source_url,
            agency,
            site_type,
            allow_keywords=["news", "release", "commission", "notice", "usitc", "investigation"],
        )
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
        return "FAIL"
    if count <= 2:
        return "CHECK"
    return "OK"



def normalize_title_for_compare(value: str) -> str:
    title = clean_text(value).lower()
    title = re.sub(r"\[[^\]]{0,80}\]", " ", title)
    title = re.sub(r"\([^)]{0,80}\)", " ", title)
    title = re.sub(r"[^0-9a-z가-힣一-龥]+", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title[:160]


def load_keyword_terms():
    """Load GTI keywords from keyword.xlsx / KEYWORD.xlsx / custom_queries.xlsx.

    Supported columns:
    - keyword, Keyword, query, search_query, custom_query
    - active/Y/N optional
    - importance/Priority optional, but not required for Step1 filtering
    """
    terms = []
    source_file = ""

    for path in KEYWORD_FILE_CANDIDATES:
        if path.exists():
            source_file = str(path)
            try:
                df = pd.read_excel(path)
            except Exception as e:
                print(f"⚠ keyword file read failed: {path} / {e}")
                continue

            df = df.loc[:, ~df.columns.astype(str).str.contains("^Unnamed")]
            key_col = None
            for col in df.columns:
                c = str(col).strip().lower()
                if c in ["keyword", "keywords", "query", "search_query", "custom_query", "제시어", "검색어"]:
                    key_col = col
                    break

            if key_col is None:
                continue

            active_col = None
            for col in df.columns:
                if str(col).strip().lower() in ["active", "use", "사용", "사용여부"]:
                    active_col = col
                    break

            for _, r in df.iterrows():
                if active_col is not None and not is_active_value(r.get(active_col, "Y")):
                    continue
                kw = clean_text(r.get(key_col, ""))
                if not kw or kw.lower() == "nan":
                    continue
                if len(kw) < 2:
                    continue
                terms.append(kw)
            break

    # Always include core terms so Step1 remains operational even if keyword.xlsx is missing.
    terms.extend(POLICY_CORE_KEYWORDS)

    # Deduplicate case-insensitively while preserving order.
    seen = set()
    out = []
    for kw in terms:
        key = kw.lower().strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(kw)

    print(f"🔎 keyword terms loaded: {len(out)}" + (f" / {source_file}" if source_file else " / default only"))
    return out


def text_has_any(text, terms):
    low = clean_text(text).lower()
    return any(str(t).lower() in low for t in terms if str(t).strip())


def calc_policy_score(row, keyword_terms=None):
    """Rough Step1 relevance score. This is not Top30 ranking.

    Purpose:
    - Keep broad collection for downstream AI analysis.
    - Provide a useful signal for STEP3/STEP4 selection.
    """
    keyword_terms = keyword_terms or []
    title = clean_text(row.get("title", ""))
    hay = " ".join([
        title,
        str(row.get("url", "")),
        str(row.get("source", "")),
        str(row.get("agency", "")),
    ]).lower()

    score = 0
    matched = []

    for term in HIGH_VALUE_POLICY_TERMS:
        if str(term).lower() in hay:
            score += 3
            matched.append(term)
    for term in MEDIUM_VALUE_POLICY_TERMS:
        if str(term).lower() in hay:
            score += 1
            matched.append(term)

    # keyword.xlsx terms are used as an additional signal, not as a hard gate.
    for term in keyword_terms:
        t = str(term).lower().strip()
        if t and len(t) >= 2 and t in hay:
            score += 1
            matched.append(term)
            if score >= 10:
                break

    if normalize_site_type(row.get("site_type", "")) == "regulation":
        score += 2

    return score, ", ".join(list(dict.fromkeys(map(str, matched)))[:12])


def is_obvious_noise_news(row, keyword_terms=None):
    """Remove only obvious agency/crime/travel/agriculture noise in STEP1."""
    joined = " ".join([
        str(row.get("title", "")),
        str(row.get("url", "")),
        str(row.get("source", "")),
        str(row.get("agency", "")),
    ])
    if not text_has_any(joined, NOISE_NEWS_KEYWORDS):
        return False
    score, _ = calc_policy_score(row, keyword_terms or [])
    return score < MIN_POLICY_SCORE_STRICT_AGENCY


def is_agency_news_strict(row):
    hay = " ".join([
        str(row.get("url", "")),
        str(row.get("source", "")),
        str(row.get("agency", "")),
    ]).lower()
    return any(h in hay for h in STRICT_AGENCY_HOST_HINTS)


def is_policy_relevant_row(row, keyword_terms):
    title = clean_text(row.get("title", ""))
    hay = " ".join([
        title,
        str(row.get("url", "")),
        str(row.get("source", "")),
        str(row.get("agency", "")),
    ])
    hay_low = hay.lower()

    # Regulations are allowed with legal/regulatory words, but menu/list rows are still blocked elsewhere.
    if normalize_site_type(row.get("site_type", "")) == "regulation":
        regulation_terms = [
            "법률", "시행령", "시행규칙", "고시", "공고", "훈령", "예규", "행정규칙", "입법예고", "행정예고",
            "regulation", "notice", "directive", "federal register", "ruling", "law", "rulemaking",
            "customs", "tariff", "trade", "import", "export", "origin", "fta", "hs",
        ]
        return text_has_any(hay, keyword_terms) or text_has_any(hay, regulation_terms)

    # CBP/newsroom and similar agency news: reject crime/travel/agriculture noise unless a policy keyword is explicit.
    if is_agency_news_strict(row):
        if text_has_any(hay, NOISE_NEWS_KEYWORDS) and not text_has_any(hay, POLICY_CORE_KEYWORDS):
            return False
        strict_terms = [
            "customs", "tariff", "trade", "import", "export", "origin", "fta", "hs", "classification",
            "ruling", "federal register", "notice", "regulation", "duty", "duties", "section 301", "uflpa",
        ]
        return text_has_any(hay, keyword_terms) or text_has_any(hay, strict_terms)

    return text_has_any(hay, keyword_terms)


def load_cumulative_keys():
    """Load cumulative keys using URL only.

    GTI STEP1 cumulative comparison must be URL-based only.
    Title/normalized-title similarity belongs to STEP3 clustering, not STEP1.
    """
    keys = set()
    if not CUMULATIVE_FILE.exists():
        return keys
    try:
        old = pd.read_excel(CUMULATIVE_FILE)
    except Exception as e:
        print(f"⚠ cumulative read failed: {CUMULATIVE_FILE} / {e}")
        return keys

    if "url" not in old.columns:
        print(f"⚠ cumulative file has no url column: {CUMULATIVE_FILE}")
        return keys

    for _, row in old.iterrows():
        nurl = normalize_url_for_compare(row.get("url", ""))
        if nurl:
            keys.add(nurl)

    print(f"📚 cumulative existing URL keys loaded: {len(keys)}")
    return keys


def split_cumulative_new_rows(df, cumulative_keys):
    """Split rows by cumulative URL duplication only.

    Do NOT compare title or normalized_title here.
    Similar-title and same-topic merge is handled later in STEP3/STEP4.
    """
    if df.empty or not cumulative_keys:
        return df.copy(), df.iloc[0:0].copy()

    work = df.copy()
    reasons = []
    for _, row in work.iterrows():
        row_reasons = []
        nurl = normalize_url_for_compare(row.get("url", ""))
        if nurl and nurl in cumulative_keys:
            row_reasons.append("already_in_cumulative_url")
        reasons.append("; ".join(row_reasons))

    work["_final_exclude_reason"] = reasons
    excluded = work[work["_final_exclude_reason"] != ""].copy()
    final = work[work["_final_exclude_reason"] == ""].copy()
    final.drop(columns=["_final_exclude_reason"], inplace=True, errors="ignore")
    return final, excluded


def update_cumulative_file(final_df):
    if final_df.empty:
        return 0

    new_df = _law1_require_headline_url(final_df, "cumulative_update")
    if new_df.empty:
        print("[LAW1 QUALITY] cumulative_update skipped: no rows with mandatory title/url")
        return 0
    new_df["cumulative_updated_at"] = now_str()

    if CUMULATIVE_FILE.exists():
        try:
            old = pd.read_excel(CUMULATIVE_FILE)
            combined = pd.concat([old, new_df], ignore_index=True)
        except Exception:
            combined = new_df
    else:
        combined = new_df

    combined = _law1_require_headline_url(combined, "cumulative_file")
    combined["_norm_url"] = combined["url"].map(normalize_url_for_compare)
    before = len(combined)
    combined = combined.drop_duplicates(subset=["_norm_url"], keep="first")
    combined.drop(columns=["_norm_url"], inplace=True, errors="ignore")

    try:
        combined.to_excel(CUMULATIVE_FILE, index=False)
    except PermissionError:
        alt = BASE_DIR / "1.site_news_cumulative_new.xlsx"
        combined.to_excel(alt, index=False)
        print(f"⚠ cumulative 파일 열림 → 대체 저장: {alt}")
    return before - len(combined)


def write_reject_debug(excluded_df=None):
    frames = []
    if rejects:
        frames.append(pd.DataFrame(rejects))
    if excluded_df is not None and not excluded_df.empty:
        ex = excluded_df.copy()
        ex["reason"] = ex.get("_final_exclude_reason", "final_excluded")
        ex["checked_at"] = now_str()
        frames.append(ex)
    if not frames:
        return
    debug = pd.concat(frames, ignore_index=True, sort=False)
    try:
        debug.to_excel(REJECT_FILE, index=False)
    except PermissionError:
        pass


def save_split_files(df):
    df.to_excel(OUT_ALL_FILE, index=False)

    df[df["site_type"] == "regulation"].copy().to_excel(OUT_REG_FILE, index=False)


def get_recent_post_col(df):
    for col in df.columns:
        name = str(col)
        if "최근게시일" in name or ("recent" in name.lower() and "post" in name.lower()):
            return col
    return None


def prepare_sites_dataframe(sites):
    """Normalize sites.xlsx after manual edits.

    The workbook is managed as one sheet. When helper columns such as
    policy_type.1/site_type.1 exist, fill blanks in the primary columns first
    and then remove helper columns. track is the routing source of truth.
    """
    sites = sites.copy()

    for base_col in ["policy_type", "site_type"]:
        helper_col = f"{base_col}.1"
        if helper_col in sites.columns:
            if base_col not in sites.columns:
                sites[base_col] = sites[helper_col]
            else:
                base_blank = sites[base_col].isna() | sites[base_col].astype(str).str.strip().eq("")
                sites.loc[base_blank, base_col] = sites.loc[base_blank, helper_col]

    drop_cols = [
        c for c in sites.columns
        if str(c).startswith("Unnamed") or str(c).endswith(".1")
    ]
    if drop_cols:
        sites = sites.drop(columns=drop_cols, errors="ignore")

    for col in ["track", "official_flag", "priority_group", "must_keep"]:
        if col not in sites.columns:
            sites[col] = ""

    if "site_type" not in sites.columns:
        sites["site_type"] = ""

    track = sites["track"].fillna("").astype(str).str.strip().str.lower()
    track_valid = track.isin(["regulation", "news"])
    sites.loc[track_valid, "site_type"] = track[track_valid]

    return sites


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


def split_final_rows(df, keyword_terms=None):
    """STEP1 final split: broad collection + obvious noise removal.

    역할 분리 원칙:
    - STEP1: 날짜/메뉴/명백한 잡뉴스/누적 중복만 제거하고 넓게 수집
    - STEP3/STEP4: 동일·유사 뉴스 클러스터링, Top30 선정, 삼성 영향도/Action 분석

    Therefore, keyword.xlsx is used as a relevance signal(policy_score), not a hard exclusion gate.
    """
    if df.empty:
        return df.copy(), df.copy()

    keyword_terms = keyword_terms or POLICY_CORE_KEYWORDS
    work = df.copy()
    reasons = []
    policy_scores = []
    matched_terms = []
    relevance_status = []

    for _, row in work.iterrows():
        row_reasons = []
        title = clean_text(row.get("title", ""))
        url_raw = str(row.get("url", ""))
        url = normalize_url_for_compare(url_raw)
        source = normalize_url_for_compare(row.get("source", ""))
        date_status = clean_text(row.get("date_status", ""))
        joined = " ".join([title, url_raw, str(row.get("source", "")), str(row.get("agency", ""))])

        score, matched = calc_policy_score(row, keyword_terms)
        policy_scores.append(score)
        matched_terms.append(matched)

        if score >= 6:
            relevance_status.append("HIGH")
        elif score >= 3:
            relevance_status.append("MEDIUM")
        elif score >= 1:
            relevance_status.append("LOW")
        else:
            relevance_status.append("REVIEW")

        # Hard exclusions retained in STEP1
        if date_status != "recent":
            row_reasons.append(f"date_status={date_status or 'blank'}")
        if len(title) > MAX_TITLE_LEN:
            row_reasons.append("title_too_long_over_180")
        if "최신 게시물 확인 필요" in title:
            row_reasons.append("diagnostic_hint_title")
        if is_menu_or_category_link(title, row.get("url", ""), row.get("source", "")):
            row_reasons.append("menu_or_category_link")
        if text_has_any(joined, MENU_TEXT_PATTERNS):
            row_reasons.append("menu_or_agency_list_text")
        if url and source and url == source:
            row_reasons.append("url_equals_source")
        if "#" in url_raw:
            row_reasons.append("fragment_or_menu_url")

        # Only strict agency/noise removal. Do not drop every low-score article here.
        if is_agency_news_strict(row) and is_obvious_noise_news(row, keyword_terms):
            row_reasons.append("obvious_agency_noise_news")

        # Optional strict mode for emergency noise control, normally False.
        if STEP1_STRICT_POLICY_FILTER and score <= 0:
            row_reasons.append("strict_mode_no_policy_score")

        reasons.append("; ".join(row_reasons))

    work["policy_score"] = policy_scores
    work["matched_policy_terms"] = matched_terms
    work["step1_relevance_status"] = relevance_status
    work["_final_exclude_reason"] = reasons
    excluded = work[work["_final_exclude_reason"] != ""].copy()
    final = work[work["_final_exclude_reason"] == ""].copy()

    final.drop(columns=["_final_exclude_reason"], inplace=True, errors="ignore")
    return final, excluded

def main():
    print("🚀 GTI STEP1 REGULATION-ONLY MODE START")

    BASE_DIR.mkdir(parents=True, exist_ok=True)

    if not SITE_FILE.exists():
        print(f"❌ sites.xlsx 없음: {SITE_FILE}")
        return

    sites = prepare_sites_dataframe(pd.read_excel(SITE_FILE))

    for col in ["parser", "site_type", "collected_count", "total_collected", "last_checked", "status"]:
        if col not in sites.columns:
            sites[col] = ""

    recent_post_col = get_recent_post_col(sites)
    keyword_terms = load_keyword_terms()
    cumulative_keys = load_cumulative_keys()

    total_before = len(results)
    active_count = 0
    skipped_count = 0

    for idx, row in sites.iterrows():
        active = row.get("active", 1)

        if not is_active_value(active):
            skipped_count += 1
            print(f"[SKIP] inactive: {row.get('site_name', '')}")
            continue

        active_count += 1

        site_name = clean_text(row.get("site_name", ""))
        source_url = str(row.get("url", "")).strip()

        type_value = str(row.get("type", "generic")).strip().lower()
        type_value, fixed_parser, site_type = normalize_source_config(
            source_url,
            type_value,
            row.get("parser", ""),
            row.get("site_type", "news"),
        )
        parser = infer_parser(type_value, fixed_parser)
        agency = site_name

        sites.at[idx, "type"] = type_value
        sites.at[idx, "parser"] = parser
        sites.at[idx, "site_type"] = site_type

        # STEP1 is now regulation-only.
        # News/site bulletin collection has moved to STEP2-3.
        if normalize_site_type(site_type) != "regulation":
            # STEP1은 법규 전용입니다. 뉴스/보도자료 사이트는 STEP2-3에서 처리하므로
            # 콘솔 로그와 출력 파일을 만들지 않고 조용히 skip합니다.
            skipped_count += 1
            sites.at[idx, "collected_count"] = 0
            sites.at[idx, "last_checked"] = now_str()
            sites.at[idx, "status"] = "SKIP_NEWS_MOVED_TO_STEP2_3"
            continue

        if not source_url.startswith("http"):
            count = 0
            sites.at[idx, "collected_count"] = count
            sites.at[idx, "last_checked"] = now_str()
            sites.at[idx, "status"] = get_status(count)
            continue

        print(f"[CRAWL] {site_name} / type={type_value} / parser={parser} / site_type={site_type}")

        crawl_start_idx = len(results)
        site_recent_dt = parse_site_recent_date(row.get(recent_post_col, "")) if recent_post_col else None

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
                if "fta.motir.go.kr/ftamain/promo/news/trend" in source_url.lower():
                    count = crawl_fta_trend(source_url, agency, site_type)
                else:
                    count = crawl_table(source_url, agency, site_type)
            else:
                count = crawl_generic(source_url, agency, site_type)

        except Exception as e:
            print(f"   ERROR: {e}")
            count = 0

        if count == 0 and parser not in {"gwanbo_parser", "gwanbo_selenium_parser"}:
            hint_count = crawl_site_hint(row, source_url, agency, site_type)
            if hint_count > 0:
                print(f"   [HINT FALLBACK] 최근게시일 기준 {hint_count}건")
                count = hint_count

        print(f" → {count}건")

        apply_site_recent_date(crawl_start_idx, agency, site_type, site_recent_dt)

        prev_total = pd.to_numeric(sites.at[idx, "total_collected"], errors="coerce")
        if pd.isna(prev_total):
            prev_total = 0

        sites.at[idx, "collected_count"] = count
        sites.at[idx, "total_collected"] = int(prev_total) + int(count)
        sites.at[idx, "last_checked"] = now_str()
        sites.at[idx, "status"] = get_status(count)

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
        sites.to_excel(SITE_FILE, index=False)

        write_reject_debug()
        print(f"🧾 제외 로그: {REJECT_FILE}")

        return

    # STEP1 internal deduplication is URL-only.
    # Title/similar-title clustering must be handled in STEP3.
    df["_norm_url"] = df["url"].map(normalize_url_for_compare)
    df = df.drop_duplicates(subset=["_norm_url"], keep="first")
    df.drop(columns=["_norm_url"], inplace=True, errors="ignore")
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
    final_df, excluded_df = split_final_rows(df, keyword_terms)

    # GTI 운영 원칙 변경(2026-06-18):
    # 1.site 단계의 raw 파일은 "최근 72시간 후보"를 보존해야 한다.
    # cumulative 중복은 메일/누적 관리용 참고정보일 뿐, raw 저장 제외 사유가 아니다.
    # 따라서 기존 split_cumulative_new_rows()로 final_df에서 제거하지 않고
    # is_cumulative_duplicate 컬럼만 부여한다.
    if final_df.empty:
        cumulative_excluded_df = final_df.copy()
        final_df["is_cumulative_duplicate"] = ""
    else:
        final_df = final_df.copy()
        final_df["_norm_url_for_cumulative"] = final_df["url"].map(normalize_url_for_compare)
        final_df["is_cumulative_duplicate"] = final_df["_norm_url_for_cumulative"].apply(
            lambda x: "Y" if x and x in cumulative_keys else "N"
        )
        cumulative_excluded_df = final_df[final_df["is_cumulative_duplicate"] == "Y"].copy()
        final_df.drop(columns=["_norm_url_for_cumulative"], inplace=True, errors="ignore")

    final_count = len(final_df)
    final_filter_removed = len(excluded_df)
    cumulative_dedup_removed = len(cumulative_excluded_df)

    try:
        audit_df.to_excel(OUT_AUDIT_FILE, index=False)
        save_split_files(final_df)
        # cumulative 파일은 이력/중복판정용으로만 갱신한다. raw 저장 건수에는 영향 없음.
        update_cumulative_file(final_df)
        if not excluded_df.empty:
            excluded_df.to_excel(FINAL_EXCLUDED_FILE, index=False)
    except PermissionError:
        alt = BASE_DIR / "1.site_news_raw_new.xlsx"
        final_df.to_excel(alt, index=False)
        print(f"⚠ 결과 파일 열림 → 대체 저장: {alt}")

    write_reject_debug(excluded_df)

    try:
        sites.to_excel(SITE_FILE, index=False)
    except PermissionError:
        alt_sites = BASE_DIR / "sites_updated.xlsx"
        sites.to_excel(alt_sites, index=False)
        print(f"⚠ sites.xlsx 열림 → 대체 저장: {alt_sites}")

    print(f"📌 active 대상: {active_count}")
    print(f"📌 inactive skip: {skipped_count}")
    print(f"📊 RAW 수집: {raw_count}")
    print(f"🧹 중복 제거: {dup_removed}")
    print(f"🧹 최종 필터 제외(누적 제외 미포함): {final_filter_removed}")
    print(f"🧾 누적 중복 표시만(Y): {cumulative_dedup_removed}")
    print(f"✅ STEP1 법규 후보 저장(최근 {HOURS_BACK}시간 기준+보호 후보 포함): {final_count}")
    try:
        new_count = int((final_df.get("is_cumulative_duplicate", pd.Series(dtype=str)).fillna("").astype(str).str.upper().ne("Y")).sum())
    except Exception:
        new_count = final_count
    print(f"🆕 신규 법규 후보 저장: {new_count}")
    print(f"📁 법규 전체 파일: {OUT_ALL_FILE}")
    print(f"📁 감사 파일: {OUT_AUDIT_FILE}")
    print(f"📁 법규 파일: {OUT_REG_FILE}")
    print(f"📁 신규 법규 파일: {OUT_REG_NEW_FILE}")
    print(f"📁 최종 제외 파일: {FINAL_EXCLUDED_FILE}")
    print(f"🧾 제외 로그: {REJECT_FILE}")
    print(f"📊 RAW 증가: {len(results) - total_before}")


# =========================================================
# GTI Regulation-first output override
# Keep this block immediately before __main__.
# It does not change news collection rules. It only strengthens the regulation
# split and adds audit fields used by STEP3/STEP4 regulation processing.
# Korean terms are generated from Unicode escapes to avoid mojibake when copied
# to C:\Temp.
# =========================================================

def _u_gti(s: str) -> str:
    return s.encode("ascii").decode("unicode_escape")


REG_KW = {
    "law": _u_gti("\\ubc95\\ub839"),
    "decree": _u_gti("\\uc2dc\\ud589\\ub839"),
    "rule": _u_gti("\\uc2dc\\ud589\\uaddc\\uce59"),
    "notice": _u_gti("\\uace0\\uc2dc"),
    "announce": _u_gti("\\uacf5\\uace0"),
    "gazette": _u_gti("\\uad00\\ubcf4"),
    "amend": _u_gti("\\uac1c\\uc815"),
    "partial_amend": _u_gti("\\uc77c\\ubd80\\uac1c\\uc815"),
    "enact": _u_gti("\\uc81c\\uc815"),
    "promulgate": _u_gti("\\uacf5\\ud3ec"),
    "effective": _u_gti("\\uc2dc\\ud589"),
    "tariff": _u_gti("\\uad00\\uc138"),
    "tariff_rate": _u_gti("\\uad00\\uc138\\uc728"),
    "customs": _u_gti("\\ud1b5\\uad00"),
    "import": _u_gti("\\uc218\\uc785"),
    "export": _u_gti("\\uc218\\ucd9c"),
    "origin": _u_gti("\\uc6d0\\uc0b0\\uc9c0"),
    "hs": _u_gti("\\ud488\\ubaa9\\ubd84\\ub958"),
    "dumping": _u_gti("\\ubc18\\ub364\\ud551"),
    "cvd": _u_gti("\\uc0c1\\uacc4\\uad00\\uc138"),
    "safeguard": _u_gti("\\uc138\\uc774\\ud504\\uac00\\ub4dc"),
    "export_control": _u_gti("\\uc218\\ucd9c\\ud1b5\\uc81c"),
    "strategic": _u_gti("\\uc804\\ub7b5\\ubb3c\\uc790"),
}

OFFICIAL_REG_DOMAINS = [
    "law.go.kr", "gwanbo.go.kr", "customs.go.kr", "unipass.customs.go.kr",
    "motir.go.kr", "mofa.go.kr", "moef.go.kr", "korea.kr",
    "federalregister.gov", "ustr.gov", "cbp.gov", "usitc.gov",
    "eur-lex.europa.eu", "taxation-customs.ec.europa.eu", "trade.ec.europa.eu",
    "wto.org", "eping.wto.org", "gov.cn", "customs.gov.cn", "mofcom.gov.cn",
    "cbic.gov.in", "dgft.gov.in", "mof.gov.vn", "customs.gov.vn",
    "dof.gob.mx", "sat.gob.mx", "in.gov.br", "receita.economia.gov.br",
]

REG_TITLE_TERMS = [
    REG_KW["law"], REG_KW["decree"], REG_KW["rule"], REG_KW["notice"],
    REG_KW["announce"], REG_KW["gazette"], REG_KW["amend"], REG_KW["partial_amend"],
    REG_KW["enact"], REG_KW["promulgate"], REG_KW["effective"],
    "notice", "regulation", "rule", "law", "decree", "ordinance",
    "amendment", "final rule", "proposed rule", "federal register",
    "official journal", "gazette",
]

TRADE_REG_TERMS = [
    REG_KW["tariff"], REG_KW["tariff_rate"], REG_KW["customs"], REG_KW["import"],
    REG_KW["export"], REG_KW["origin"], REG_KW["hs"], REG_KW["dumping"],
    REG_KW["cvd"], REG_KW["safeguard"], REG_KW["export_control"], REG_KW["strategic"],
    "customs", "tariff", "duty", "import", "export", "rules of origin",
    "origin", "fta", "cepa", "epa", "hs code", "classification",
    "anti-dumping", "antidumping", "countervailing", "safeguard",
    "section 301", "section 232", "export control", "entity list",
    "uflpa", "forced labor", "cbam", "carbon border",
]

REG_NOISE_TERMS = [
    _u_gti("\\ucc44\\uc6a9"), _u_gti("\\uc778\\uc0ac"), _u_gti("\\uad50\\uc721"),
    _u_gti("\\ud589\\uc0ac"), _u_gti("\\ud1b5\\uacc4"), _u_gti("\\ub9c8\\uc57d"),
    "recruit", "hiring", "career", "vacancy", "statistics", "event",
    "drug", "narcotic", "smuggling",
]


def _reg_text(row) -> str:
    return " ".join([
        clean_text(row.get("title", "")),
        clean_text(row.get("url", "")),
        clean_text(row.get("source", "")),
        clean_text(row.get("agency", "")),
        clean_text(row.get("site_type", "")),
    ])


def _has_any_plain(text: str, terms: list[str]) -> bool:
    low = str(text).lower()
    return any(str(t).lower() in low for t in terms if str(t).strip())


def _official_domain_score(url: str, source: str) -> tuple[int, str]:
    joined = f"{url} {source}".lower()
    hits = [d for d in OFFICIAL_REG_DOMAINS if d in joined]
    return (30 if hits else 0), "; ".join(hits[:5])


def _reg_signal(row) -> tuple[int, str, str]:
    text = _reg_text(row)
    score = 0
    reasons = []
    domain_score, domains = _official_domain_score(row.get("url", ""), row.get("source", ""))
    if domain_score:
        score += domain_score
        reasons.append(f"official_domain:{domains}")
    if normalize_site_type(row.get("site_type", "")) == "regulation":
        score += 25
        reasons.append("site_type_regulation")
    title_hits = [t for t in REG_TITLE_TERMS if str(t).lower() in text.lower()]
    trade_hits = [t for t in TRADE_REG_TERMS if str(t).lower() in text.lower()]
    noise_hits = [t for t in REG_NOISE_TERMS if str(t).lower() in text.lower()]
    if title_hits:
        score += min(25, len(title_hits) * 5)
        reasons.append("reg_title:" + ";".join(title_hits[:5]))
    if trade_hits:
        score += min(35, len(trade_hits) * 7)
        reasons.append("trade_reg:" + ";".join(trade_hits[:5]))
    if noise_hits and not trade_hits:
        score -= 30
        reasons.append("noise:" + ";".join(noise_hits[:5]))
    if "law.go.kr" in text.lower() and ("lsinfop.do" in text.lower() or "admrinfols.do" in text.lower()):
        score += 20
        reasons.append("law_go_kr_detail")
    if "gwanbo.go.kr" in text.lower():
        score += 20
        reasons.append("official_gazette")
    kind = "official_trade_regulation" if score >= 55 and trade_hits else "official_regulation" if score >= 45 else "review"
    return max(0, min(score, 100)), kind, "; ".join(reasons)


def enhance_regulation_rows(df):
    if df is None or df.empty:
        return df
    out = df.copy()
    scores, kinds, reasons, official_flags, fallback_bodies = [], [], [], [], []
    for _, row in out.iterrows():
        score, kind, reason = _reg_signal(row)
        scores.append(score)
        kinds.append(kind)
        reasons.append(reason)
        official_flags.append("Y" if score >= 45 else "N")
        fallback_bodies.append(
            " | ".join([
                f"Title: {clean_text(row.get('title', ''))}",
                f"Agency: {clean_text(row.get('agency', ''))}",
                f"Date: {clean_text(row.get('date', ''))}",
                f"URL: {clean_text(row.get('url', ''))}",
                f"Source: {clean_text(row.get('source', ''))}",
                f"Signals: {reason}",
            ])
        )
    out["official_regulation_score"] = scores
    out["official_regulation_type"] = kinds
    out["official_regulation_flag"] = official_flags
    out["official_regulation_reason"] = reasons
    out["regulation_fallback_body"] = fallback_bodies
    # Promote official regulation-like rows from site/news split into regulation.
    promote = out["official_regulation_score"].fillna(0).astype(float).ge(45)
    out.loc[promote, "site_type"] = "regulation"
    return out


_original_save_split_files = save_split_files

def save_split_files(df):
    enhanced = enhance_regulation_rows(df)
    enhanced.to_excel(OUT_ALL_FILE, index=False)
    reg = enhanced[enhanced["site_type"] == "regulation"].copy()
    news = enhanced[enhanced["site_type"] == "news"].copy()
    # Sort regulations by official/trade signal so 1-1 is useful for audit.
    if not reg.empty and "official_regulation_score" in reg.columns:
        reg = reg.sort_values(["official_regulation_score", "date"], ascending=[False, False], kind="stable")
    reg.to_excel(OUT_REG_FILE, index=False)
    news.to_excel(OUT_NEWS_FILE, index=False)


OUT_REG_REVIEW_FILE = BASE_DIR / "1-1.regulation_review_raw.xlsx"

PROTECTED_FOREIGN_REG_DOMAINS = [
    "federalregister.gov", "ustr.gov", "cbp.gov", "usitc.gov", "commerce.gov",
    "bis.doc.gov", "trade.gov", "ecfr.gov",
    "eur-lex.europa.eu", "taxation-customs.ec.europa.eu", "trade.ec.europa.eu",
    "ec.europa.eu", "commission.europa.eu",
    "wto.org", "eping.wto.org",
    "customs.gov.cn", "gacc.gov.cn", "mofcom.gov.cn", "gov.cn",
    "dgft.gov.in", "cbic.gov.in", "commerce.gov.in",
    "customs.gov.vn", "mof.gov.vn", "moit.gov.vn",
    "customs.go.th", "mof.go.th",
    "dof.gob.mx", "sat.gob.mx", "siicex.gob.mx",
    "in.gov.br", "receita.economia.gov.br", "gov.br",
    "customs.gov.my", "miti.gov.my",
    "beacukai.go.id", "kemendag.go.id",
]

PROTECTED_FOREIGN_REG_AGENCY_HINTS = [
    "federal register", "ustr", "cbp", "usitc", "bis", "bureau of industry and security",
    "taxation and customs union", "taxud", "eur-lex", "european commission",
    "wto", "dgft", "cbic", "gacc", "china customs", "mofcom",
    "vietnam customs", "moit", "mexico", "sat", "receita federal",
]

PROTECTED_FOREIGN_REG_TERMS = [
    "customs", "tariff", "duty", "duties", "import", "export", "origin",
    "rules of origin", "fta", "cepa", "epa", "hs code", "classification",
    "ruling", "advance ruling", "valuation", "drawback", "bonded",
    "anti-dumping", "antidumping", "countervailing", "safeguard",
    "section 301", "section 232", "uflpa", "forced labor",
    "export control", "entity list", "restricted party", "sanction",
    "cbam", "carbon border", "regulation", "notice", "directive",
    "final rule", "proposed rule", "official journal", "gazette",
]

PROTECTED_FINAL_EXCLUDE_REASONS = [
    "date_status=no_date",
    "date_status=old_date",
    "title_too_long_over_180",
]

PROTECTED_BLOCK_REASONS = [
    "menu_or_category_link",
    "menu_or_agency_list_text",
    "url_equals_source",
    "fragment_or_menu_url",
    "diagnostic_hint_title",
    "invalid_title",
    "invalid_url",
]

PROTECTED_CONTENT_TERMS = [
    "tariff", "duty", "duties", "import", "export", "origin",
    "rules of origin", "fta", "cepa", "epa", "hs code", "classification",
    "ruling", "advance ruling", "valuation", "drawback", "bonded",
    "anti-dumping", "antidumping", "countervailing", "safeguard",
    "section 301", "section 232", "uflpa", "forced labor",
    "export control", "entity list", "sanction", "cbam", "carbon border",
    "executive order", "final rule", "proposed rule", "regulation", "notice",
    "directive", "announcement", "gazette",
]

PROTECTED_GENERIC_TITLES = [
    "download (type : pdf)", "download", "laws & regulations", "federal register notices",
    "help for exporters and importers", "coverage of major imports & exports",
    "business, economy, euro", "application of eu law", "value added tax",
    "national tax administrations", "organizational structure", "service navigation",
]

PROTECTED_DOCUMENT_URL_HINTS = [
    ".pdf", "/documents/20", "/document/20", "/notice/", "/notices/",
    "/announcement", "/announcements", "/press-releases/20", "/statics/",
    "public notice", "trade notice", "notification", "circular",
]

PROTECTED_NON_TRADE_NOISE = [
    "repatriation", "bureau of land management", "department of the interior",
    "museum", "cultural item", "archaeology", "career", "hiring", "vacancy",
]


def _protected_foreign_reg_text(row) -> str:
    return " ".join([
        clean_text(row.get("title", "")),
        clean_text(row.get("url", "")),
        clean_text(row.get("source", "")),
        clean_text(row.get("agency", "")),
        clean_text(row.get("matched_policy_terms", "")),
        clean_text(row.get("official_regulation_reason", "")),
    ]).lower()


def _protected_hits(text: str, terms: list[str]) -> list[str]:
    return [term for term in terms if str(term).lower() in text]


def _looks_like_actual_official_document_url(row) -> bool:
    url = clean_text(row.get("url", "")).lower()
    return any(h in url for h in PROTECTED_DOCUMENT_URL_HINTS)


def _normalized_protected_title(title: str) -> str:
    title = clean_text(title).lower()
    title = re.sub(r"^[\s\-\*\u2022·ㆍ]+", "", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


def _repair_protected_title(row):
    title = clean_text(row.get("title", ""))
    if _normalized_protected_title(title) not in PROTECTED_GENERIC_TITLES:
        return title

    url = clean_text(row.get("url", ""))
    tail = urlparse(url).path.rsplit("/", 1)[-1]
    tail = unescape(tail).replace("%20", " ").replace("_", " ").replace("-", " ")
    tail = re.sub(r"\.(pdf|html?|aspx?)$", "", tail, flags=re.I)
    tail = re.sub(r"\s+", " ", tail).strip()
    if len(tail) >= 12:
        return tail[:180]
    return title


def is_protected_foreign_regulation_candidate(row, exclude_reason: str = "") -> tuple[bool, str, int]:
    """Keep official overseas regulation candidates for review instead of dropping them.

    STEP1 often cannot parse dates from foreign official sites. For Samsung customs
    monitoring, it is safer to preserve those official candidates and let STEP3/4
    or a human review decide relevance.
    """
    text = _protected_foreign_reg_text(row)
    reason_text = str(exclude_reason or "")

    if not any(r in reason_text for r in PROTECTED_FINAL_EXCLUDE_REASONS):
        return False, "", 0
    if any(r in reason_text for r in PROTECTED_BLOCK_REASONS):
        return False, "", 0
    if any(noise in text for noise in PROTECTED_NON_TRADE_NOISE):
        return False, "", 0

    url_low = clean_text(row.get("url", "")).lower()
    title_low = _normalized_protected_title(row.get("title", ""))
    if title_low in PROTECTED_GENERIC_TITLES and not _looks_like_actual_official_document_url(row):
        return False, "", 0
    if "federalregister.gov" in url_low and "/documents/20" not in url_low:
        return False, "", 0

    domain_hits = _protected_hits(text, PROTECTED_FOREIGN_REG_DOMAINS)
    agency_hits = _protected_hits(text, PROTECTED_FOREIGN_REG_AGENCY_HINTS)
    term_hits = _protected_hits(text, PROTECTED_FOREIGN_REG_TERMS)
    content_text = " ".join([
        clean_text(row.get("title", "")),
        clean_text(row.get("url", "")),
    ]).lower()
    content_hits = _protected_hits(content_text, PROTECTED_CONTENT_TERMS)

    score = 0
    if domain_hits:
        score += 45
    if agency_hits:
        score += 20
    if term_hits:
        score += min(35, len(term_hits) * 7)
    if content_hits:
        score += min(25, len(content_hits) * 8)
    if normalize_site_type(row.get("site_type", "")) == "regulation":
        score += 15

    # Existing official regulation scoring, if available, is used as supporting evidence.
    try:
        reg_score, reg_kind, reg_reason = _reg_signal(row)
    except Exception:
        reg_score, reg_kind, reg_reason = 0, "", ""
    if reg_score >= 45:
        score += 15

    if any(noise in text for noise in ["career", "hiring", "vacancy", "press officer", "social media"]):
        score -= 25

    keep = bool((domain_hits or agency_hits) and term_hits and content_hits and score >= 65)
    reason = "; ".join([
        "protected_foreign_official_regulation",
        f"score={max(0, min(score, 100))}",
        "domains=" + ",".join(domain_hits[:5]) if domain_hits else "",
        "agencies=" + ",".join(agency_hits[:5]) if agency_hits else "",
        "terms=" + ",".join(term_hits[:8]) if term_hits else "",
        "content_terms=" + ",".join(content_hits[:8]) if content_hits else "",
        f"base_reg={reg_kind}:{reg_score}" if reg_score else "",
        f"original_exclude={reason_text}",
    ])
    reason = "; ".join([x for x in reason.split("; ") if x])
    return keep, reason, max(0, min(score, 100))


_base_split_final_rows = split_final_rows


def split_final_rows(df, keyword_terms=None):
    final, excluded = _base_split_final_rows(df, keyword_terms)
    if excluded is None or excluded.empty:
        return final, excluded

    review_rows = []
    review_indices = []
    keep_rows = []

    for idx, row in excluded.iterrows():
        reason = clean_text(row.get("_final_exclude_reason", ""))
        keep, keep_reason, keep_score = is_protected_foreign_regulation_candidate(row, reason)
        if not keep:
            continue

        recovered = row.copy()
        recovered["site_type"] = "regulation"
        recovered["title"] = _repair_protected_title(row)
        recovered["date_quality"] = clean_text(row.get("date_status", "")) or "unknown"
        recovered["protected_regulation_candidate"] = "Y"
        recovered["protected_regulation_score"] = keep_score
        recovered["protected_regulation_reason"] = keep_reason
        recovered["step1_relevance_status"] = "REVIEW_OFFICIAL_FOREIGN_REG"
        recovered["_final_exclude_reason"] = ""
        keep_rows.append(recovered)

        review_copy = row.copy()
        review_copy["title"] = _repair_protected_title(row)
        review_copy["protected_regulation_candidate"] = "Y"
        review_copy["protected_regulation_score"] = keep_score
        review_copy["protected_regulation_reason"] = keep_reason
        review_rows.append(review_copy)
        review_indices.append(idx)

    if keep_rows:
        recovered_df = pd.DataFrame(keep_rows)
        recovered_df.drop(columns=["_final_exclude_reason"], inplace=True, errors="ignore")
        final = pd.concat([final, recovered_df], ignore_index=True, sort=False)
        excluded = excluded.drop(index=review_indices, errors="ignore").copy()
        print(f"[REG REVIEW KEEP] foreign official regulation candidates preserved: {len(recovered_df)}")

    if review_rows:
        review_df = pd.DataFrame(review_rows)
        try:
            review_df.to_excel(OUT_REG_REVIEW_FILE, index=False)
            print(f"[REG REVIEW SAVE] {OUT_REG_REVIEW_FILE} rows={len(review_df)}")
        except Exception as e:
            print(f"[REG REVIEW SAVE WARN] skipped: {OUT_REG_REVIEW_FILE} / {e}")

    return final, excluded


_previous_save_split_files = save_split_files


def save_split_files(df):
    enhanced = enhance_regulation_rows(df)
    enhanced = enhanced[enhanced["site_type"] == "regulation"].copy()
    enhanced.to_excel(OUT_ALL_FILE, index=False)

    reg = enhanced[enhanced["site_type"] == "regulation"].copy()
    news = enhanced[enhanced["site_type"] == "news"].copy()

    if not reg.empty and "official_regulation_score" in reg.columns:
        sort_cols = [c for c in ["protected_regulation_candidate", "official_regulation_score", "date"] if c in reg.columns]
        if sort_cols:
            ascending = [False if c != "date" else False for c in sort_cols]
            reg = reg.sort_values(sort_cols, ascending=ascending, kind="stable")

    reg.to_excel(OUT_REG_FILE, index=False)
    # STEP1은 법규 전용입니다. 1-2.site_news_raw.xlsx는 더 이상 생성하지 않습니다.
    # 뉴스/기관소식은 STEP2-3에서 별도 수집합니다.

    if "protected_regulation_candidate" in enhanced.columns:
        review = enhanced[enhanced["protected_regulation_candidate"].fillna("").astype(str).eq("Y")].copy()
        if not review.empty:
            try:
                review.to_excel(OUT_REG_REVIEW_FILE, index=False)
            except Exception as e:
                print(f"[REG REVIEW SAVE WARN] skipped: {OUT_REG_REVIEW_FILE} / {e}")


def parse_gwanbo_text(text, source_url, agency, site_type, page_date):
    """Robust Korean gazette parser.

    The original parser can fail when Korean regex literals are damaged by
    encoding display issues. This version uses explicit Unicode ranges and
    refuses to create diagnostic/menu rows.
    """
    before = len(results)
    text = clean_text(text)
    if not text:
        return 0

    bad_terms = [
        "\ucd5c\uc2e0 \uac8c\uc2dc\ubb3c \ud655\uc778 \ud544\uc694",
        "\uad00\ubcf4\ubcf4\uae30",
        "\ubaa9\ucc28 \ub2e4\uc6b4\ub85c\ub4dc",
        "\uc804\uccb4 \ub2e4\uc6b4\ub85c\ub4dc",
        "\ub9c8\uc774\ud398\uc774\uc9c0",
    ]

    patterns = [
        r"([가-힣A-Za-z0-9ㆍ·\.\-\s]{1,80}(?:법률|대통령령|총리령|부령|고시|공고|훈령|예규)제?\s*\d{4}[-–]?\d{0,5}호\s*\([^)]{2,180}\)?)",
        r"([가-힣A-Za-z0-9ㆍ·\.\-\s]{1,80}(?:법률|대통령령|총리령|부령|고시|공고|훈령|예규)제?\s*\d{1,5}호\s*\([^)]{2,180}\)?)",
    ]

    seen = set()
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            title = clean_text(match.group(1))
            if len(title) < 10 or len(title) > MAX_TITLE_LEN:
                continue
            if any(term in title for term in bad_terms):
                continue
            if is_menu_or_category_link(title, source_url, source_url):
                continue

            title = re.sub(r"\s+", " ", title).strip()
            key = title.lower()
            if key in seen:
                continue
            seen.add(key)

            item_url = f"{source_url}?gwanboDate={page_date:%Y%m%d}&gwanboTitle={quote(title[:160])}"
            add_result(page_date, title, item_url, source_url, agency, site_type)
            if len(seen) >= MAX_GWANBO_ITEMS:
                break

    return len(results) - before


def crawl_gwanbo_requests(source_url, agency, site_type):
    before = len(results)
    res = fetch_html(source_url)
    if not res:
        return 0

    soup = BeautifulSoup(res.text, "html.parser")
    text = clean_text(soup.get_text("\n", strip=True))
    page_date = extract_date_from_text(text) or datetime.now()
    count = parse_gwanbo_text(text, source_url, agency, site_type, page_date)
    if count == 0:
        print("   [GWANBO WARN] no gazette item parsed; diagnostic fallback suppressed")
    return len(results) - before


def crawl_gwanbo(source_url, agency, site_type):
    return crawl_gwanbo_selenium(source_url, agency, site_type)


TRADE_REMEDY_REVIEW_TERMS = [
    "\ub364\ud551", "\ubc18\ub364\ud551", "\uc0c1\uacc4\uad00\uc138",
    "\ub364\ud551\ubc29\uc9c0\uad00\uc138", "\ubb34\uc5ed\uc704\uc6d0\ud68c",
    "\ubb34\uc5ed\uad6c\uc81c", "\uc0b0\uc5c5\ud53c\ud574\uc870\uc0ac",
    "anti-dumping", "antidumping", "countervailing", "trade remedy",
    "safeguard", "ad/cvd", "dumping",
]

TRADE_REMEDY_OFFICIAL_HINTS = [
    "law.go.kr", "unipass.customs.go.kr", "customs.go.kr", "gwanbo.go.kr",
    "clhs.co.kr", "krcaa.or.kr", "motie.go.kr", "mofa.go.kr",
]


def is_trade_remedy_review_candidate(row, exclude_reason=""):
    reason = str(exclude_reason or "").lower()
    if "date_status=old_date" not in reason and "date_status=no_date" not in reason:
        return False, "", 0
    if any(block in reason for block in PROTECTED_BLOCK_REASONS):
        return False, "", 0

    text = " ".join([
        clean_text(row.get("title", "")),
        clean_text(row.get("url", "")),
        clean_text(row.get("source", "")),
        clean_text(row.get("agency", "")),
        clean_text(row.get("matched_policy_terms", "")),
    ]).lower()

    term_hits = [t for t in TRADE_REMEDY_REVIEW_TERMS if t.lower() in text]
    official_hits = [h for h in TRADE_REMEDY_OFFICIAL_HINTS if h in text]
    if not term_hits:
        return False, "", 0

    score = min(100, 45 + len(term_hits) * 12 + len(official_hits) * 10)
    keep = score >= 55
    reason_text = "; ".join([
        "review_trade_remedy_old_or_no_date",
        f"score={score}",
        "terms=" + ",".join(term_hits[:8]),
        "official_hints=" + ",".join(official_hits[:5]) if official_hits else "",
        f"original_exclude={exclude_reason}",
    ])
    reason_text = "; ".join([x for x in reason_text.split("; ") if x])
    return keep, reason_text, score


_protected_split_final_rows = split_final_rows


def split_final_rows(df, keyword_terms=None):
    final, excluded = _protected_split_final_rows(df, keyword_terms)
    if excluded is None or excluded.empty:
        return final, excluded

    review_rows = []
    for _, row in excluded.iterrows():
        reason = clean_text(row.get("_final_exclude_reason", ""))
        keep, keep_reason, keep_score = is_trade_remedy_review_candidate(row, reason)
        if not keep:
            continue
        review_copy = row.copy()
        review_copy["site_type"] = "regulation"
        review_copy["date_quality"] = clean_text(row.get("date_status", "")) or "unknown"
        review_copy["protected_regulation_candidate"] = "REVIEW_AD_CVD_OLD_DATE"
        review_copy["protected_regulation_score"] = keep_score
        review_copy["protected_regulation_reason"] = keep_reason
        review_copy["step1_relevance_status"] = "REVIEW_TRADE_REMEDY_OLD_DATE"
        review_rows.append(review_copy)

    if review_rows:
        review_df = pd.DataFrame(review_rows)
        try:
            if OUT_REG_REVIEW_FILE.exists():
                old_review = pd.read_excel(OUT_REG_REVIEW_FILE)
                review_df = pd.concat([old_review, review_df], ignore_index=True, sort=False)
                if "url" in review_df.columns:
                    review_df["_norm_url"] = review_df["url"].map(normalize_url_for_compare)
                    review_df = review_df.drop_duplicates(subset=["_norm_url"], keep="first")
                    review_df = review_df.drop(columns=["_norm_url"], errors="ignore")
            review_df.to_excel(OUT_REG_REVIEW_FILE, index=False)
            print(f"[REG REVIEW SAVE] trade remedy old/no-date candidates appended: {len(review_rows)}")
        except Exception as e:
            print(f"[REG REVIEW SAVE WARN] trade remedy review skipped: {OUT_REG_REVIEW_FILE} / {e}")

    return final, excluded



# =========================================================
# GTI v3.4 operational override
# - 72h collection controlled by GTI_STEP1_HOURS_BACK
# - Split cumulative display/raw and new-only file
# - Harden Gwanbo parser: click "전체보기" and preserve warning row if parsing fails
# =========================================================

def _gwanbo_daily_url(base_url=None):
    today = datetime.now().strftime("%Y%m%d")
    base = str(base_url or "https://www.gwanbo.go.kr/user/search/searchDaily.do")
    return base

def _gwanbo_add_warning_row(source_url, agency, site_type, page_date, detail=""):
    """When gwanbo parser returns 0 despite the daily page being reachable, keep a visible review row."""
    title = f"관보 게재현황 확인 필요 - 자동 목차 파싱 실패 ({page_date:%Y-%m-%d})"
    if detail:
        title = f"{title} / {clean_text(detail, 80)}"
    item_url = f"{source_url}?gwanboDate={page_date:%Y%m%d}&parserStatus=NEED_REVIEW"
    add_result(page_date, title, item_url, source_url, agency, site_type)
    if results:
        results[-1]["gwanbo_parser_status"] = "NEED_REVIEW_ZERO_PARSED"
    return 1

def _parse_gwanbo_visible_text(text, source_url, agency, site_type, page_date):
    """Parse visible gazette daily text, including compact rows like 고용노동부고시제2026-44호(...)."""
    before = len(results)
    text = clean_text(text)
    if not text:
        return 0

    # Remove common UI/menu blocks but keep official item lines.
    lines = [clean_text(x) for x in re.split(r"[\n\r]+|(?=ㆍ)|(?=·)|(?=국가)|(?=[가-힣A-Za-z]+(?:부|청|처|위원회|부처|총리령|대통령령|고시|공고))", text) if clean_text(x)]
    expanded = "\n".join(lines) if lines else text

    patterns = [
        # 부처명고시제2026-44호(제목)
        r"([가-힣A-Za-z0-9ㆍ·\.\-\s]{0,90}(?:법률|대통령령|총리령|부령|고시|공고|훈령|예규)\s*제?\s*\d{1,4}[-–]?\d{0,6}\s*호\s*\([^)]{2,220}\))",
        # 국가보훈부령제68호(제목)
        r"([가-힣A-Za-z0-9ㆍ·\.\-\s]{0,90}(?:법률|대통령령|총리령|부령|고시|공고|훈령|예규)\s*제?\s*\d{1,6}\s*호\s*\([^)]{2,220}\))",
        # If parentheses are missing, keep up to 160 chars.
        r"([가-힣A-Za-z0-9ㆍ·\.\-\s]{0,90}(?:법률|대통령령|총리령|부령|고시|공고|훈령|예규)\s*제?\s*\d{1,4}[-–]?\d{0,6}\s*호\s*[가-힣A-Za-z0-9ㆍ·\.\-\s]{5,160})",
    ]

    bad_terms = [
        "최초 사용자", "관보보기", "목차 다운로드", "전체 다운로드", "마이페이지",
        "관심 관보", "타임스탬프", "전체보기", "검색", "누리집",
    ]
    seen = set()
    for pattern in patterns:
        for match in re.finditer(pattern, expanded):
            title = clean_text(match.group(1))
            title = re.sub(r"\s+", " ", title).strip(" -ㆍ·")
            if len(title) < 10 or len(title) > 220:
                continue
            if any(b in title for b in bad_terms):
                continue
            # Cut off trailing UI text if it leaks into the match.
            title = re.split(r"(?:전체보기|목차다운로드|처음|이전|다음|마지막)", title)[0].strip()
            if len(title) < 10:
                continue
            key = title.lower()
            if key in seen:
                continue
            seen.add(key)
            item_url = f"{source_url}?gwanboDate={page_date:%Y%m%d}&gwanboTitle={quote(title[:160])}"
            add_result(page_date, title, item_url, source_url, agency, site_type)
            if len(seen) >= MAX_GWANBO_ITEMS:
                break
    return len(results) - before

def crawl_gwanbo_requests(source_url, agency, site_type):
    before = len(results)
    res = fetch_html(source_url)
    if not res:
        print("   [GWANBO WARN] request failed")
        return _gwanbo_add_warning_row(source_url, agency, site_type, datetime.now(), "REQUEST_FAILED")

    soup = BeautifulSoup(res.text, "html.parser")
    text = soup.get_text("\n", strip=True)
    page_date = extract_date_from_text(text) or datetime.now()
    count = _parse_gwanbo_visible_text(text, source_url, agency, site_type, page_date)
    if count == 0:
        print("   [GWANBO WARN] request parsed 0; review row added")
        return _gwanbo_add_warning_row(source_url, agency, site_type, page_date, "REQUEST_ZERO")
    return len(results) - before

def crawl_gwanbo_selenium(source_url, agency, site_type):
    before = len(results)
    page_date = datetime.now()

    if not SELENIUM_AVAILABLE:
        print("   [GWANBO WARN] selenium unavailable; using requests fallback")
        return crawl_gwanbo_requests(source_url, agency, site_type)

    driver = None
    try:
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1800,1600")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(options=options)
        driver.get(source_url)
        time.sleep(4)

        # Try clicking "전체보기" because daily gazette table is often lazy-rendered.
        for xpath in [
            "//*[contains(text(),'전체보기')]",
            "//button[contains(.,'전체보기')]",
            "//a[contains(.,'전체보기')]",
        ]:
            try:
                elems = driver.find_elements("xpath", xpath)
                for elem in elems[:3]:
                    if elem.is_displayed():
                        driver.execute_script("arguments[0].click();", elem)
                        time.sleep(2)
                        break
            except Exception:
                pass

        html = driver.page_source or ""
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text("\n", strip=True)
        page_date = extract_date_from_text(text) or datetime.now()
        count = _parse_gwanbo_visible_text(text, source_url, agency, site_type, page_date)
        if count > 0:
            return len(results) - before

        print("   [GWANBO WARN] selenium parsed 0; trying requests fallback")
        req_count = crawl_gwanbo_requests(source_url, agency, site_type)
        if req_count > 0:
            return len(results) - before

        return _gwanbo_add_warning_row(source_url, agency, site_type, page_date, "SELENIUM_ZERO")

    except Exception as e:
        print(f"   [GWANBO SELENIUM ERROR] {e}")
        return crawl_gwanbo_requests(source_url, agency, site_type)

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

def crawl_gwanbo(source_url, agency, site_type):
    return crawl_gwanbo_selenium(source_url, agency, site_type)

# LAW1/NEWSREST final split rule (v7):
# - 1-1.regulation_raw.xlsx must contain ONLY real official regulation / legal notice rows.
# - Everything else from STEP1 is written to 1-2.site_news_raw.xlsx as news/reference candidate.
# - STEP2-3 also collects official agency news, so 1-2 is a compatibility + fallback news file.
LAW1_NEWS_OR_PRESS_HINTS = [
    "korea.kr", "policy briefing", "pressreleaseview", "briefingroom",
    "press release", "press-releases", "newsroom", "/news/", "latest-news",
    "\uc815\ucc45\ube0c\ub9ac\ud551", "\ubcf4\ub3c4\uc790\ub8cc", "\ube0c\ub9ac\ud551",
    "\ub274\uc2a4", "\uae30\uc0ac", "\ub3d9\ud5a5", "\uc124\uba85\ud68c", "\uc138\ubbf8\ub098",
]

LAW1_INTERNAL_OR_LOW_ACTION_HINTS = [
    "\uc0c1 \uc6b4\uc601", "\uc0c1\uc6b4\uc601", "\ud575\uc2ec\uac00\uce58\uc0c1", "\ud45c\ucc3d",
    "\uc778\uc0ac", "\uc870\uc9c1", "\ud589\uc0ac", "\ucc44\uc6a9",
]

LAW1_GWANBO_FALSE_TRADE_HINTS = [
    "출입국", "수출입은행", "국적상실", "국적이탈", "외국인사무소", "비자", "체류",
]

LAW1_CUSTOMS_TRADE_STRONG_TERMS = [
    "관세", "통관", "세관", "세관장확인", "통합공고", "수출입물품", "수출입물품등",
    "품목분류", "hs", "hs코드", "관세율", "관세환급", "환급사무", "보세", "보세공장",
    "보세운송", "자유무역", "fta", "원산지", "덤핑방지관세", "반덤핑", "상계관세",
    "무역위원회", "대외무역", "전략물자", "수출통제", "수입규제", "할당관세",
    "customs", "tariff", "duty", "duties", "classification", "rules of origin",
    "anti-dumping", "antidumping", "countervailing", "safeguard", "export control",
]

LAW1_CUSTOMS_TRADE_CONTEXT_TERMS = [
    "수입", "수출", "무역", "교역", "수출입",
]


def _law1_has_required_headline_url(row) -> bool:
    title = clean_text(row.get("title", ""))
    url = clean_text(row.get("url", ""))
    return bool(title and url.startswith("http") and normalize_url_for_compare(url))


def _law1_require_headline_url(df, context: str):
    if df is None or df.empty:
        return df.copy() if df is not None else pd.DataFrame()
    work = df.copy()
    if "title" in work.columns:
        if "headline" not in work.columns:
            work["headline"] = work["title"]
        else:
            blank_headline = work["headline"].isna() | work["headline"].astype(str).str.strip().eq("")
            work.loc[blank_headline, "headline"] = work.loc[blank_headline, "title"]
    mask = work.apply(_law1_has_required_headline_url, axis=1)
    removed = int((~mask).sum())
    if removed:
        print(f"[LAW1 QUALITY] {context}: removed {removed} rows without mandatory title/url")
    return work[mask].copy()


def _law1_text(row, cols=None) -> str:
    cols = cols or [
        "title", "url", "source", "agency", "site_type", "official_regulation_reason",
        "matched_policy_terms", "regulation_fallback_body",
    ]
    return " ".join(clean_text(row.get(c, "")) for c in cols).lower()


def _law1_is_news_or_press_material(row) -> bool:
    text = _law1_text(row)
    if any(h.lower() in text for h in LAW1_NEWS_OR_PRESS_HINTS):
        return True
    # Policy briefing and general official press pages are not law sources even when
    # their site row is accidentally marked as regulation in sites.xlsx.
    agency = clean_text(row.get("agency", "")).lower()
    source = clean_text(row.get("source", "")).lower()
    return ("policy" in agency and "brief" in agency) or ("korea.kr" in source)


def _law1_is_cumulative_duplicate(row) -> bool:
    return clean_text(row.get("is_cumulative_duplicate", "")).upper() == "Y"


def _law1_is_review_only_candidate(row) -> bool:
    protected = clean_text(row.get("protected_regulation_candidate", "")).upper() == "Y"
    no_date = clean_text(row.get("date_status", "")).lower() == "no_date"
    return protected or no_date


def _law1_is_low_action_internal_notice(row) -> bool:
    text = _law1_text(row)
    return any(h.lower() in text for h in LAW1_INTERNAL_OR_LOW_ACTION_HINTS)


def _law1_compact_text(row) -> str:
    text = " ".join([
        clean_text(row.get("title", "")),
        clean_text(row.get("headline", "")),
        clean_text(row.get("matched_policy_terms", "")),
        clean_text(row.get("official_regulation_reason", "")),
        clean_text(row.get("regulation_fallback_body", "")),
    ]).lower()
    return re.sub(r"\s+", "", text)


def _law1_is_gwanbo_row(row) -> bool:
    text = _law1_text(row)
    agency = clean_text(row.get("agency", ""))
    return "gwanbo.go.kr" in text or "관보" in text or "관보" in agency


def _law1_has_false_trade_signal(row) -> bool:
    compact = _law1_compact_text(row)
    return any(term.lower().replace(" ", "") in compact for term in LAW1_GWANBO_FALSE_TRADE_HINTS)


def _law1_has_customs_trade_signal(row) -> bool:
    text = _law1_text(row)
    compact = _law1_compact_text(row)
    if _law1_has_false_trade_signal(row):
        return False
    for term in LAW1_CUSTOMS_TRADE_STRONG_TERMS:
        needle = term.lower().replace(" ", "")
        if needle and (needle in compact or term.lower() in text):
            return True
    legal_form = any(term in compact for term in ["고시", "공고", "법률", "시행령", "시행규칙", "입법예고", "행정예고", "규칙"])
    return bool(legal_form and any(term in compact for term in LAW1_CUSTOMS_TRADE_CONTEXT_TERMS))


def _law1_is_real_official_regulation(row) -> bool:
    try:
        score = float(row.get("official_regulation_score", 0) or 0)
    except Exception:
        score = 0.0
    text = _reg_text(row).lower() if "_reg_text" in globals() else " ".join([clean_text(row.get(c, "")) for c in ["title", "url", "source", "agency", "site_type"]]).lower()
    if _law1_is_news_or_press_material(row):
        return False
    if _law1_is_low_action_internal_notice(row):
        return False
    if normalize_site_type(row.get("site_type", "")) != "regulation":
        return False
    if score < 45:
        return False
    # Real regulation/legal publication form. This prevents general official news/press pages
    # from being treated as Regulation just because they are from an official agency.
    legal_form_terms = [
        "law.go.kr", "gwanbo", "federalregister.gov", "eur-lex", "official journal",
        "notice", "regulation", "rule", "final rule", "proposed rule", "public notice",
        "고시", "공고", "훈령", "예규", "법", "법률", "시행령", "시행규칙", "입법예고", "행정예고", "관보",
    ]
    trade_terms = TRADE_REG_TERMS if "TRADE_REG_TERMS" in globals() else ["customs", "tariff", "import", "export", "관세", "통관", "수입", "수출", "원산지", "fta"]
    has_legal_form = any(t.lower() in text for t in legal_form_terms)
    has_trade_signal = any(t.lower() in text for t in trade_terms) or _law1_has_customs_trade_signal(row)
    if _law1_is_gwanbo_row(row):
        return bool(has_legal_form and _law1_has_customs_trade_signal(row))
    return bool(has_legal_form and has_trade_signal)

def save_split_files(df):
    enhanced = enhance_regulation_rows(df).copy()
    enhanced = _law1_require_headline_url(enhanced, "save_split_files")
    if enhanced.empty:
        enhanced.to_excel(OUT_ALL_FILE, index=False)
        enhanced.to_excel(OUT_REG_FILE, index=False)
        enhanced.to_excel(OUT_REG_NEW_FILE, index=False)
        enhanced.to_excel(OUT_NEWS_FILE, index=False)
        return

    law1_mask = enhanced.apply(_law1_is_real_official_regulation, axis=1)
    enhanced["law1_regulation_flag"] = law1_mask.map(lambda x: "Y" if x else "N")

    # Keep an all-candidate audit file, but split official-law-only vs news/reference candidates.
    enhanced.to_excel(OUT_ALL_FILE, index=False)

    review_mask = enhanced.apply(_law1_is_review_only_candidate, axis=1)
    duplicate_mask = enhanced.apply(_law1_is_cumulative_duplicate, axis=1)
    reg_final_mask = law1_mask & ~review_mask & ~duplicate_mask

    reg = enhanced[reg_final_mask].copy()
    news = enhanced[~reg_final_mask].copy()
    if not news.empty:
        news["site_type"] = "news"
        news["law1_news_reason"] = news.apply(
            lambda r: (
                "official_news_or_press_material" if _law1_is_news_or_press_material(r)
                else "review_only_no_date_or_protected_candidate" if _law1_is_review_only_candidate(r)
                else "cumulative_duplicate_excluded_from_regulation" if _law1_is_cumulative_duplicate(r)
                else "not_real_official_regulation_or_low_trade_signal"
            ),
            axis=1,
        )

    if not reg.empty and "official_regulation_score" in reg.columns:
        sort_cols = [c for c in ["protected_regulation_candidate", "official_regulation_score", "date"] if c in reg.columns]
        if sort_cols:
            reg = reg.sort_values(sort_cols, ascending=[False] * len(sort_cols), kind="stable")

    reg.to_excel(OUT_REG_FILE, index=False)
    news.to_excel(OUT_NEWS_FILE, index=False)

    reg_new = reg.copy()
    try:
        reg_new.to_excel(OUT_REG_NEW_FILE, index=False)
    except Exception as e:
        print(f"⚠ 신규 법규 파일 저장 실패: {OUT_REG_NEW_FILE} / {e}")

    if "protected_regulation_candidate" in enhanced.columns:
        review = enhanced[review_mask & law1_mask].copy()
        if not review.empty:
            try:
                review.to_excel(OUT_REG_REVIEW_FILE, index=False)
            except Exception as e:
                print(f"[REG REVIEW SAVE WARN] skipped: {OUT_REG_REVIEW_FILE} / {e}")

    print(
        f"[LAW1 FINAL] regulation={len(reg)}, news_or_reference={len(news)}, "
        f"review_only={int((review_mask & law1_mask).sum())}, "
        f"cumulative_duplicate_removed={int((duplicate_mask & law1_mask).sum())}"
    )


# =========================================================
# GTI Domestic Official Law Watch Patch v29 - 2026-07-03
# Purpose:
# - 관보(gwanbo.go.kr) / 국가법령정보센터(law.go.kr) 법규는 일반 뉴스 후보와 별도로 보존한다.
# - 할당관세, 관세법 제71조, FTA/원산지, 협정세율, HS/품목분류, 통관/보세, 수출통제 등
#   삼성전자 관세업무 핵심 법규는 제목/기관명의 한글 띄어쓰기 깨짐에도 감지한다.
# - 1-1.regulation_raw.xlsx는 최근 법규 후보 전체를 유지하고,
#   1-1.regulation_new_raw.xlsx에서만 누적 중복을 제외한다.
# Korean literals are generated from unicode escapes to survive CP949/UTF-8 copy paths.
# =========================================================

_GTI_PRE_V29_ENHANCE_REGULATION_ROWS = enhance_regulation_rows
_GTI_PRE_V29_LAW1_IS_REAL_OFFICIAL_REGULATION = _law1_is_real_official_regulation


def _v29_u(s: str) -> str:
    return s.encode("ascii").decode("unicode_escape")


def _v29_clean_compact(value: str) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"\s+", "", text)
    return text


def _v29_row_text(row) -> str:
    cols = [
        "title", "headline", "url", "source", "agency", "site_type",
        "matched_policy_terms", "official_regulation_reason", "regulation_fallback_body",
        "summary", "Summary", "description", "Description",
    ]
    return " ".join(clean_text(row.get(c, "")) for c in cols)


V29_DOMESTIC_OFFICIAL_DOMAINS = [
    "law.go.kr", "gwanbo.go.kr", "customs.go.kr", "unipass.customs.go.kr",
    "moleg.go.kr", "moef.go.kr", "motie.go.kr", "korea.kr",
]

V29_DOMESTIC_LEGAL_FORMS = [
    _v29_u("\\uace0\\uc2dc"),              # 고시
    _v29_u("\\uacf5\\uace0"),              # 공고
    _v29_u("\\ubc95\\ub960"),              # 법률
    _v29_u("\\ubc95\\ub839"),              # 법령
    _v29_u("\\uc2dc\\ud589\\ub839"),       # 시행령
    _v29_u("\\uc2dc\\ud589\\uaddc\\uce59"), # 시행규칙
    _v29_u("\\ub300\\ud1b5\\ub839\\ub839"), # 대통령령
    _v29_u("\\uae30\\ud68d\\uc7ac\\uc815\\ubd80\\ub839"), # 기획재정부령
    _v29_u("\\uad00\\uc138\\uccad\\uace0\\uc2dc"), # 관세청고시
    _v29_u("\\uc785\\ubc95\\uc608\\uace0"), # 입법예고
    _v29_u("\\ud589\\uc815\\uc608\\uace0"), # 행정예고
    _v29_u("\\ud6c8\\ub839"),              # 훈령
    _v29_u("\\uc608\\uaddc"),              # 예규
]

V29_SAMSUNG_CUSTOMS_CRITICAL_TERMS = [
    _v29_u("\\ud560\\ub2f9\\uad00\\uc138"), # 할당관세
    _v29_u("\\uad00\\uc138\\ubc95\\uc81c71\\uc870"), # 관세법제71조
    _v29_u("\\uad00\\uc138\\ubc9571\\uc870"), # 관세법71조
    _v29_u("\\uc790\\uc720\\ubb34\\uc5ed\\ud611\\uc815"), # 자유무역협정
    _v29_u("\\ud611\\uc815\\uc138\\uc728"), # 협정세율
    _v29_u("\\uc6d0\\uc0b0\\uc9c0"),       # 원산지
    _v29_u("\\uc6d0\\uc0b0\\uc9c0\\uc99d\\uba85"), # 원산지증명
    _v29_u("\\ud488\\ubaa9\\ubd84\\ub958"), # 품목분류
    _v29_u("\\uc138\\ubc88"),              # 세번
    _v29_u("\\uad00\\uc138\\uc728"),       # 관세율
    _v29_u("\\uad00\\uc138"),              # 관세
    _v29_u("\\ud1b5\\uad00"),              # 통관
    _v29_u("\\uc218\\uc785\\ud1b5\\uad00"), # 수입통관
    _v29_u("\\uc218\\ucd9c\\ud1b5\\uad00"), # 수출통관
    _v29_u("\\uc218\\ucd9c\\uc785\\ud654\\ubb3c"), # 수출입화물
    _v29_u("\\uac80\\uc0ac\\ube44\\uc6a9"), # 검사비용
    _v29_u("\\ubcf4\\uc138"),              # 보세
    _v29_u("\\ubc18\\ub364\\ud551"),       # 반덤핑
    _v29_u("\\uc0c1\\uacc4\\uad00\\uc138"), # 상계관세
    _v29_u("\\uc138\\uc774\\ud504\\uac00\\ub4dc"), # 세이프가드
    _v29_u("\\uc218\\ucd9c\\ud1b5\\uc81c"), # 수출통제
    _v29_u("\\uc804\\ub7b5\\ubb3c\\uc790"), # 전략물자
    _v29_u("\\uc218\\uc785\\uaddc\\uc81c"), # 수입규제
    "fta", "hs", "hscode", "hs code", "classification", "rulesoforigin",
    "tariffquota", "tariff", "customs", "duty", "anti-dumping", "antidumping",
    "countervailing", "safeguard", "exportcontrol", "entitylist", "uflpa",
]

V29_DOMESTIC_LOW_VALUE_NOISE = [
    _v29_u("\\uc9c1\\uc81c"),              # 직제
    _v29_u("\\uc778\\uc0ac"),              # 인사
    _v29_u("\\ucc44\\uc6a9"),              # 채용
    _v29_u("\\uc0c1\\ud6c8"),              # 상훈
    _v29_u("\\uacf5\\ubaa8\\uc804"),       # 공모전
    _v29_u("\\ubd80\\uc815\\uccad\\ud0c1"), # 부정청탁
    _v29_u("\\uac1c\\uc778\\uc815\\ubcf4"), # 개인정보
    _v29_u("\\uc2b9\\uac1d\\uc608\\uc57d\\uc790\\ub8cc"), # 승객예약자료
]


def _v29_domestic_law_signal(row) -> tuple[bool, str, int]:
    raw_text = _v29_row_text(row)
    compact = _v29_clean_compact(raw_text)
    text_low = raw_text.lower()

    domain_hits = [d for d in V29_DOMESTIC_OFFICIAL_DOMAINS if d in text_low]
    legal_hits = [t for t in V29_DOMESTIC_LEGAL_FORMS if _v29_clean_compact(t) in compact]
    critical_hits = []
    for term in V29_SAMSUNG_CUSTOMS_CRITICAL_TERMS:
        needle = _v29_clean_compact(term)
        if needle and needle in compact:
            critical_hits.append(term)

    noise_hits = [t for t in V29_DOMESTIC_LOW_VALUE_NOISE if _v29_clean_compact(t) in compact]

    score = 0
    if domain_hits:
        score += 40
    if legal_hits:
        score += min(25, len(legal_hits) * 8)
    if critical_hits:
        score += min(45, len(critical_hits) * 10)
    if "gwanbo.go.kr" in text_low:
        score += 10
    if "law.go.kr" in text_low:
        score += 10
    if noise_hits and not critical_hits:
        score -= 35

    # 관보/법령정보센터는 공식 출처 + 법규 형식 + 관세 핵심어 중 하나면 보존한다.
    keep = bool(domain_hits and legal_hits and critical_hits and score >= 70)

    reason_parts = [
        "v29_domestic_official_law_watch",
        f"score={max(0, min(score, 100))}",
        "domains=" + ",".join(domain_hits[:5]) if domain_hits else "",
        "legal_forms=" + ",".join(legal_hits[:5]) if legal_hits else "",
        "critical_terms=" + ",".join(map(str, critical_hits[:8])) if critical_hits else "",
        "noise=" + ",".join(noise_hits[:5]) if noise_hits else "",
    ]
    return keep, "; ".join([p for p in reason_parts if p]), max(0, min(score, 100))


def _law1_has_customs_trade_signal(row) -> bool:
    keep, _, _ = _v29_domestic_law_signal(row)
    if keep:
        return True
    compact = _v29_clean_compact(_v29_row_text(row))
    for term in V29_SAMSUNG_CUSTOMS_CRITICAL_TERMS:
        needle = _v29_clean_compact(term)
        if needle and needle in compact:
            return True
    return False


def enhance_regulation_rows(df):
    out = _GTI_PRE_V29_ENHANCE_REGULATION_ROWS(df)
    if out is None or out.empty:
        return out
    out = out.copy()
    for idx, row in out.iterrows():
        keep, reason, score = _v29_domestic_law_signal(row)
        if not keep:
            continue
        prev_score = 0
        try:
            prev_score = int(float(row.get("official_regulation_score", 0) or 0))
        except Exception:
            prev_score = 0
        out.at[idx, "site_type"] = "regulation"
        out.at[idx, "official_regulation_flag"] = "Y"
        out.at[idx, "official_regulation_type"] = "domestic_critical_trade_regulation"
        out.at[idx, "official_regulation_score"] = max(prev_score, score)
        prev_reason = clean_text(row.get("official_regulation_reason", ""))
        out.at[idx, "official_regulation_reason"] = (prev_reason + "; " if prev_reason else "") + reason
        out.at[idx, "protected_regulation_candidate"] = "N"
        out.at[idx, "step1_relevance_status"] = "HIGH_DOMESTIC_OFFICIAL_LAW"
        fallback = clean_text(row.get("regulation_fallback_body", ""))
        if not fallback:
            out.at[idx, "regulation_fallback_body"] = " | ".join([
                f"Title: {clean_text(row.get('title', ''))}",
                f"Agency: {clean_text(row.get('agency', ''))}",
                f"Date: {clean_text(row.get('date', ''))}",
                f"URL: {clean_text(row.get('url', ''))}",
                f"Signals: {reason}",
            ])
    return out


def _law1_is_real_official_regulation(row) -> bool:
    keep, _, _ = _v29_domestic_law_signal(row)
    if keep:
        return True
    return _GTI_PRE_V29_LAW1_IS_REAL_OFFICIAL_REGULATION(row)


def save_split_files(df):
    enhanced = enhance_regulation_rows(df).copy()
    enhanced = _law1_require_headline_url(enhanced, "save_split_files_v29")
    if enhanced.empty:
        enhanced.to_excel(OUT_ALL_FILE, index=False)
        enhanced.to_excel(OUT_REG_FILE, index=False)
        enhanced.to_excel(OUT_REG_NEW_FILE, index=False)
        enhanced.to_excel(OUT_NEWS_FILE, index=False)
        return

    law1_mask = enhanced.apply(_law1_is_real_official_regulation, axis=1)
    enhanced["law1_regulation_flag"] = law1_mask.map(lambda x: "Y" if x else "N")
    enhanced.to_excel(OUT_ALL_FILE, index=False)

    review_mask = enhanced.apply(_law1_is_review_only_candidate, axis=1)
    duplicate_mask = enhanced.apply(_law1_is_cumulative_duplicate, axis=1)

    # v29: regulation_raw keeps recent official law candidates even if already in cumulative.
    # new_raw is the place where cumulative duplicates are removed.
    reg_mask = law1_mask & ~review_mask
    reg = enhanced[reg_mask].copy()
    reg_new = enhanced[reg_mask & ~duplicate_mask].copy()
    news = enhanced[~reg_mask].copy()

    if not news.empty:
        news["site_type"] = "news"
        news["law1_news_reason"] = news.apply(
            lambda r: (
                "official_news_or_press_material" if _law1_is_news_or_press_material(r)
                else "review_only_no_date_or_protected_candidate" if _law1_is_review_only_candidate(r)
                else "not_real_official_regulation_or_low_trade_signal"
            ),
            axis=1,
        )

    sort_cols = [c for c in ["official_regulation_score", "date"] if c in reg.columns]
    if sort_cols and not reg.empty:
        reg = reg.sort_values(sort_cols, ascending=[False] * len(sort_cols), kind="stable")
    sort_cols_new = [c for c in ["official_regulation_score", "date"] if c in reg_new.columns]
    if sort_cols_new and not reg_new.empty:
        reg_new = reg_new.sort_values(sort_cols_new, ascending=[False] * len(sort_cols_new), kind="stable")

    reg.to_excel(OUT_REG_FILE, index=False)
    news.to_excel(OUT_NEWS_FILE, index=False)
    reg_new.to_excel(OUT_REG_NEW_FILE, index=False)

    review = enhanced[review_mask & law1_mask].copy()
    if not review.empty:
        try:
            review.to_excel(OUT_REG_REVIEW_FILE, index=False)
        except Exception as e:
            print(f"[REG REVIEW SAVE WARN] skipped: {OUT_REG_REVIEW_FILE} / {e}")

    print(
        f"[LAW1 FINAL v29] regulation={len(reg)}, new_regulation={len(reg_new)}, "
        f"news_or_reference={len(news)}, review_only={int((review_mask & law1_mask).sum())}, "
        f"cumulative_duplicate_marked={int((duplicate_mask & law1_mask).sum())}"
    )
if __name__ == "__main__":
    main()
