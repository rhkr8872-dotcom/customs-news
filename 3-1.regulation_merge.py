# -*- coding: utf-8 -*-
# GTI FINAL CORE v6 - Article body extraction with Selenium URL recovery
"""
GTI STEP3 - LAW1 Regulation / News Article Body Extract + Representative Clustering
----------------------------------------------------------------------------
목적
1) STEP3에서 법규/뉴스 본문을 확보한다.
2) 뉴스는 유사·중복 기사 Reuters/Bloomberg/Yahoo/MSN/Google RSS 재배포 등을 Cluster로 묶는다.
3) Cluster별 대표기사 1건만 3-2.news_article_summary.xlsx에 저장한다.
4) 전체 클러스터 멤버는 3-2.news_article_cluster_audit.xlsx에 별도 저장한다.

권장 위치: C:/Temp/3-1.regulation_merge.py
실행: python C:/Temp/3-1.regulation_merge.py

입력 후보
- Regulation: C:/Temp/3-1.regulation_summary.xlsx 또는 C:/Temp/1-1.regulation_raw.xlsx
- News:       C:/Temp/3-2.news_summary.xlsx 또는 C:/Temp/3-2.news_merge.xlsx
              없으면 1-2.site_news_raw.xlsx, 2-1.naver_news_raw.xlsx, 2-2.google_news_raw.xlsx, 2-3.rss_news_raw.xlsx 자동 병합

출력
- C:/Temp/3-1.regulation_article_summary.xlsx
- C:/Temp/3-2.news_article_summary.xlsx              # 대표기사만 저장
- C:/Temp/3-2.news_article_cluster_audit.xlsx        # 클러스터 전체 멤버 감사용
- C:/Temp/3-2.news_article_before_cluster.xlsx       # 클러스터 전 300건 원본 보존
"""

import os
import re
import json
import time
import hashlib
import warnings
import traceback
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, unquote, urlunparse, quote

import pandas as pd

warnings.filterwarnings("ignore")

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
BASE_DIR = r"C:/Temp"

# LAW1 rule: only 1-1.regulation_raw.xlsx is accepted as Regulation input.
# All other official notices / site outputs are News candidates via 1-2 / 2-x / 3-2.
REG_INPUT_CANDIDATES = [
    os.path.join(BASE_DIR, "1-1.regulation_raw.xlsx"),
]
NEWS_INPUT_CANDIDATES = [
    os.path.join(BASE_DIR, "3-2.news_summary.xlsx"),
    os.path.join(BASE_DIR, "3-2.news_merge.xlsx"),
    os.path.join(BASE_DIR, "3-2.news_merged.xlsx"),
]
NEWS_RAW_FALLBACKS = [
    os.path.join(BASE_DIR, "1-2.site_news_raw.xlsx"),
    os.path.join(BASE_DIR, "2-1.naver_news_raw.xlsx"),
    os.path.join(BASE_DIR, "2-2.google_news_raw.xlsx"),
    os.path.join(BASE_DIR, "2-3.rss_news_raw.xlsx"),
]

REG_OUT = os.path.join(BASE_DIR, "3-1.regulation_article_summary.xlsx")
NEWS_OUT = os.path.join(BASE_DIR, "3-2.news_article_summary.xlsx")
NEWS_BEFORE_CLUSTER_OUT = os.path.join(BASE_DIR, "3-2.news_article_before_cluster.xlsx")
NEWS_CLUSTER_AUDIT_OUT = os.path.join(BASE_DIR, "3-2.news_article_cluster_audit.xlsx")

# 뉴스 본문추출/클러스터링 대상 최대치. STEP3 전단에서 이미 300건이면 그대로 사용.
NEWS_MAX_ROWS = 300

# 대표 클러스터링 후 목표 상한. 원하면 180으로 고정, None이면 전체 대표기사 저장.
NEWS_REPRESENTATIVE_TARGET_MAX = 180

# 본문 추출 타임아웃
HTTP_TIMEOUT = 12
SLEEP_SEC = 0.15
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

# 대표기사 선정 시 우대 매체
SOURCE_PRIORITY = {
    "reuters": 100,
    "bloomberg": 96,
    "financial times": 94,
    "ft.com": 94,
    "associated press": 92,
    "ap news": 92,
    "wall street journal": 91,
    "wsj": 91,
    "nikkei": 90,
    "politico": 88,
    "cnbc": 84,
    "yahoo": 60,
    "msn": 58,
    "google": 45,
    "naver": 45,
}

# 클러스터링에서 의미 없는 단어 제거
STOPWORDS = set("""
 the a an and or of to in on for from by with at as is are was were be been being into over under after before amid against about this that these those it its their his her you your our will would could should may might can says said report reports update latest news breaking live why how what when where who korea korean china chinese us usa u.s united states eu europe european uk britain japan india vietnam mexico tariff tariffs trade customs duty duties import imports export exports regulation regulations government ministry agency samsung electronics global market markets economy economic business company companies new more first last june july august september october november december monday tuesday wednesday thursday friday saturday sunday
 관세 무역 통상 수입 수출 규제 법령 고시 공고 발표 보도자료 뉴스 관련 대상 적용 변경 개정 시행 정부 산업부 기재부 관세청 한국 중국 미국 일본 유럽 인도 베트남 멕시코 삼성전자 글로벌 주요 속보 최신
""".split())

ISSUE_KEYWORDS = [
    "entity list", "bis", "export control", "semiconductor", "chip", "tariff", "section 301", "section 232",
    "anti-dumping", "countervailing", "fta", "rules of origin", "origin", "hs code", "classification",
    "cbam", "uflpa", "forced labor", "sanction", "customs", "duty", "de minimis", "ev", "battery",
    "steel", "aluminum", "rare earth", "critical minerals", "china", "morocco", "eu", "us", "korea",
]

# -----------------------------------------------------------------------------
# UTIL
# -----------------------------------------------------------------------------
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    print(f"[{now_str()}] {msg}", flush=True)


def ensure_dir(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)


def first_existing(paths):
    for p in paths:
        if os.path.exists(p):
            return p
    return None


def read_excel_safe(path):
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_excel(path)
    except Exception as e:
        log(f"WARN read failed: {path} / {e}")
        return pd.DataFrame()


def clean_excel_cell(value, max_len=32000):
    """Excel/openpyxl 저장 불가 제어문자 제거 + 셀 최대 길이 보호."""
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    if not isinstance(value, str):
        return value

    value = ILLEGAL_CHARACTERS_RE.sub("", value)
    value = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]", "", value)
    value = (
        value.replace("\ufeff", "")
        .replace("\u200b", "")
        .replace("\u200c", "")
        .replace("\u200d", "")
    )
    # Excel 셀 최대 32,767자. 여유를 두고 32,000자로 제한.
    if max_len and len(value) > max_len:
        value = value[:max_len] + " ...[TRUNCATED_FOR_EXCEL]"
    return value


def sanitize_dataframe_for_excel(df):
    """DataFrame 전체를 Excel 안전 문자열로 변환한다."""
    if df is None:
        return pd.DataFrame()
    out = df.copy()
    for col in out.columns:
        out[col] = out[col].map(clean_excel_cell)
    # 컬럼명에도 불법문자 방어
    out.columns = [str(clean_excel_cell(c, max_len=200)) for c in out.columns]
    return out


def save_excel(df, path):
    ensure_dir(path)
    safe_df = sanitize_dataframe_for_excel(df)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        safe_df.to_excel(writer, index=False, sheet_name="data")
    log(f"[SAVE] {path}")


def s(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


def pick_col(df, names):
    low = {str(c).lower(): c for c in df.columns}
    for n in names:
        if n in df.columns:
            return n
        if n.lower() in low:
            return low[n.lower()]
    return None


def ensure_columns(df, defaults):
    for c, v in defaults.items():
        if c not in df.columns:
            df[c] = v
    return df


def normalize_url(url):
    url = s(url)
    if not url:
        return ""
    url = url.replace("&amp;", "&")
    # Google alert/rss redirect 처리
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        for key in ["url", "q", "u"]:
            if key in qs and qs[key]:
                cand = unquote(qs[key][0])
                if cand.startswith("http") and "google." not in urlparse(cand).netloc:
                    url = cand
                    parsed = urlparse(url)
                    break
        # tracking query 제거
        drop_keys = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid", "gclid"}
        qs2 = parse_qs(parsed.query)
        kept = []
        for k, vals in qs2.items():
            if k.lower() in drop_keys:
                continue
            for v in vals:
                kept.append((k, v))
        query = "&".join([f"{k}={v}" for k, v in kept])
        netloc = parsed.netloc.lower().replace("www.", "")
        path = re.sub(r"/+$", "", parsed.path)
        return urlunparse((parsed.scheme or "https", netloc, path, "", query, ""))
    except Exception:
        return url


# -----------------------------------------------------------------------------
# ORIGINAL URL RECOVERY - Google News redirect + multi URL columns
# -----------------------------------------------------------------------------
GOOGLE_RESOLVE_ENABLED = os.getenv("GTI_STEP3_GOOGLE_NEWS_RESOLVE", "1").strip().upper() not in {"0", "N", "NO", "FALSE"}
GOOGLE_RESOLVE_TIMEOUT = int(os.getenv("GTI_STEP3_GOOGLE_NEWS_RESOLVE_TIMEOUT", "10"))
_GOOGLE_RESOLVE_CACHE = {}



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


def is_google_article_redirect_url(url):
    raw = s(url).lower()
    if not raw.startswith(("http://", "https://")):
        return False
    parsed = urlparse(raw)
    return "news.google" in parsed.netloc.lower() and ("/rss/articles/" in parsed.path.lower() or "/articles/" in parsed.path.lower())


def is_real_original_url(url):
    raw = s(url)
    if not raw.startswith(("http://", "https://")):
        return False
    parsed = urlparse(raw.lower())
    if "news.google" in parsed.netloc or "google." in parsed.netloc:
        return False
    return True


def google_news_token(url):
    try:
        parsed = urlparse(s(url))
        parts = [x for x in parsed.path.split("/") if x]
        if len(parts) >= 2 and parts[-2] in {"articles", "read"}:
            return parts[-1]
    except Exception:
        pass
    return ""


def fetch_google_decode_params(token):
    try:
        import requests
        headers = {"User-Agent": USER_AGENT}
        for prefix in ("https://news.google.com/articles/", "https://news.google.com/rss/articles/"):
            r = requests.get(prefix + token, headers=headers, timeout=GOOGLE_RESOLVE_TIMEOUT)
            html = r.text or ""
            sig = re.search(r'data-n-a-sg="([^"]+)"', html)
            ts = re.search(r'data-n-a-ts="([^"]+)"', html)
            if sig and ts:
                return sig.group(1), ts.group(1)
    except Exception:
        pass
    return "", ""


def decode_google_news_token(token, signature, timestamp):
    try:
        import requests
        endpoint = "https://news.google.com/_/DotsSplashUi/data/batchexecute"
        payload = [
            "Fbv4je",
            (
                '["garturlreq",[["X","X",["X","X"],null,null,1,1,"US:en",null,1,'
                'null,null,null,null,null,0,1],"X","X",1,[1,1,1],1,1,null,0,0,null,0],'
                f'"{token}",{timestamp},"{signature}"]'
            ),
        ]
        body = "f.req=" + quote(json.dumps([[payload]], separators=(",", ":")))
        headers = {
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            "User-Agent": USER_AGENT,
        }
        r = requests.post(endpoint, data=body.encode("utf-8"), headers=headers, timeout=GOOGLE_RESOLVE_TIMEOUT)
        text = r.text or ""
        parsed = json.loads(text.split("\n\n", 1)[1])[:-2]
        return json.loads(parsed[0][2])[1]
    except Exception:
        return ""


def resolve_google_news_url(url):
    raw = s(url)
    if not GOOGLE_RESOLVE_ENABLED or not is_google_article_redirect_url(raw):
        return ""
    if raw in _GOOGLE_RESOLVE_CACHE:
        return _GOOGLE_RESOLVE_CACHE[raw]
    token = google_news_token(raw)
    if not token:
        _GOOGLE_RESOLVE_CACHE[raw] = ""
        return ""
    signature, timestamp = fetch_google_decode_params(token)
    if signature and timestamp:
        resolved = normalize_url(decode_google_news_token(token, signature, timestamp))
        if is_real_original_url(resolved):
            _GOOGLE_RESOLVE_CACHE[raw] = resolved
            return resolved
    _GOOGLE_RESOLVE_CACHE[raw] = ""
    return ""


def choose_source_url_for_body(row):
    """Prefer already-restored original links before fetching article body."""
    candidates = []
    for col in ["original_url", "OriginalURLCandidate", "BestLinkURL", "URL", "GoogleURL", "Link", "link"]:
        if col in row and s(row.get(col)):
            candidates.append(s(row.get(col)))

    # 1) non-Google original URL first
    for cand in candidates:
        norm = normalize_url(cand)
        if is_real_original_url(norm):
            return norm, "ORIGINAL_URL_SELECTED"

    # 2) Google News / Google Alert redirect decode, then Selenium browser fallback
    for cand in candidates:
        norm = normalize_url(cand)
        if is_google_article_redirect_url(norm):
            resolved = resolve_google_news_url(norm)
            if resolved:
                return resolved, "GOOGLE_NEWS_RESOLVED_STEP3"
        if is_google_intermediate_url(norm):
            resolved, status = resolve_google_url_by_selenium(norm, timeout=GOOGLE_RESOLVE_TIMEOUT)
            if resolved and is_real_original_url(resolved):
                return normalize_url(resolved), status or "RESOLVED_SELENIUM"
            return norm, status or "GOOGLE_REMAINED"

    # 3) last fallback
    for cand in candidates:
        norm = normalize_url(cand)
        if norm.startswith(("http://", "https://")):
            return norm, "FALLBACK_URL_SELECTED"
    return "", "EMPTY_URL"


def domain_of(url):
    try:
        return urlparse(normalize_url(url)).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def clean_text(text, max_len=None):
    text = s(text)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[\u200b\ufeff]", "", text)
    if max_len and len(text) > max_len:
        text = text[:max_len]
    return text.strip()


def normalize_title(title):
    t = s(title).lower()
    t = re.sub(r"\|.*$", " ", t)
    t = re.sub(r" - (reuters|bloomberg|yahoo finance|msn|cnbc|ap news|financial times|ft\.com).*$", " ", t)
    t = re.sub(r"\[(.*?)\]", " ", t)
    t = re.sub(r"\((reuters|bloomberg|ap|afp|yonhap|연합뉴스).*?\)", " ", t)
    t = re.sub(r"[^0-9a-z가-힣%]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def title_tokens(title):
    t = normalize_title(title)
    toks = [x for x in t.split() if len(x) >= 2 and x not in STOPWORDS]
    return toks


def jaccard(a, b):
    a, b = set(a), set(b)
    if not a or not b:
        return 0.0
    return len(a & b) / max(1, len(a | b))


def md5_short(text, n=12):
    return hashlib.md5(s(text).encode("utf-8", errors="ignore")).hexdigest()[:n]


def date_yyyymmdd(x):
    txt = s(x)
    if not txt:
        return "00000000"
    try:
        dt = pd.to_datetime(txt, errors="coerce")
        if pd.notna(dt):
            return dt.strftime("%Y%m%d")
    except Exception:
        pass
    m = re.search(r"(20\d{2})[-./]?(\d{1,2})[-./]?(\d{1,2})", txt)
    if m:
        return f"{m.group(1)}{int(m.group(2)):02d}{int(m.group(3)):02d}"
    return "00000000"


def to_num(x, default=0):
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default

# -----------------------------------------------------------------------------
# ARTICLE EXTRACTION
# -----------------------------------------------------------------------------
def requests_get_text(url):
    try:
        import requests
        headers = {"User-Agent": USER_AGENT, "Accept-Language": "ko,en-US;q=0.9,en;q=0.8"}
        r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT, allow_redirects=True)
        if r.status_code >= 400:
            return "", f"HTTP_{r.status_code}", url
        ct = r.headers.get("content-type", "").lower()
        if "pdf" in ct or normalize_url(r.url).lower().endswith(".pdf"):
            return "", "PDF_NOT_PARSED", r.url
        html = r.text or ""
        if len(html) < 200:
            return "", "HTML_TOO_SHORT", r.url
        return extract_text_from_html(html), "FETCHED_HTML", r.url
    except Exception as e:
        return "", f"FETCH_ERROR:{type(e).__name__}", url


def extract_text_from_html(html):
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "form", "aside"]):
            tag.decompose()
        candidates = []
        for sel in ["article", "main", "div.article", "div.article-body", "div.story-body", "div.entry-content", "section"]:
            for node in soup.select(sel):
                txt = clean_text(node.get_text(" "))
                if len(txt) > 300:
                    candidates.append(txt)
        if candidates:
            candidates.sort(key=len, reverse=True)
            return clean_text(candidates[0], 12000)
        body = clean_text(soup.get_text(" "), 12000)
        # 반복 메뉴성 문구 제거 후에도 짧으면 실패 처리에서 걸림
        return body
    except Exception:
        text = re.sub(r"<script.*?</script>", " ", html, flags=re.I | re.S)
        text = re.sub(r"<style.*?</style>", " ", text, flags=re.I | re.S)
        text = re.sub(r"<[^>]+>", " ", text)
        return clean_text(text, 12000)


def is_bad_body(text):
    txt = clean_text(text)
    if len(txt) < 180:
        return True, "TOO_SHORT"
    low = txt.lower()
    bad_markers = [
        "enable javascript", "access denied", "robot check", "captcha", "cookies are disabled",
        "페이지를 찾을 수", "서비스 이용에 불편", "검색 결과", "메뉴", "로그인",
    ]
    if any(m in low for m in bad_markers):
        return True, "BLOCK_OR_MENU_PAGE"
    # 단어 다양성이 낮으면 메뉴 페이지 가능성
    words = re.findall(r"[A-Za-z가-힣0-9]+", low)
    if len(set(words)) < 40 and len(txt) < 600:
        return True, "LOW_SIGNAL"
    return False, "OK"


def extract_article_for_row(row, is_regulation=False):
    url, url_status = choose_source_url_for_body(row)
    if not url:
        url = normalize_url(row.get("URL", "") or row.get("original_url", ""))
        url_status = "LEGACY_URL_FALLBACK" if url else "EMPTY_URL"
    original_url = url
    existing = clean_text(row.get("article_body", ""))
    if existing:
        bad, status = is_bad_body(existing)
        if not bad:
            return existing, "EXISTING_BODY_OK", "OFFICIAL" if is_regulation else "MEDIA", "Y", "OK", "EXISTING", len(existing), original_url

    # 입력 요약이 있는 경우 fallback 후보
    fallback_parts = []
    for c in ["Summary", "Description", "Snippet", "Content", "Headline", "Title"]:
        if c in row and s(row.get(c)):
            fallback_parts.append(s(row.get(c)))
    fallback = clean_text(" ".join(fallback_parts), 3000)

    body = ""
    status = url_status or "EMPTY_URL"
    final_url = original_url
    if url.startswith("http"):
        body, fetch_status, final_url = requests_get_text(url)
        status = f"{url_status}|{fetch_status}" if url_status else fetch_status
        time.sleep(SLEEP_SEC)

    body = clean_text(body, 12000)
    bad, q = is_bad_body(body)
    if not bad:
        return body, status, "OFFICIAL" if is_regulation else "MEDIA", "Y", "OK", "FETCHED_HTML", len(body), final_url

    if len(fallback) >= 40:
        return fallback, "INPUT_FALLBACK", "OFFICIAL" if is_regulation else "MEDIA", "Y", "FALLBACK_OK", "INPUT_FALLBACK", len(fallback), final_url

    return "", f"{status}:{q}", "OFFICIAL" if is_regulation else "MEDIA", "N", q if q else "EMPTY", "EMPTY", 0, final_url


def add_hints(df):
    def hint_effective(text):
        t = s(text)
        hits = []
        patterns = [
            r"시행\s*20\d{2}[.\-년\s]\s*\d{1,2}[.\-월\s]\s*\d{1,2}",
            r"20\d{2}[.\-/년\s]\s*\d{1,2}[.\-/월\s]\s*\d{1,2}\s*(?:부터|까지|시행|effective|takes effect)",
            r"effective\s+(?:on\s+)?[A-Z][a-z]+\s+\d{1,2},\s*20\d{2}",
        ]
        for p in patterns:
            hits += re.findall(p, t, flags=re.I)
        return "; ".join(dict.fromkeys([clean_text(x) for x in hits[:6]]))

    def hint_hs(text):
        t = s(text)
        hits = re.findall(r"\b(?:HS|HTS|HTSUS|품목번호|세번)\s*[:#]?\s*([0-9]{4}(?:\.[0-9]{2,6})?)", t, flags=re.I)
        hits += re.findall(r"\b([0-9]{4}\.[0-9]{2,6})\b", t)
        return "; ".join(dict.fromkeys(hits[:10]))

    def hint_rate(text):
        t = s(text)
        hits = re.findall(r"(?:관세율|세율|tariff|duty|rate)[^.;\n]{0,40}?\b([0-9]{1,3}(?:\.[0-9]+)?\s*%)", t, flags=re.I)
        hits += re.findall(r"\b([0-9]{1,3}(?:\.[0-9]+)?\s*%)\s*(?:tariff|duty|관세|세율)", t, flags=re.I)
        return "; ".join(dict.fromkeys([clean_text(x) for x in hits[:10]]))

    def hint_change(text):
        t = clean_text(text, 2000)
        # 본문 초반 및 change 문장 일부만 보존
        sent = re.split(r"(?<=[.!?。])\s+", t)
        keys = ["tariff", "duty", "customs", "export control", "FTA", "origin", "HS", "관세", "세율", "수입", "수출", "시행", "개정", "할당관세"]
        picked = [x for x in sent if any(k.lower() in x.lower() for k in keys)]
        return clean_text("; ".join(picked[:3]), 1200)

    for c in ["effective_date_hint", "change_detail_hint", "hs_hint", "tariff_rate_hint"]:
        if c not in df.columns:
            df[c] = ""
    bodies = df.get("article_body", pd.Series([""] * len(df)))
    df["effective_date_hint"] = [hint_effective(x) for x in bodies]
    df["change_detail_hint"] = [hint_change(x) for x in bodies]
    df["hs_hint"] = [hint_hs(x) for x in bodies]
    df["tariff_rate_hint"] = [hint_rate(x) for x in bodies]
    return df


def article_quality_rank(row):
    """Higher is better: fetched article text should beat metadata fallback."""
    source = s(row.get("article_body_source", "")).upper()
    ok = s(row.get("article_body_ok", "")).upper()
    chars = to_num(row.get("article_body_chars"), 0)
    status = s(row.get("article_quality_status", "")).upper()

    if source in {"FETCHED_HTML", "EXISTING"} and ok == "Y":
        if chars >= 1200:
            return 100
        if chars >= 500:
            return 92
        return 84
    if source in {"OFFICIAL_METADATA_FALLBACK"} and ok == "Y":
        return 78
    if source in {"INPUT_FALLBACK"} and ok == "Y":
        if chars >= 500:
            return 64
        return 55
    if "FALLBACK" in status and ok == "Y":
        return 50
    return 0


def step4_body_penalty(row):
    rank = article_quality_rank(row)
    source = s(row.get("article_body_source", "")).upper()
    ok = s(row.get("article_body_ok", "")).upper()

    if ok != "Y":
        return -45
    if source == "INPUT_FALLBACK":
        return -18
    if rank < 70:
        return -12
    return 0


def step4_body_hint(row):
    source = s(row.get("article_body_source", "")).upper()
    ok = s(row.get("article_body_ok", "")).upper()
    status = s(row.get("article_extract_status", ""))
    hints = []

    if ok != "Y":
        hints.append("body_missing_step4_exclude_or_heavy_discount")
    elif source == "INPUT_FALLBACK":
        hints.append("metadata_fallback_step4_discount")
    elif source in {"FETCHED_HTML", "EXISTING"}:
        hints.append("full_body_available")
    elif "FALLBACK" in source:
        hints.append("fallback_body_review")

    if "GOOGLE" in status.upper() or "EMPTY_URL" in status.upper():
        hints.append("url_or_google_resolution_issue")

    return "; ".join(hints)

# -----------------------------------------------------------------------------
# INPUT LOAD
# -----------------------------------------------------------------------------
def load_regulation_input():
    p = first_existing(REG_INPUT_CANDIDATES)
    if not p:
        log("[REGULATION] input not found")
        return pd.DataFrame()
    df = read_excel_safe(p)
    log(f"[REGULATION] input={p} rows={len(df)}")
    return df


def load_news_input():
    p = first_existing(NEWS_INPUT_CANDIDATES)
    if p:
        df = read_excel_safe(p)
        log(f"[NEWS] input={p} rows={len(df)}")
    else:
        frames = []
        for fp in NEWS_RAW_FALLBACKS:
            d = read_excel_safe(fp)
            if len(d):
                d["SourceFile"] = os.path.basename(fp)
                frames.append(d)
                log(f"[NEWS] fallback load {os.path.basename(fp)} rows={len(d)}")
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        log(f"[NEWS] fallback total rows={len(df)}")

    if df.empty:
        return df

    # Headline/URL 표준화
    hcol = pick_col(df, ["Headline", "Title", "title", "제목"])
    ucol = pick_col(df, ["URL", "Link", "link", "url", "링크"])
    if hcol and hcol != "Headline":
        df["Headline"] = df[hcol]
    if ucol and ucol != "URL":
        df["URL"] = df[ucol]
    df = ensure_columns(df, {"Headline": "", "URL": ""})

    # 점수 컬럼이 없으면 기본값
    for c in ["FinalScore", "Score", "TopicScore", "RiskScore", "SamsungImpactScore"]:
        if c not in df.columns:
            df[c] = 0

    # URL 기준 1차 중복 제거
    df["_norm_url"] = df["URL"].map(normalize_url)
    df["_title_norm"] = df["Headline"].map(normalize_title)
    df = df.sort_values(by=["FinalScore", "Score"], ascending=False, na_position="last")
    df = df.drop_duplicates(subset=["_norm_url"], keep="first")
    df = df.drop_duplicates(subset=["_title_norm"], keep="first")

    # 상위 NEWS_MAX_ROWS만 본문 추출. 이미 STEP3 전단에서 300건 선별된 구조 유지.
    if len(df) > NEWS_MAX_ROWS:
        df = df.head(NEWS_MAX_ROWS).copy()
    else:
        df = df.copy()
    df.drop(columns=[c for c in ["_norm_url", "_title_norm"] if c in df.columns], inplace=True, errors="ignore")
    log(f"[NEWS] target rows={len(df)}")
    return df

# -----------------------------------------------------------------------------
# BODY EXTRACTION PIPELINE
# -----------------------------------------------------------------------------
def process_articles(df, is_regulation=False):
    if df.empty:
        return df
    defaults = {
        "original_url": "",
        "article_body": "",
        "article_extract_status": "",
        "article_source_type": "OFFICIAL" if is_regulation else "MEDIA",
        "article_body_ok": "N",
        "article_quality_status": "",
        "article_body_source": "",
        "article_body_chars": 0,
        "article_quality_rank": 0,
        "step4_body_penalty": 0,
        "step4_body_hint": "",
        "article_last_checked": "",
    }
    df = ensure_columns(df, defaults)
    rows = []
    total = len(df)
    for idx, row in df.iterrows():
        body, status, source_type, ok, qstat, bsource, chars, final_url = extract_article_for_row(row, is_regulation=is_regulation)
        r = row.to_dict()
        r["original_url"] = final_url or s(row.get("URL", ""))
        r["article_body"] = body
        r["article_extract_status"] = status
        r["article_source_type"] = source_type
        r["article_body_ok"] = ok
        r["article_quality_status"] = qstat
        r["article_body_source"] = bsource
        r["article_body_chars"] = chars
        r["article_quality_rank"] = article_quality_rank(pd.Series(r))
        r["step4_body_penalty"] = step4_body_penalty(pd.Series(r))
        r["step4_body_hint"] = step4_body_hint(pd.Series(r))
        r["article_last_checked"] = now_str()
        rows.append(r)
        if not is_regulation and len(rows) % 25 == 0:
            log(f"[NEWS] extracted={len(rows)}/{total}")
    out = pd.DataFrame(rows)
    out = add_hints(out)
    return out

# -----------------------------------------------------------------------------
# REPRESENTATIVE CLUSTERING
# -----------------------------------------------------------------------------
def source_priority(row):
    text = " ".join([s(row.get("Agency", "")), s(row.get("Source", "")), s(row.get("Publisher", "")), domain_of(row.get("URL", ""))]).lower()
    best = 0
    for k, v in SOURCE_PRIORITY.items():
        if k in text:
            best = max(best, v)
    return best


def issue_signature(row):
    title = s(row.get("Headline", ""))
    body = s(row.get("article_body", ""))[:2000]
    country = s(row.get("Country", ""))
    issue = s(row.get("IssueKey", "")) or s(row.get("Category", ""))
    text = (title + " " + body).lower()

    matched = []
    for kw in ISSUE_KEYWORDS:
        if kw.lower() in text:
            matched.append(kw.replace(" ", "_"))
    if not matched:
        toks = title_tokens(title)
        # 숫자/고유명사성 토큰 우선
        scored = []
        for tok in toks:
            if tok in STOPWORDS:
                continue
            weight = 1
            if re.search(r"\d", tok):
                weight += 2
            if len(tok) >= 6:
                weight += 1
            scored.append((weight, tok))
        scored.sort(reverse=True)
        matched = [t for _, t in scored[:5]]

    country_norm = re.sub(r"[^a-z가-힣,]+", "", country.lower())[:30]
    issue_norm = re.sub(r"[^a-z0-9가-힣_]+", "", issue.lower())[:30]
    sig = "_".join([x for x in [issue_norm, country_norm] + matched[:6] if x])
    if not sig:
        sig = normalize_title(title)[:80]
    return sig


def initial_cluster_key(row):
    # 기존 IssueClusterKey가 충분히 의미 있으면 우선 활용
    existing = s(row.get("IssueClusterKey", ""))
    if existing and len(existing) >= 8 and existing.lower() not in ["nan", "none", "null"]:
        return "EXISTING_" + re.sub(r"[^a-zA-Z0-9가-힣_\-]+", "_", existing.lower())[:100]

    url = normalize_url(row.get("URL", ""))
    dom = domain_of(url)
    title_norm = normalize_title(row.get("Headline", ""))
    toks = title_tokens(title_norm)

    # Google/MSN/Yahoo 등 재배포는 제목 기반으로 강하게 묶음
    if any(x in dom for x in ["google", "yahoo", "msn", "news.google", "finance.yahoo"]):
        return "TITLE_" + md5_short(" ".join(toks[:12]))

    # 일반 기사도 이슈 서명 기반 1차 묶음
    return "ISSUE_" + md5_short(issue_signature(row))


def refine_clusters(df):
    """
    1차 key로 묶은 후, key가 다르더라도 제목 토큰 유사도가 높은 singleton/소형 cluster를 추가 병합.
    외부 라이브러리 없이 300건 수준에서 충분히 빠르게 동작.
    """
    df = df.copy()
    df["_tokens"] = df["Headline"].map(title_tokens)
    df["_sig"] = df.apply(issue_signature, axis=1)
    df["_cluster_key"] = df.apply(initial_cluster_key, axis=1)

    # Union-Find
    parent = list(range(len(df)))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    # 같은 1차 key 병합
    key_to_idx = {}
    for i, k in enumerate(df["_cluster_key"].tolist()):
        if k in key_to_idx:
            union(key_to_idx[k], i)
        else:
            key_to_idx[k] = i

    # 제목/이슈 유사도 병합
    records = df.to_dict("records")
    n = len(records)
    for i in range(n):
        ti = records[i]["_tokens"]
        if len(ti) < 3:
            continue
        for j in range(i + 1, n):
            tj = records[j]["_tokens"]
            if len(tj) < 3:
                continue
            sim = jaccard(ti, tj)
            same_issue = records[i]["_sig"] == records[j]["_sig"]
            # Reuters/Bloomberg/Yahoo/MSN 같은 같은 기사 제목은 대체로 0.55 이상
            if sim >= 0.58 or (same_issue and sim >= 0.42):
                union(i, j)

    root_to_id = {}
    ids = []
    for i in range(n):
        r = find(i)
        if r not in root_to_id:
            # 날짜 + 이슈 signature 기반 ID
            d = date_yyyymmdd(records[i].get("Date", ""))
            sig = re.sub(r"[^a-zA-Z0-9가-힣_]+", "_", records[i]["_sig"])[:60]
            root_to_id[r] = f"CL_{d}_{md5_short(sig, 8)}"
        ids.append(root_to_id[r])
    df["ClusterID"] = ids
    return df


def representative_score(row):
    score = 0.0
    score += to_num(row.get("FinalScore"), 0) * 10
    score += to_num(row.get("Score"), 0) * 4
    score += to_num(row.get("TopicScore"), 0) * 2
    score += to_num(row.get("SamsungImpactScore"), 0) * 2
    score += source_priority(row) * 3
    quality_rank = article_quality_rank(row)
    body_source = s(row.get("article_body_source", "")).upper()
    score += quality_rank * 5
    if body_source in {"FETCHED_HTML", "EXISTING"}:
        score += 260
    elif body_source == "INPUT_FALLBACK":
        score -= 120
    elif s(row.get("article_body_ok")) != "Y":
        score -= 300
    if s(row.get("article_body_ok")) == "Y":
        score += 180
    score += min(to_num(row.get("article_body_chars"), 0), 8000) / 30
    if s(row.get("Priority", "")).upper() == "CORE":
        score += 120
    if s(row.get("Tier", "")).upper() == "CORE":
        score += 120
    # Google Alert URL 자체는 대표기사에서 감점. 단 원문 URL 해결됐으면 감점 적음.
    dom = domain_of(row.get("URL", ""))
    if "google" in dom:
        score -= 80
    if s(row.get("URLRestoreStatus", "")).upper() == "GOOGLE_UNRESOLVED":
        score -= 90
    if "msn" in dom:
        score -= 20
    return score


def make_cluster_representatives(df):
    if df.empty:
        return df, df
    work = refine_clusters(df)
    work["_rep_score"] = work.apply(representative_score, axis=1)

    rep_rows = []
    audit_rows = []
    for cid, g in work.groupby("ClusterID", dropna=False):
        g = g.copy().sort_values("_rep_score", ascending=False)
        rep = g.iloc[0].copy()
        cluster_size = len(g)
        related_count = max(0, cluster_size - 1)
        urls = [s(x) for x in g.get("URL", pd.Series()).tolist() if s(x)]
        original_urls = [s(x) for x in g.get("original_url", pd.Series()).tolist() if s(x)]
        all_urls = list(dict.fromkeys(urls + original_urls))
        titles = [clean_text(x, 250) for x in g.get("Headline", pd.Series()).tolist() if s(x)]
        sources = []
        for _, rr in g.iterrows():
            src = s(rr.get("Agency", "")) or s(rr.get("Publisher", "")) or s(rr.get("Source", "")) or domain_of(rr.get("URL", ""))
            if src:
                sources.append(src)
        sources = list(dict.fromkeys(sources))

        rep["ClusterID"] = cid
        rep["ClusterSize"] = cluster_size
        rep["RelatedCount"] = related_count
        rep["DuplicateCount"] = related_count
        rep["RepresentativeURL"] = s(rep.get("URL", ""))
        rep["RelatedURLs"] = " | ".join(all_urls[:30])
        rep["RelatedSources"] = " | ".join(sources[:30])
        rep["ClusterSources"] = " | ".join(sources[:30])
        rep["ClusterHeadlines"] = " | ".join(titles[:20])
        rep["ClusterMemberTitles"] = " | ".join(titles[:30])
        rep["RepresentativeReason"] = (
            f"대표기사 선정: FinalScore={s(rep.get('FinalScore'))}, "
            f"SourcePriority={source_priority(rep)}, BodyOK={s(rep.get('article_body_ok'))}, "
            f"BodySource={s(rep.get('article_body_source'))}, "
            f"QualityRank={s(rep.get('article_quality_rank'))}, "
            f"ClusterSize={cluster_size}"
        )
        members = []
        for _, m in g.iterrows():
            members.append({
                "Headline": s(m.get("Headline", "")),
                "URL": s(m.get("URL", "")),
                "Source": s(m.get("Agency", "")) or s(m.get("Publisher", "")) or s(m.get("Source", "")),
                "FinalScore": to_num(m.get("FinalScore"), 0),
                "BodyOK": s(m.get("article_body_ok", "")),
                "BodySource": s(m.get("article_body_source", "")),
                "QualityRank": to_num(m.get("article_quality_rank"), 0),
                "Step4BodyHint": s(m.get("step4_body_hint", "")),
            })
        rep["ClusterMembersJSON"] = json.dumps(members, ensure_ascii=False)
        rep_rows.append(rep)

        rank = 1
        for _, m in g.iterrows():
            ar = m.copy()
            ar["ClusterID"] = cid
            ar["ClusterRank"] = rank
            ar["IsRepresentative"] = "Y" if rank == 1 else "N"
            ar["RepresentativeHeadline"] = s(rep.get("Headline", ""))
            ar["RepresentativeURL"] = s(rep.get("URL", ""))
            ar["RelatedCount"] = related_count
            audit_rows.append(ar)
            rank += 1

    reps = pd.DataFrame(rep_rows)
    audit = pd.DataFrame(audit_rows)

    # 대표기사 재정렬: CORE/FinalScore/클러스터크기/본문품질 우선
    sort_cols = []
    for c in ["Priority", "Tier"]:
        if c in reps.columns:
            reps[f"_{c}_sort"] = reps[c].astype(str).str.upper().map(lambda x: 1 if x == "CORE" else 0)
            sort_cols.append(f"_{c}_sort")
    for c in ["FinalScore", "Score", "ClusterSize", "article_quality_rank", "article_body_chars"]:
        if c in reps.columns:
            sort_cols.append(c)
    if sort_cols:
        reps = reps.sort_values(sort_cols, ascending=[False] * len(sort_cols), na_position="last")

    # 목표 180건으로 제한. 단 CORE는 우선 보존.
    if NEWS_REPRESENTATIVE_TARGET_MAX and len(reps) > NEWS_REPRESENTATIVE_TARGET_MAX:
        reps = reps.head(NEWS_REPRESENTATIVE_TARGET_MAX).copy()

    # 내부 컬럼 제거
    drop_cols = [c for c in reps.columns if c.startswith("_")]
    reps.drop(columns=drop_cols, inplace=True, errors="ignore")
    audit.drop(columns=[c for c in audit.columns if c.startswith("_")], inplace=True, errors="ignore")
    return reps, audit

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main():
    log("STEP3 REGULATION/NEWS ARTICLE SUMMARY START")

    # 1) Regulation
    reg = load_regulation_input()
    if not reg.empty:
        reg2 = process_articles(reg, is_regulation=True)
        body_ok = int((reg2.get("article_body_ok", "") == "Y").sum())
        bad = len(reg2) - body_ok
        fetched = int((reg2.get("article_body_source", "") == "FETCHED_HTML").sum())
        fallback = int((reg2.get("article_body_source", "") == "INPUT_FALLBACK").sum())
        google_unresolved = int(reg2.get("article_extract_status", pd.Series(dtype=str)).astype(str).str.contains("GOOGLE", case=False, na=False).sum())
        log(f"[REGULATION] rows={len(reg2)}, body_ok={body_ok}, fetched_ok={fetched}, fallback_ok={fallback}, google_unresolved={google_unresolved}, bad_or_empty={bad}")
        save_excel(reg2, REG_OUT)
    else:
        log("[REGULATION] skipped")

    # 2) News
    news = load_news_input()
    if news.empty:
        log("[NEWS] skipped - no input")
        return

    news2 = process_articles(news, is_regulation=False)
    save_excel(news2, NEWS_BEFORE_CLUSTER_OUT)

    before_rows = len(news2)
    body_ok = int((news2.get("article_body_ok", "") == "Y").sum())
    fetched = int((news2.get("article_body_source", "") == "FETCHED_HTML").sum())
    fallback = int((news2.get("article_body_source", "") == "INPUT_FALLBACK").sum())
    google_unresolved = int(news2.get("article_extract_status", pd.Series(dtype=str)).astype(str).str.contains("GOOGLE|google|alerts|EMPTY_URL", na=False).sum())
    bad = before_rows - body_ok
    step4_discount = int(news2.get("step4_body_penalty", pd.Series(dtype=int)).astype(str).ne("0").sum())
    log(f"[NEWS-BEFORE-CLUSTER] rows={before_rows}, body_ok={body_ok}, fetched_ok={fetched}, fallback_ok={fallback}, google_unresolved={google_unresolved}, bad_or_empty={bad}, step4_body_discount={step4_discount}")

    reps, audit = make_cluster_representatives(news2)
    after_rows = len(reps)
    reduced = before_rows - after_rows
    rate = (reduced / before_rows * 100) if before_rows else 0
    clustered_groups = int((audit.groupby("ClusterID").size() > 1).sum()) if not audit.empty and "ClusterID" in audit.columns else 0
    max_cluster = int(audit.groupby("ClusterID").size().max()) if not audit.empty and "ClusterID" in audit.columns else 0

    save_excel(reps, NEWS_OUT)
    save_excel(audit, NEWS_CLUSTER_AUDIT_OUT)

    body_ok2 = int((reps.get("article_body_ok", "") == "Y").sum()) if not reps.empty else 0
    fetched2 = int((reps.get("article_body_source", "") == "FETCHED_HTML").sum()) if not reps.empty else 0
    fallback2 = int((reps.get("article_body_source", "") == "INPUT_FALLBACK").sum()) if not reps.empty else 0
    bad2 = after_rows - body_ok2
    step4_discount2 = int(reps.get("step4_body_penalty", pd.Series(dtype=int)).astype(str).ne("0").sum()) if not reps.empty else 0

    log(f"[NEWS-REP-CLUSTER] before={before_rows}, after={after_rows}, reduced={reduced}, reduction_rate={rate:.1f}%")
    log(f"[NEWS-REP-CLUSTER] clustered_groups={clustered_groups}, max_cluster_size={max_cluster}")
    log(f"[NEWS] rows={after_rows}, body_ok={body_ok2}, fetched_ok={fetched2}, fallback_ok={fallback2}, bad_or_empty={bad2}, step4_body_discount={step4_discount2}")
    log("STEP3 DONE")


# =========================================================
# GTI Regulation body recovery override
# Keep this block immediately before __main__.
# =========================================================

def _u_reg(s0: str) -> str:
    return s0.encode("ascii").decode("unicode_escape")


REG_TARIFF_TERMS = [
    _u_reg("\\uad00\\uc138"), _u_reg("\\uad00\\uc138\\uc728"), _u_reg("\\ud1b5\\uad00"),
    _u_reg("\\uc218\\uc785"), _u_reg("\\uc218\\ucd9c"), _u_reg("\\uc6d0\\uc0b0\\uc9c0"),
    _u_reg("\\ud488\\ubaa9\\ubd84\\ub958"), _u_reg("\\ubc18\\ub364\\ud551"),
    _u_reg("\\uc0c1\\uacc4\\uad00\\uc138"), _u_reg("\\uc218\\ucd9c\\ud1b5\\uc81c"),
    "customs", "tariff", "duty", "import", "export", "rules of origin",
    "origin", "fta", "cepa", "epa", "hs code", "classification",
    "anti-dumping", "antidumping", "countervailing", "safeguard",
    "section 301", "section 232", "export control", "entity list", "cbam",
]

REG_OFFICIAL_DOMAINS = [
    "law.go.kr", "gwanbo.go.kr", "customs.go.kr", "unipass.customs.go.kr",
    "motir.go.kr", "federalregister.gov", "ustr.gov", "cbp.gov", "usitc.gov",
    "eur-lex.europa.eu", "taxation-customs.ec.europa.eu", "wto.org",
]


def _pick_value_case(row, names):
    for name in names:
        if name in row and s(row.get(name)):
            return s(row.get(name))
    lower = {str(k).lower(): k for k in getattr(row, "index", [])}
    for name in names:
        k = lower.get(str(name).lower())
        if k is not None and s(row.get(k)):
            return s(row.get(k))
    return ""


def _reg_join(row) -> str:
    return " ".join([
        _pick_value_case(row, ["Headline", "Title", "title"]),
        _pick_value_case(row, ["URL", "url", "Link", "link"]),
        _pick_value_case(row, ["Source", "source"]),
        _pick_value_case(row, ["Agency", "agency"]),
        _pick_value_case(row, ["official_regulation_reason"]),
        _pick_value_case(row, ["matched_policy_terms"]),
    ])


def _is_official_reg_row(row) -> bool:
    text = _reg_join(row).lower()
    if _pick_value_case(row, ["official_regulation_flag"]).upper() == "Y":
        return True
    if any(d in text for d in REG_OFFICIAL_DOMAINS):
        return True
    return _pick_value_case(row, ["site_type"]).lower() == "regulation"


def _has_trade_reg_signal(row) -> bool:
    text = _reg_join(row).lower()
    return any(str(t).lower() in text for t in REG_TARIFF_TERMS)


def _build_reg_fallback_body(row, fetch_status="") -> str:
    parts = [
        "OFFICIAL REGULATION FALLBACK BODY",
        f"Title: {_pick_value_case(row, ['Headline', 'Title', 'title'])}",
        f"Date: {_pick_value_case(row, ['Date', 'date'])}",
        f"Agency: {_pick_value_case(row, ['Agency', 'agency'])}",
        f"URL: {_pick_value_case(row, ['URL', 'url', 'original_url', 'Link', 'link'])}",
        f"Source: {_pick_value_case(row, ['Source', 'source'])}",
        f"FetchStatus: {fetch_status}",
        f"Signals: {_pick_value_case(row, ['official_regulation_reason', 'matched_policy_terms'])}",
        f"ExistingText: {_pick_value_case(row, ['regulation_fallback_body', 'Summary', 'summary', 'Description', 'description', 'Snippet', 'Content', 'article_body'])}",
    ]
    return clean_text(" | ".join([p for p in parts if p and not p.endswith(': ')]), 6000)


def extract_article_for_row(row, is_regulation=False):
    """Final override: recover the best source URL, then fetch article body.

    Fix:
    - url_status is always defined.
    - Use choose_source_url_for_body() so BestLinkURL / OriginalURLCandidate / GoogleURL are considered.
    - For regulation rows, create official metadata fallback instead of failing when body fetch is blocked.
    """
    try:
        url, url_status = choose_source_url_for_body(row)
    except Exception as exc:
        url = ""
        url_status = f"URL_CHOOSE_ERROR:{type(exc).__name__}"

    if not url:
        legacy = _pick_value_case(row, [
            "original_url", "OriginalURLCandidate", "BestLinkURL", "URL", "url", "GoogleURL", "Link", "link"
        ])
        url = normalize_url(legacy)
        url_status = "LEGACY_URL_FALLBACK" if url else "EMPTY_URL"

    original_url = url
    existing = clean_text(_pick_value_case(row, ["article_body"]))
    if existing:
        bad, existing_status = is_bad_body(existing)
        if not bad:
            return existing, "EXISTING_BODY_OK", "OFFICIAL" if is_regulation else "MEDIA", "Y", "OK", "EXISTING", len(existing), original_url

    fallback = clean_text(" ".join(
        _pick_value_case(row, [c]) for c in [
            "regulation_fallback_body", "Summary", "summary", "Description", "description",
            "Snippet", "Content", "Headline", "Title", "title",
        ] if _pick_value_case(row, [c])
    ), 4000)

    body = ""
    status = url_status or "EMPTY_URL"
    final_url = original_url
    if url.startswith("http"):
        body, fetch_status, final_url = requests_get_text(url)
        status = f"{url_status}|{fetch_status}" if url_status else fetch_status
        time.sleep(SLEEP_SEC)

    body = clean_text(body, 12000)
    bad, q = is_bad_body(body)
    if not bad:
        return body, status, "OFFICIAL" if is_regulation else "MEDIA", "Y", "OK", "FETCHED_HTML", len(body), final_url

    # Regulation is important: if official source/body is blocked, keep metadata body for Step4 review.
    if is_regulation and _is_official_reg_row(row):
        reg_body = _build_reg_fallback_body(row, fetch_status=f"{status}:{q}")
        quality = "OFFICIAL_TRADE_FALLBACK" if _has_trade_reg_signal(row) else "OFFICIAL_FALLBACK_REVIEW"
        return reg_body, f"{status}:{q}:OFFICIAL_FALLBACK", "OFFICIAL", "Y", quality, "OFFICIAL_METADATA_FALLBACK", len(reg_body), final_url

    if len(fallback) >= 40:
        return fallback, "INPUT_FALLBACK", "OFFICIAL" if is_regulation else "MEDIA", "Y", "FALLBACK_OK", "INPUT_FALLBACK", len(fallback), final_url

    return "", f"{status}:{q}", "OFFICIAL" if is_regulation else "MEDIA", "N", q if q else "EMPTY", "EMPTY", 0, final_url

def load_regulation_input():
    p = first_existing(REG_INPUT_CANDIDATES)
    if not p:
        log("[REGULATION] input not found")
        return pd.DataFrame()
    df = read_excel_safe(p)
    log(f"[REGULATION] input={p} rows={len(df)}")
    if df.empty:
        return df
    hcol = pick_col(df, ["Headline", "Title", "title"])
    ucol = pick_col(df, ["URL", "url", "Link", "link"])
    dcol = pick_col(df, ["Date", "date"])
    acol = pick_col(df, ["Agency", "agency"])
    scol = pick_col(df, ["Source", "source"])
    if hcol and hcol != "Headline":
        df["Headline"] = df[hcol]
    if ucol and ucol != "URL":
        df["URL"] = df[ucol]
    if dcol and dcol != "Date":
        df["Date"] = df[dcol]
    if acol and acol != "Agency":
        df["Agency"] = df[acol]
    if scol and scol != "Source":
        df["Source"] = df[scol]
    return ensure_columns(df, {"Headline": "", "URL": "", "Date": "", "Agency": "", "Source": ""})


def add_hints(df):
    for c in ["effective_date_hint", "change_detail_hint", "hs_hint", "tariff_rate_hint"]:
        if c not in df.columns:
            df[c] = ""
    bodies = df.get("article_body", pd.Series([""] * len(df)))
    def hint_effective_safe(text):
        t = s(text)
        hits = re.findall(r"20\d{2}[.\-/]\s*\d{1,2}[.\-/]\s*\d{1,2}", t)
        hits += re.findall(r"effective\s+(?:on\s+)?[A-Z][a-z]+\s+\d{1,2},\s*20\d{2}", t, flags=re.I)
        return "; ".join(dict.fromkeys([clean_text(x) for x in hits[:6]]))
    def hint_hs_safe(text):
        t = s(text)
        hits = re.findall(r"\b(?:HS|HTS|HTSUS|hs code|tariff classification).{0,20}?([0-9]{4}(?:\.[0-9]{2,6})?)", t, flags=re.I)
        hits += re.findall(r"\b([0-9]{4}\.[0-9]{2,6})\b", t)
        return "; ".join(dict.fromkeys(hits[:10]))
    def hint_rate_safe(text):
        t = s(text)
        hits = re.findall(r"(?:tariff|duty|rate).{0,40}?\b([0-9]{1,3}(?:\.[0-9]+)?\s*%)", t, flags=re.I)
        hits += re.findall(r"\b([0-9]{1,3}(?:\.[0-9]+)?\s*%)", t)
        return "; ".join(dict.fromkeys([clean_text(x) for x in hits[:10]]))
    def hint_change_safe(text):
        t = clean_text(text, 2000)
        sent = re.split(r"(?<=[.!?])\s+", t)
        keys = ["tariff", "duty", "customs", "export control", "fta", "origin", "hs", "notice", "regulation"]
        picked = [x for x in sent if any(k in x.lower() for k in keys)]
        return clean_text("; ".join(picked[:4]) or t[:600], 1200)
    df["effective_date_hint"] = [hint_effective_safe(x) for x in bodies]
    df["change_detail_hint"] = [hint_change_safe(x) for x in bodies]
    df["hs_hint"] = [hint_hs_safe(x) for x in bodies]
    df["tariff_rate_hint"] = [hint_rate_safe(x) for x in bodies]
    return df


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log("FATAL ERROR")
        log(str(e))
        traceback.print_exc()
        raise
