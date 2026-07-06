# GTI FINAL CORE v6 - Google News FAST with deferred Selenium original URL recovery
# =========================================================
# GTI STEP2-2 - GOOGLE NEWS RAW + ORIGINAL URL PARALLEL FINAL v3.4
# 紐⑹쟻: 鍮좊Ⅸ ?섏쭛 + Google News ?먮Ц URL 蹂묐젹 蹂듦뎄 + ?ㅽ뻾?쒓컙 濡쒓렇
# ?먯튃: url 而щ읆? ?먮Ц URL ?곗꽑, google_url 而щ읆? Google News ?먮낯 URL 蹂댁〈
# =========================================================

import os
import re
import json
import time
import pandas as pd
import feedparser
import requests
from datetime import datetime, timedelta
from urllib.parse import quote, unquote, urlparse, parse_qs
from bs4 import BeautifulSoup

print("GTI STEP2-2 GOOGLE NEWS-ONLY + ORIGINAL URL PARALLEL START v3.4")

# =============================
# PATH / CONFIG
# =============================
BASE_PATH = os.getenv("GTI_BASE_PATH", "C:\\temp\\")
if not BASE_PATH.endswith(("\\", "/")):
    BASE_PATH += "\\"
KEYWORD_FILE = os.path.join(BASE_PATH, "keyword.xlsx")
RAW_FILE = os.path.join(BASE_PATH, "2-2.google_news_raw.xlsx")
URL_CACHE_FILE = os.path.join(BASE_PATH, "google_news_url_cache.csv")

LOOKBACK_HOURS = int(os.getenv("GTI_LOOKBACK_HOURS", "72"))
SLEEP_SEC = 0.05

# Original URL resolve option.
# Y: resolve Google News RSS URLs to publisher/source URLs.
# N: keep Google News URLs for faster collection.
ENABLE_ORIGINAL_URL_RESOLVE = os.getenv("GTI_STEP2_RESOLVE_ORIGINAL_URL", "N").strip().upper() != "N"
URL_RESOLVE_WORKERS = int(os.getenv("GTI_STEP2_URL_WORKERS", "5"))
URL_RESOLVE_TIMEOUT = int(os.getenv("GTI_STEP2_URL_TIMEOUT", "20"))
URL_RESOLVE_RETRY = int(os.getenv("GTI_STEP2_URL_RETRY", "2"))
URL_RESOLVE_INTERVAL = float(os.getenv("GTI_STEP2_URL_INTERVAL", "0.25"))
# v6: Step2 is a broad collection step. Do not spend hours resolving every Google URL.
# Keep GoogleURL and resolve final candidates later in STEP3/STEP4 via Selenium.
URL_RESOLVE_LIMIT = int(os.getenv("GTI_STEP2_URL_RESOLVE_LIMIT", "0"))  # 0 = no limit if enabled


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}



def safe_to_excel(df, output_path, index=False):
    """Excel ????덉쟾 ?⑥닔.
    - 湲곗〈 xlsx媛 Excel?먯꽌 ?대젮 ?덉뼱 PermissionError媛 ?섎㈃ timestamp 諛깆뾽 ?뚯씪濡????    - Windows ?뚯씪 ?좉툑 ?뚮Ц???꾩껜 ?섏쭛 ?깃났 ??留덉?留???μ뿉???ㅽ뙣?섎뒗 臾몄젣 諛⑹?
    """
    try:
        df.to_excel(output_path, index=index)
        return output_path, "OK"
    except PermissionError:
        base, ext = os.path.splitext(output_path)
        alt_path = f"{base}_{datetime.now():%Y%m%d_%H%M%S}{ext}"
        df.to_excel(alt_path, index=index)
        return alt_path, "PERMISSION_LOCKED_SAVED_AS_ALT"
    except OSError as e:
        # Excel/OneDrive/諛깆떊 ?좉툑 ??Windows ????ㅻ쪟 ?鍮?        base, ext = os.path.splitext(output_path)
        alt_path = f"{base}_{datetime.now():%Y%m%d_%H%M%S}{ext}"
        try:
            df.to_excel(alt_path, index=index)
            return alt_path, f"OSERROR_SAVED_AS_ALT:{type(e).__name__}"
        except Exception:
            raise

FINAL_COLS = [
    "date",
    "title",
    "headline",
    "url",
    "google_url",
    "source",
    "summary",
    "collected_at",
    "keyword",
    "language",
    "publisher",
    "category",
    "importance",
    "importance_score",
    "score_reason",
    "url_decode_status",
    "original_url_candidate",
    "rss_url",
]

# =============================
# UTILS
# =============================

def clean_html(text):
    if pd.isna(text):
        return ""
    soup = BeautifulSoup(str(text), "html.parser")
    return re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).strip()


def normalize_text(text):
    return re.sub(r"\s+", " ", clean_html(text)).lower().strip()


def contains_any(text, terms):
    t = normalize_text(text)
    return any(str(term).lower() in t for term in terms if str(term).strip())


def keyword_equals_any(keyword, terms):
    k = normalize_text(keyword)
    return any(k == normalize_text(term) for term in terms)


def parse_datetime(entry):
    for key in ["published_parsed", "updated_parsed"]:
        try:
            v = getattr(entry, key, None)
            if v:
                return datetime(*v[:6])
        except Exception:
            pass
    return datetime.now()


def is_recent(dt):
    return dt >= datetime.now() - timedelta(hours=LOOKBACK_HOURS)


def normalize_title(title):
    title = clean_html(title).lower()

    # Google News title usually: "Article title - Publisher"
    # dedup 紐⑹쟻????publisher ?쒓굅
    if " - " in title:
        title = title.rsplit(" - ", 1)[0]

    title = re.sub(r"[^0-9a-z媛-?ｄ?-榕γ걖-?붵궊-?담꺖\s]", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


def normalize_title(title):
    title = clean_html(title).lower()
    if " - " in title:
        title = title.rsplit(" - ", 1)[0]
    title = re.sub(r"[^\w\s]", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


def get_google_locale(language):
    lang = str(language).upper().strip()
    locale_map = {
        "EN": {"hl": "en", "gl": "US", "ceid": "US:en"},
        "KR": {"hl": "ko", "gl": "KR", "ceid": "KR:ko"},
        "CN": {"hl": "zh-CN", "gl": "CN", "ceid": "CN:zh-Hans"},
        "ES": {"hl": "es", "gl": "ES", "ceid": "ES:es"},
        "PT": {"hl": "pt", "gl": "BR", "ceid": "BR:pt-419"},
        "TR": {"hl": "tr", "gl": "TR", "ceid": "TR:tr"},
        "VI": {"hl": "vi", "gl": "VN", "ceid": "VN:vi"},
        "HI": {"hl": "hi", "gl": "IN", "ceid": "IN:hi"},
    }
    return locale_map.get(lang, locale_map["EN"])


def importance_to_score(v):
    s = str(v).strip().upper()
    if s in ["100", "HIGH", "H", "A"]:
        return 100
    if s in ["70", "80", "MEDIUM", "M", "B"]:
        return 70
    if s in ["50", "LOW", "L", "C"]:
        return 50
    try:
        return int(float(s))
    except Exception:
        return 50


def extract_publisher(entry, title):
    try:
        src = entry.get("source", {})
        if isinstance(src, dict):
            src_title = src.get("title", "")
            if src_title:
                return clean_html(src_title)
    except Exception:
        pass

    try:
        if " - " in str(title):
            return str(title).rsplit(" - ", 1)[-1].strip()
    except Exception:
        pass

    return ""


# =============================
# SCORE / URL HINT
# =============================

STRONG_KEEP_TERMS = [
    "tariff", "tariffs", "customs duty", "customs duties", "customs clearance",
    "forced labor", "uflpa", "section 301", "section 232", "export control",
    "export controls", "entity list", "denied persons", "anti-dumping",
    "antidumping", "countervailing", "countervailing duty", "ad/cvd",
    "관세", "통관", "수출통제", "수출 통제", "제재", "강제노동",
    "반덤핑", "상계관세", "무역구제", "세이프가드",
]

WEAK_SINGLE_KEYWORDS = [
    "epa", "sta", "수입", "customs",
]

WEAK_SINGLE_SUPPORT_CONTEXT = [
    "tariff", "customs duty", "trade agreement", "economic partnership agreement",
    "fta", "origin", "rules of origin", "export control", "forced labor",
    "section 301", "section 232", "ad/cvd", "anti-dumping", "countervailing",
    "관세", "통관", "원산지", "자유무역협정", "경제동반자협정", "수출통제",
    "반덤핑", "상계관세", "무역구제",
]

TRADE_REMEDY_TERMS = [
    "anti-dumping", "antidumping", "countervailing", "countervailing duty",
    "ad/cvd", "trade remedy", "safeguard", "반덤핑", "상계관세",
    "무역구제", "세이프가드",
]

GENERIC_IMPORT_NOISE_TERMS = [
    "import car", "imported car", "import beer", "imported beer", "import food",
    "import price", "import prices", "luxury import", "수입차", "수입맥주",
    "수입식품", "수입물가", "수입 가격", "병행수입", "수입 브랜드",
]


def adjust_importance_score(keyword, title, summary, base_score):
    text = f"{title} {summary}"
    score = int(base_score)
    reasons = []

    strong = contains_any(text, STRONG_KEEP_TERMS)
    trade_remedy = contains_any(keyword, TRADE_REMEDY_TERMS) or contains_any(text, TRADE_REMEDY_TERMS)
    weak_single = keyword_equals_any(keyword, WEAK_SINGLE_KEYWORDS)
    weak_supported = contains_any(text, WEAK_SINGLE_SUPPORT_CONTEXT)

    if strong:
        score += 25
        reasons.append("strong_trade_policy_context")

    if trade_remedy:
        score += 35
        reasons.append("trade_remedy_forced_high")

    if weak_single and not weak_supported:
        score -= 35
        reasons.append("weak_single_keyword_penalty")

    if keyword_equals_any(keyword, ["수입"]) and contains_any(text, GENERIC_IMPORT_NOISE_TERMS) and not strong:
        score -= 35
        reasons.append("generic_import_noise_penalty")

    return max(0, min(score, 150)), ", ".join(reasons) or "base"


def extract_original_url_candidate(entry, google_url):
    """
    Google RSS URLs are often encoded article links. STEP2 keeps the Google URL,
    but this stores cheap hints so STEP3 can attempt stronger restoration first.
    """
    candidates = []

    for key in ["id", "guid", "link"]:
        value = str(entry.get(key, "") or "").strip()
        if value:
            candidates.append(value)

    try:
        for link in entry.get("links", []) or []:
            href = str(link.get("href", "") or "").strip()
            if href:
                candidates.append(href)
    except Exception:
        pass

    for value in candidates:
        parsed = urlparse(value)
        qs = parse_qs(parsed.query)
        for param in ["url", "u", "q"]:
            if param in qs and qs[param]:
                decoded = unquote(str(qs[param][0]))
                if decoded.startswith("http") and "news.google." not in decoded:
                    return decoded

    if google_url and "news.google." not in google_url:
        return google_url

    return ""


def elapsed_text(seconds):
    seconds = float(seconds)
    if seconds < 60:
        return f"{seconds:.1f}s"
    return f"{seconds/60:.1f}m"


def is_google_news_url(value):
    u = str(value or "").lower().strip()
    return "news.google.com/rss/articles/" in u or "news.google.com/articles/" in u



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


def is_bad_original_url(value):
    u = str(value or "").lower().strip()
    if not u or u in {"nan", "none", "null", "-"}:
        return True
    if not (u.startswith("http://") or u.startswith("https://")):
        return True
    bad_patterns = [
        "news.google.com/rss/articles/", "news.google.com/articles/", "news.google.com/",
        "googleusercontent.com", "gstatic.com", "ggpht.com",
        # Google News article pages contain analytics/script URLs. These are NOT article originals.
        "google-analytics.com", "googletagmanager.com", "doubleclick.net",
        "google.com/analytics", "analytics.js", "gtag/js", "googlesyndication.com",
        "googleadservices.com", "googleapis.com", "google.com/pagead",
    ]
    if any(x in u for x in bad_patterns):
        return True
    if re.search(r"\.(png|jpg|jpeg|gif|webp|svg)(\?|$)", u):
        return True
    return False


URL_CACHE = {}
URL_CACHE_LOADED = False


def safe_url(value):
    value = str(value or "").strip()
    if not value:
        return ""
    try:
        return quote(unquote(value), safe=":/?#[]@!$&'()*+,;=%")
    except Exception:
        return value.replace(" ", "%20")


def load_url_cache():
    global URL_CACHE_LOADED, URL_CACHE
    if URL_CACHE_LOADED:
        return URL_CACHE
    URL_CACHE_LOADED = True
    URL_CACHE = {}
    if not os.path.exists(URL_CACHE_FILE):
        return URL_CACHE
    try:
        cache_df = pd.read_csv(URL_CACHE_FILE, dtype=str, encoding="utf-8-sig").fillna("")
        for _, row in cache_df.iterrows():
            google_url = str(row.get("google_url", "")).strip()
            resolved_url = safe_url(row.get("resolved_url", ""))
            if google_url and resolved_url and not is_bad_original_url(resolved_url):
                URL_CACHE[google_url] = resolved_url
    except Exception as e:
        print(f"WARN url cache read failed: {URL_CACHE_FILE} / {type(e).__name__}")
    return URL_CACHE


def save_url_cache():
    if not URL_CACHE:
        return
    try:
        rows = [
            {"google_url": k, "resolved_url": v, "updated_at": datetime.now().replace(microsecond=0)}
            for k, v in sorted(URL_CACHE.items())
            if k and v and not is_bad_original_url(v)
        ]
        pd.DataFrame(rows).to_csv(URL_CACHE_FILE, index=False, encoding="utf-8-sig")
    except Exception as e:
        print(f"WARN url cache save failed: {URL_CACHE_FILE} / {type(e).__name__}")


def _google_news_article_id(value):
    try:
        parsed = urlparse(str(value or ""))
        parts = [p for p in parsed.path.split("/") if p]
        return parts[-1] if parts else ""
    except Exception:
        return ""


def _extract_article_url_from_google_text(text):
    if not text:
        return ""
    variants = [text, text.replace("\\/", "/")]

    patterns = [
        r"data-n-au=[\"'](https?://[^\"']+)[\"']",
        r"data-url=[\"'](https?://[^\"']+)[\"']",
        r"href=[\"'](https?://[^\"']+)[\"']",
        r"url=(https?%3A%2F%2F[^&\"'<>]+)",
        r"(https?:\\/\\/[^\"'<>\\]+)",
        r"(https?://[^\"'<>\s]+)",
    ]
    for t in variants:
        for pat in patterns:
            for m in re.finditer(pat, t, flags=re.I):
                cand = unquote(m.group(1).replace("\\/", "/")).rstrip(".,;?)\"")
                if cand and not is_bad_original_url(cand):
                    return cand
    return ""


def _extract_google_decode_params(page_text):
    sg = ""
    ts = ""
    m = re.search(r'data-n-a-sg=["\']([^"\']+)["\']', page_text or "")
    if m:
        sg = m.group(1)
    m = re.search(r'data-n-a-ts=["\']([^"\']+)["\']', page_text or "")
    if m:
        ts = m.group(1)
    return sg, ts


def _decode_google_news_batchexecute(article_id, page_text):
    if not article_id:
        return ""
    sg, ts = _extract_google_decode_params(page_text)
    if not sg or not ts:
        return ""
    try:
        req_obj = (
            f'["garturlreq",[["X","X",["X","X"],null,null,1,1,"US:en",null,1,'
            f'null,null,null,null,null,0,1],"X","X",1,[1,1,1],1,1,null,0,0,null,0],'
            f'"{article_id}",{int(ts)},"{sg}"]'
        )
        payload = [["Fbv4je", req_obj]]
        body = "f.req=" + quote(json.dumps([payload], separators=(",", ":")))
        resp = requests.post(
            "https://news.google.com/_/DotsSplashUi/data/batchexecute",
            data=body,
            headers={**HEADERS, "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"},
            timeout=URL_RESOLVE_TIMEOUT,
        )
        if resp.status_code == 429:
            return ""
        if resp.status_code != 200:
            return ""
        text = resp.text or ""
        try:
            parsed = json.loads(text.split("\n\n", 1)[1])[:-2]
            decoded = json.loads(parsed[0][2])[1]
            decoded = safe_url(decoded)
            if decoded and not is_bad_original_url(decoded):
                return decoded
        except Exception:
            return _extract_article_url_from_google_text(text)
    except Exception:
        return ""
    return ""


def resolve_google_news_original_url(google_url):
    """Google News URL 1嫄댁쓣 ?먮Ц URL濡?蹂듦뎄?쒕떎.

    ?곗꽑?쒖쐞:
    1) requests redirect 理쒖쥌 URL
    2) Google News article page HTML ?덉쓽 ?먮Ц URL
    3) batchexecute 諛⑹떇 decode
    """
    u = str(google_url or "").strip()
    if not u:
        return "", "EMPTY_URL"
    if not is_google_news_url(u):
        u = safe_url(u)
        if not is_bad_original_url(u):
            return u, "OK_ALREADY_ORIGINAL"
        return "", "BAD_NON_GOOGLE_URL"

    cache = load_url_cache()
    if u in cache and not is_bad_original_url(cache[u]):
        return cache[u], "OK_CACHE"

    last_error = ""
    for attempt in range(URL_RESOLVE_RETRY + 1):
        try:
            if attempt:
                time.sleep(URL_RESOLVE_INTERVAL * (attempt + 1))
            resp = requests.get(u, headers=HEADERS, allow_redirects=True, timeout=URL_RESOLVE_TIMEOUT)
            if resp.status_code == 429:
                last_error = "GOOGLE_429_RATE_LIMIT"
                time.sleep(max(1.0, URL_RESOLVE_INTERVAL * 4))
                continue
            final_url = str(resp.url or "").strip()
            if final_url and not is_bad_original_url(final_url):
                final_url = safe_url(final_url)
                URL_CACHE[u] = final_url
                save_url_cache()
                return final_url, "OK_REDIRECT"

            page_text = (resp.text or "")[:500000]
            found = _extract_article_url_from_google_text(page_text)
            if found and not is_bad_original_url(found):
                found = safe_url(found)
                URL_CACHE[u] = found
                save_url_cache()
                return found, "OK_HTML"

            found = _decode_google_news_batchexecute(_google_news_article_id(u), page_text)
            if found and not is_bad_original_url(found):
                found = safe_url(found)
                URL_CACHE[u] = found
                save_url_cache()
                return found, "OK_BATCHEXECUTE"

            last_error = "STILL_GOOGLE_OR_NO_URL"
        except Exception as e:
            last_error = f"ERROR_{type(e).__name__}"
            time.sleep(URL_RESOLVE_INTERVAL)

    # Final browser-grade fallback: if a human click opens the publisher URL, Selenium should recover it.
    selenium_url, selenium_status = resolve_google_url_by_selenium(u, timeout=URL_RESOLVE_TIMEOUT)
    if selenium_url and not is_bad_original_url(selenium_url):
        selenium_url = safe_url(selenium_url)
        URL_CACHE[u] = selenium_url
        save_url_cache()
        return selenium_url, selenium_status

    return "", selenium_status if selenium_status not in {"GOOGLE_REMAINED", "NOT_GOOGLE_URL"} else (last_error or "FAILED")


def resolve_original_urls_parallel(df):
    if df.empty:
        return df
    if not ENABLE_ORIGINAL_URL_RESOLVE:
        df["url_decode_status"] = "DEFERRED_TO_STEP3"
        return df

    started = time.perf_counter()
    load_url_cache()

    # ?대? cheap hint濡??먮Ц ?꾨낫媛 ?덈뒗 嫄댁? ?곗꽑 ?ъ슜
    df["original_url_candidate"] = df.get("original_url_candidate", "").fillna("").astype(str).str.strip()
    has_hint = df["original_url_candidate"].apply(lambda x: bool(x) and not is_bad_original_url(x))
    df.loc[has_hint, "url"] = df.loc[has_hint, "original_url_candidate"]
    df.loc[has_hint, "url_decode_status"] = "OK_HINT"

    need_mask = ~has_hint & df["google_url"].astype(str).apply(is_google_news_url)
    urls = df.loc[need_mask, "google_url"].dropna().astype(str).str.strip().unique().tolist()
    if URL_RESOLVE_LIMIT > 0 and len(urls) > URL_RESOLVE_LIMIT:
        print(f"URL RESOLVE LIMIT: {len(urls)} -> {URL_RESOLVE_LIMIT}; remaining URLs deferred to STEP3")
        deferred_urls = set(urls[URL_RESOLVE_LIMIT:])
        urls = urls[:URL_RESOLVE_LIMIT]
        df.loc[df["google_url"].isin(deferred_urls), "url_decode_status"] = "DEFERRED_TO_STEP3_LIMIT"
    else:
        deferred_urls = set()

    print(f"URL RESOLVE START: target={len(urls)} / mode=sequential_cache_first / timeout={URL_RESOLVE_TIMEOUT}s")
    if not urls:
        print("?뵕 URL RESOLVE SKIP: no Google News URL target")
        return df

    results = {}
    done = 0
    ok = 0
    for url in urls:
        try:
            original_url, status = resolve_google_news_original_url(url)
        except Exception as e:
            original_url, status = "", f"ERROR_{type(e).__name__}"
        results[url] = (original_url, status)
        done += 1
        if original_url:
            ok += 1
        if status == "GOOGLE_429_RATE_LIMIT":
            print("   - URL RESOLVE paused: Google rate limit detected, remaining unresolved URLs will be marked")
            for rest_url in urls[done:]:
                results[rest_url] = ("", "GOOGLE_429_RATE_LIMIT")
            break
        if done % 25 == 0 or done == len(urls):
            print(f"   - URL RESOLVE progress: {done}/{len(urls)} / success={ok}")
        time.sleep(URL_RESOLVE_INTERVAL)

    for idx, row in df.loc[need_mask].iterrows():
        google_url = str(row.get("google_url", "")).strip()
        if google_url in deferred_urls:
            df.at[idx, "url"] = google_url
            df.at[idx, "url_decode_status"] = "DEFERRED_TO_STEP3_LIMIT"
            continue
        original_url, status = results.get(google_url, ("", "NOT_TRIED"))
        if original_url and not is_bad_original_url(original_url):
            df.at[idx, "original_url_candidate"] = original_url
            df.at[idx, "url"] = original_url
            df.at[idx, "url_decode_status"] = status
        else:
            # Failed Google URL remains in google_url and will be retried in STEP3.
            df.at[idx, "url"] = google_url
            df.at[idx, "url_decode_status"] = status or "FAILED"

    for col in ["url", "google_url", "original_url_candidate"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).apply(safe_url)

    save_url_cache()
    elapsed = time.perf_counter() - started
    total = len(urls)
    success_rate = (ok / total * 100) if total else 0
    print(f"?뵕 URL RESOLVE DONE: target={total}, success={ok}, fail={total-ok}, success_rate={success_rate:.1f}%, elapsed={elapsed_text(elapsed)}")
    return df

# =============================
# KEYWORD LOAD
# =============================

def load_keywords():
    keywords = pd.read_excel(KEYWORD_FILE)
    keywords.columns = [str(c).strip().lower() for c in keywords.columns]

    required_cols = ["keyword", "language", "category", "importance", "active"]
    for col in required_cols:
        if col not in keywords.columns:
            raise Exception(f"??KEYWORD ?뚯씪 ?꾩닔 而щ읆 ?놁쓬: {col}")

    keywords = keywords[keywords["active"].astype(str).str.upper().str.strip() == "Y"]
    keywords = keywords.dropna(subset=["keyword"])
    keywords["keyword"] = keywords["keyword"].astype(str).str.strip()
    keywords = keywords[keywords["keyword"] != ""].copy()

    return keywords

# =============================
# COLLECT
# =============================

def collect_google_rss(keywords):
    rows = []

    for _, row in keywords.iterrows():
        kw = str(row.get("keyword", "")).strip()
        lang = str(row.get("language", "EN")).strip().upper()
        category = str(row.get("category", "")).strip()
        importance = row.get("importance", "")
        importance_score = importance_to_score(importance)

        locale = get_google_locale(lang)
        query = quote(kw)

        rss_url = (
            "https://news.google.com/rss/search?"
            f"q={query}"
            f"&hl={locale['hl']}"
            f"&gl={locale['gl']}"
            f"&ceid={locale['ceid']}"
        )

        feed = feedparser.parse(rss_url)

        for entry in feed.entries:
            dt = parse_datetime(entry)
            if not is_recent(dt):
                continue

            title = clean_html(entry.get("title", ""))
            if not title:
                continue

            google_url = entry.get("link", "")
            summary = clean_html(entry.get("summary", ""))
            publisher = extract_publisher(entry, title)
            adjusted_score, score_reason = adjust_importance_score(
                kw, title, summary, importance_score
            )
            original_url_candidate = extract_original_url_candidate(entry, google_url)

            # ?섏쭛 吏곹썑?먮뒗 Google URL ??? dedup ??蹂묐젹 ?먮Ц URL 蹂듦뎄 ?④퀎?먯꽌 url/original_url_candidate 媛깆떊
            rows.append({
                "date": dt,
                "title": title,
                "headline": title,
                "url": google_url,
                "google_url": google_url,
                "source": "Google News RSS",
                "summary": summary,
                "collected_at": datetime.now().replace(microsecond=0),
                "keyword": kw,
                "language": lang,
                "publisher": publisher,
                "category": category,
                "importance": importance,
                "importance_score": adjusted_score,
                "score_reason": score_reason,
                "url_decode_status": "PENDING_STEP2_RESOLVE" if ENABLE_ORIGINAL_URL_RESOLVE else "DEFERRED_TO_STEP3",
                "original_url_candidate": original_url_candidate,
                "rss_url": rss_url,
            })

        time.sleep(SLEEP_SEC)

    return pd.DataFrame(rows)

# =============================
# DEDUP
# =============================

def dedup_fast(df):
    before = len(df)

    df["google_url_key"] = df["google_url"].astype(str).str.strip().str.lower()
    df["title_key"] = df["title"].apply(normalize_title)

    df = df.sort_values(["importance_score", "date"], ascending=[False, False])

    # 1李? Google URL 湲곗? ?뺥솗 以묐났 ?쒓굅
    df = df.drop_duplicates(subset=["google_url_key"], keep="first")

    # 2李? title_key 湲곗? 以묐났 ?쒓굅
    # ?숈씪 湲곗궗媛 keyword留??ㅻⅤ寃??щ윭 踰??≫엳??寃쎌슦 ?쒓굅
    before_title = len(df)
    df = df.drop_duplicates(subset=["title_key"], keep="first")

    print(f"?뱤 DEDUP GOOGLE_URL/TITLE: {before} -> {len(df)}")
    print(f"   - title dedup effect: {before_title} -> {len(df)}")

    df = df.drop(columns=["google_url_key", "title_key"], errors="ignore")
    return df

# =============================
# MAIN
# =============================

def main():
    total_start = time.perf_counter()

    t0 = time.perf_counter()
    keywords = load_keywords()
    print(f"?뵊 active keywords: {len(keywords)}")
    print(f"??keyword load elapsed: {elapsed_text(time.perf_counter() - t0)}")

    t0 = time.perf_counter()
    df = collect_google_rss(keywords)
    print(f"??rss collect elapsed: {elapsed_text(time.perf_counter() - t0)}")

    if df.empty:
        print("??No data collected")
        saved_path, save_status = safe_to_excel(pd.DataFrame(columns=FINAL_COLS), RAW_FILE, index=False)
        print(f"?뮶 saved empty file: {saved_path} / status={save_status}")
        print(f"??total elapsed: {elapsed_text(time.perf_counter() - total_start)}")
        return

    print(f"?뱤 TOTAL RAW: {len(df)}")

    before = len(df)
    df = df[df["date"].apply(is_recent)].copy()
    print(f"?뱤 24h FILTER: {before} -> {len(df)}")

    t0 = time.perf_counter()
    df = dedup_fast(df)
    print(f"??dedup elapsed: {elapsed_text(time.perf_counter() - t0)}")

    t0 = time.perf_counter()
    df = resolve_original_urls_parallel(df)
    print(f"??original url resolve stage elapsed: {elapsed_text(time.perf_counter() - t0)}")

    df = df.sort_values(["importance_score", "date"], ascending=[False, False])
    df = df[FINAL_COLS]

    t0 = time.perf_counter()
    saved_path, save_status = safe_to_excel(df, RAW_FILE, index=False)
    print(f"??excel save elapsed: {elapsed_text(time.perf_counter() - t0)}")

    ok_status_count = df["url_decode_status"].astype(str).str.startswith("OK").sum() if "url_decode_status" in df.columns else 0
    ok_real_count = df["url"].apply(lambda x: bool(str(x).strip()) and not is_bad_original_url(x)).sum()
    bad_real_count = len(df) - int(ok_real_count)
    print("?뮶 saved:", RAW_FILE)
    print(f"??FINAL SAVE ROWS: {len(df)}")
    print(f"??ORIGINAL URL STATUS OK: {ok_status_count}/{len(df)} ({(ok_status_count/len(df)*100 if len(df) else 0):.1f}%)")
    print(f"??ORIGINAL URL REAL OK: {ok_real_count}/{len(df)} ({(ok_real_count/len(df)*100 if len(df) else 0):.1f}%)")
    if bad_real_count:
        print(f"?좑툘 ORIGINAL URL BAD/GOOGLE/FALLBACK: {bad_real_count}")
    print(f"??TOTAL STEP2-2 elapsed: {elapsed_text(time.perf_counter() - total_start)}")
    print("??STEP2-2 GOOGLE NEWS + ORIGINAL URL PARALLEL DONE v3.4")


if __name__ == "__main__":
    main()
