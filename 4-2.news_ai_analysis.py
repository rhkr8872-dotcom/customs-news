# -*- coding: utf-8 -*-
# GTI FINAL CORE v5 - Gemini news analysis, title keyword strict
"""
GTI STEP4-2 NEWS-ONLY AI ANALYSIS - GUARDRAIL v4.1

Purpose
- Read ONLY C:\\Temp\\3-2.news_summary.xlsx.
- Prevent stale/irrelevant items from entering executive mail.
- Do NOT force Top30: select only rows passing quality gates.
- Preserve reliable links via BestLinkURL / GoogleURL.
- Also write legacy C:\\Temp\\4.news_ai_analysis.xlsx so Step5 cannot reuse an old stale file.

Hard gates
- reject old news older than GTI_STEP4_NEWS_MAX_AGE_HOURS (default 72h)
- reject invalid URLs including fonts.googleapis / analytics / ad URLs
- reject webinar/seminar/tender/training/event notices
- reject weak Samsung relevance unless a very strong official trade-control issue
"""
from __future__ import annotations

import os
import re
import json
import ssl
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote, unquote, urlparse

import pandas as pd

BASE_DIR = Path(os.getenv("GTI_BASE_DIR", r"C:\Temp"))
INPUT_ARTICLE_FILE = BASE_DIR / "3-2.news_article_summary.xlsx"
INPUT_SUMMARY_FILE = BASE_DIR / "3-2.news_summary.xlsx"
INPUT_FILE = Path(os.getenv("GTI_STEP4_NEWS_INPUT", str(INPUT_ARTICLE_FILE)))
OUT_SUMMARY = BASE_DIR / "4-2.news_ai_summary.xlsx"
OUT_CUMULATIVE = BASE_DIR / "4-2.news_ai_cumulative.xlsx"
OUT_AUDIT = BASE_DIR / "4-2.news_ai_audit_candidates.xlsx"
OUT_EXCLUDED = BASE_DIR / "4-2.news_ai_excluded.xlsx"
OUT_LEGACY = BASE_DIR / "4.news_ai_analysis.xlsx"
GOOGLE_RESOLVE_CACHE_FILE = BASE_DIR / "google_news_url_cache.csv"

MAX_AGE_HOURS = int(os.getenv("GTI_STEP4_NEWS_MAX_AGE_HOURS", "72"))
TOP_N_MAX = int(os.getenv("GTI_STEP4_TOP_N_MAX", "50"))
NEWS_TARGET_MIN = int(os.getenv("GTI_STEP4_NEWS_TARGET_MIN", "30"))
NEWS_TARGET_MAX = int(os.getenv("GTI_STEP4_NEWS_TARGET_MAX", str(TOP_N_MAX)))
MIN_SELECT_SCORE = int(os.getenv("GTI_STEP4_MIN_SELECT_SCORE", "75"))
POLICY_WATCH_MIN_SCORE = int(os.getenv("GTI_STEP4_POLICY_WATCH_MIN_SCORE", "75"))
NEWS_EXPAND_MIN_SCORE = int(os.getenv("GTI_STEP4_NEWS_EXPAND_MIN_SCORE", "55"))
GOOGLE_RESOLVE_ENABLED = os.getenv("GTI_GOOGLE_NEWS_RESOLVE", "1").strip().upper() not in {"0", "N", "NO", "FALSE"}
GOOGLE_RESOLVE_TIMEOUT = int(os.getenv("GTI_GOOGLE_NEWS_RESOLVE_TIMEOUT", "25"))
GOOGLE_RESOLVE_INTERVAL = float(os.getenv("GTI_GOOGLE_NEWS_RESOLVE_INTERVAL", "0.3"))

BAD_URL_PATTERNS = [
    "google-analytics.com", "googletagmanager.com", "doubleclick.net", "analytics.js", "gtag/js",
    "googlesyndication.com", "googleadservices.com", "google.com/pagead", "fonts.googleapis.com",
    "fonts.gstatic.com", "googleusercontent.com", "static.xx.fbcdn", "pixel", "beacon",
]

GOOGLE_RESOLVE_CACHE: dict[str, str] = {}
GOOGLE_RESOLVE_CACHE_LOADED = False

EVENT_NOISE_TERMS = [
    "webinar", "seminar", "conference", "summit", "workshop", "training", "education", "lecture",
    "forum", "symposium", "registration", "tender", "call for tender", "rfp", "expo", 
    "opening ceremony", "ceremony", "award", "recruit", "invitation", "apply now", "join the upcoming",
    "웨비나", "세미나", "컨퍼런스", "서밋", "워크숍", "교육", "강의", "설명회", "간담회", "포럼",
    "입찰", "공모", "행사", "박람회", "전시회", "수상", "시상", "모집", "참가신청", 
    "cms summit", "aeo vs seo", "seo",
]

LOW_VALUE_TERMS = [
    "수입차", "중국차", "자동차 시장", "건강기능식품", "고등어", "오징어", "냉동", "맛집", "여행",
    "관광", "스포츠", "야구", "축구", "주가", "부동산", "아파트", "범죄", "마약", "밀수범",
    "politics", "election", "war", "ceasefire", "opinion", "editorial", "celebrity",
    "정치", "선거", "전쟁", "휴전", "외교", "사설", "칼럼", "기자회견",
]

GENERAL_ECONOMY_TERMS = [
    "gdp", "growth", "economy", "economic", "investment", "market", "business", "trade volume",
    "경제", "성장률", "투자", "시장", "산업동향", "무역동향", "수출 증가", "수출 감소", "수출 호조", "환율", "원화", "원달러", "원ㆍ달러", "환율", "외환", "투기적", "금융시장", "금리", "f4",
]

FINANCIAL_INDUSTRY_NOISE_TERMS = [
    "stock", "stocks", "share price", "shares", "futures", "perpetual futures", "crypto", "coin",
    "bitcoin", "listing", "listed", "earnings", "profit", "sales outlook", "market outlook",
    "beneficiary", "rally", "주가", "증시", "선물", "무기한 선물", "코인", "상장", "실적",
    "영업이익", "매출", "수혜", "호황", "장비시장", "시장동향", "전망", "랠리",
]

REAL_EVENT_NOISE_TERMS = [
    "webinar", "seminar", "workshop", "training", "conference registration",
    "call for tender", "tender", "rfp", "recruit", "recruitment",
    "채용", "공무직", "합격자", "면접전형", "입찰", "설명회", "교육", "세미나", "웨비나",
]

SAMSUNG_GENERAL_NOISE_TERMS = [
    "brand value", "brand ranking", "share price", "stock", "stocks", "earnings",
    "strategy meeting", "market cap", "analyst", "profit outlook", "sales outlook",
    "brand", "investment", "investor", "money", "logistics hub",
    "브랜드", "브랜드 가치", "브랜드 순위", "주가", "증시", "실적", "전략회의", "글로벌 전략회의",
    "시가총액", "주식", "큰 돈", "돈 벌", "투자", "수혜주", "전망", "칼럼",
    "고환율", "물류 거점", "기술 수출", "초음파 기술 수출",
]

CONCRETE_TRADE_POLICY_TERMS = [
    "tariff", "customs duty", "import duty", "quota", "section 301", "section 232",
    "anti-dumping", "anti dumping", "antidumping", "countervailing", "ad/cvd",
    "safeguard", "forced labor", "uflpa", "export control", "entity list",
    "cbam", "carbon border", "rules of origin", "hs code",
    "관세", "쿼터", "반덤핑", "상계관세", "무역구제", "강제노동", "수출통제",
    "원산지", "품목분류", "통관",
]

TRADE_POLICY_DIRECT_TERMS = [
    "tariff", "tariffs", "customs duty", "import duty", "quota", "duty-free quota",
    "section 301", "section 232", "anti-dumping", "anti dumping", "antidumping",
    "countervailing", "ad/cvd", "safeguard", "forced labor", "uflpa",
    "export control", "entity list", "cbam", "carbon border", "fta", "rules of origin",
    "hs code", "classification", "clearance", "declaration",
    "관세", "관세율", "쿼터", "무관세", "반덤핑", "상계관세", "무역구제",
    "세이프가드", "강제노동", "수출통제", "수출 통제", "전략물자", "탄소국경", "원산지",
    "품목분류", "통관", "신고",
]

SAMSUNG_EXACT_TERMS = [
    "samsung", "samsung electronics", "samsung sdi", "samsung display", "삼성", "삼성전자", "삼성sdi", "삼성디스플레이", "삼전",
]
SEMICONDUCTOR_TERMS = ["semiconductor", "chip", "chips", "hbm", "memory", "반도체", "칩", "메모리", "ai chip", "ai chips"]
MOBILE_TERMS = ["smartphone", "mobile phone", "handset", "galaxy", "스마트폰", "휴대폰", "갤럭시"]
BATTERY_TERMS = ["battery", "batteries", "ev battery", "배터리", "이차전지"]
DISPLAY_TERMS = ["display", "oled", "디스플레이"]
PRODUCT_TERMS = SEMICONDUCTOR_TERMS + MOBILE_TERMS + BATTERY_TERMS + DISPLAY_TERMS

TOPIC_RULES = [
    ("EXPORT_CONTROL", ["export control", "export controls", "entity list", "bis", "denied persons", "수출통제", "전략물자", "제재", "ai chip", "ai chips"]),
    ("AD_CVD", ["anti-dumping", "anti dumping", "antidumping", "countervailing", "ad/cvd", "cvd", "반덤핑", "상계관세", "무역구제"]),
    ("CBAM_CARBON", ["cbam", "carbon border", "carbon border adjustment", "탄소국경"]),
    ("ORIGIN_FTA", ["fta", "cepa", "usmca", "rules of origin", "origin", "원산지", "자유무역협정"]),
    ("HS_CLASSIFICATION", ["hs code", "classification", "tariff classification", "품목분류", "hs코드"]),
    ("TARIFF", ["section 301", "301조", "section 232", "232조", "reciprocal tariff", "tariff", "tariffs", "customs duty", "import duty", "관세", "관세율", "추가관세", "상호관세"]),
    ("CUSTOMS", ["customs", "clearance", "declaration", "통관", "세관", "관세청"]),
]

MUST_KEEP_POLICY_TERMS = [
    "section 301", "301조", "section 232", "232조", "reciprocal tariff",
    "tariff cap", "tariff ceiling", "tariff-rate quota", "tariff rate quota",
    "tariff quota", "duty-free quota", "duty free quota", "무관세 쿼터",
    "관세상한", "관세 쿼터", "anti-dumping", "anti dumping", "antidumping",
    "countervailing", "countervailing duty", "countervailing duties", "ad/cvd",
    "safeguard", "steel safeguard", "steel overcapacity", "steel quota",
    "steel tariff", "aluminum tariff", "forced labor", "uflpa",
    "export control", "entity list", "cbam", "carbon border",
    "반덤핑", "상계관세", "무역구제", "세이프가드", "강제노동", "수출통제",
]

MUST_KEEP_POLICY_COMBOS = [
    (["steel", "철강"], ["quota", "쿼터", "safeguard", "tariff", "관세", "무관세"]),
    (["aluminum", "알루미늄"], ["quota", "쿼터", "safeguard", "tariff", "관세", "무관세"]),
    (["battery", "배터리"], ["tariff", "관세", "301", "section 301"]),
    (["semiconductor", "chip", "반도체", "칩"], ["export control", "entity list", "tariff", "관세", "수출통제"]),
]

TOPIC_KR = {
    "EXPORT_CONTROL": "수출통제",
    "AD_CVD": "반덤핑/상계관세",
    "CBAM_CARBON": "CBAM",
    "ORIGIN_FTA": "FTA/원산지",
    "HS_CLASSIFICATION": "HS/품목분류",
    "TARIFF": "관세정책",
    "CUSTOMS": "통관/세관",
    "TRADE_GENERAL": "무역일반",
}

OUTPUT_COLS = [
    "rank", "Date", "Headline", "URL", "GoogleURL", "OriginalURLCandidate", "BestLinkURL", "URL_Quality",
    "Country", "Agency", "Publisher", "priority_group", "mail_section", "selected", "Risk", "final_score",
    "topic", "topic_score", "samsung_impact", "samsung_impact_score", "subsidiary_score", "action_score", "urgency_score",
    "topic_keyword", "topic_reason", "issue_type", "cluster_key", "RegulationRelated", "RegulationTransferType",
    "affected_subsidiary", "affected_subsidiaries", "affected_products", "subsidiary_products", "subsidiary_reason",
    "impact_production_subsidiaries", "impact_sales_subsidiaries", "impact_products", "fta_impact", "export_control_impact",
    "hs_impact", "tariff_impact", "RequiredAction", "ActionOwner", "ExecutiveMessage", "samsung_score", "samsung_reason",
    "Summary", "AI Analysis", "Action Plan", "KeywordMatches", "SelectReason", "RejectReason", "Source", "SourceFile",
    "original_url", "article_body", "ClusterHeadlines", "article_extract_status", "article_source_type",
    "effective_date_hint", "change_detail_hint", "hs_hint", "tariff_rate_hint", "last_checked",
]

LEGACY_COLS = [
    "No", "Content Type", "Mail Group", "Samsung Impact", "Affected Subsidiary", "Impact Reason", "Date", "Headline",
    "Summary", "AI Analysis", "Action Plan", "Country", "Agency", "Risk", "Importance Score", "Priority Group",
    "Issue", "Cluster", "URL", "Source", "Source File",
]



# ======================================================================
# GTI STEP4 Gemini Original-URL Analysis Patch v5.0
# ======================================================================

GEMINI_API_KEY = (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()
GEMINI_MODEL = os.getenv("GTI_GEMINI_MODEL", "gemini-1.5-flash").strip()
USE_GEMINI = os.getenv("GTI_STEP4_USE_GEMINI", "Y").strip().upper() not in {"N", "NO", "0", "FALSE"}
ARTICLE_FETCH_TIMEOUT = int(os.getenv("GTI_ARTICLE_FETCH_TIMEOUT", "12"))
ARTICLE_MAX_CHARS = int(os.getenv("GTI_ARTICLE_MAX_CHARS", "12000"))
GEMINI_CACHE_FILE = BASE_DIR / "gti_step4_gemini_cache.xlsx"
_GEMINI_CACHE = None

def _ensure_gemini_cache():
    global _GEMINI_CACHE
    if _GEMINI_CACHE is not None:
        return _GEMINI_CACHE
    _GEMINI_CACHE = {}
    if GEMINI_CACHE_FILE.exists():
        try:
            df_cache = pd.read_excel(GEMINI_CACHE_FILE)
            for _, r in df_cache.iterrows():
                key = clean(r.get("cache_key", ""))
                if key:
                    _GEMINI_CACHE[key] = {
                        "Summary": clean(r.get("Summary", "")),
                        "AI Analysis": clean(r.get("AI Analysis", "")),
                        "Action Plan": clean(r.get("Action Plan", "")),
                        "ExecutiveMessage": clean(r.get("ExecutiveMessage", "")),
                        "article_extract_status": clean(r.get("article_extract_status", "")),
                    }
        except Exception:
            _GEMINI_CACHE = {}
    return _GEMINI_CACHE

def _save_gemini_cache():
    try:
        cache = _ensure_gemini_cache()
        if not cache:
            return
        rows = []
        for key, val in cache.items():
            row = {"cache_key": key}
            row.update(val)
            rows.append(row)
        pd.DataFrame(rows).to_excel(GEMINI_CACHE_FILE, index=False)
    except Exception:
        pass

def _analysis_cache_key(url: str, headline: str) -> str:
    u = safe_url(url)
    try:
        h = normalize_text(headline)[:120]
    except Exception:
        h = clean(headline).lower()[:120]
    return f"{u}|{h}"

def _html_unescape(text: str) -> str:
    try:
        import html as _html
        return _html.unescape(text or "")
    except Exception:
        return text or ""

def _strip_html_to_text(html_text: str) -> str:
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html_text or "")
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?is)<noscript[^>]*>.*?</noscript>", " ", text)
    text = re.sub(r"(?i)</(p|div|li|h1|h2|h3|tr|br)>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = _html_unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return "\n".join(line.strip() for line in text.splitlines() if line.strip()).strip()

def _extract_meta_description(html_text: str) -> str:
    patterns = [
        r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:description["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']description["\']',
    ]
    for pat in patterns:
        m = re.search(pat, html_text or "", re.I | re.S)
        if m:
            return _html_unescape(re.sub(r"\s+", " ", m.group(1))).strip()
    return ""

def _looks_like_title_only(text: str, title: str) -> bool:
    t = clean(text)
    h = clean(title)
    if not t:
        return True
    if h and (t == h or t.replace(" ", "") == h.replace(" ", "")):
        return True
    if h and len(t) <= len(h) + 30 and h[:25] in t:
        return True
    bad = ["관련 뉴스입니다", "공식 규제/공지 후보입니다", "본문에서 확인 불가"]
    return any(x in t for x in bad) and len(t) < 160

def fetch_article_body_for_ai(url: str) -> tuple[str, str]:
    u = safe_url(url)
    if not u:
        return "", "NO_URL"
    if u.lower().endswith(".pdf"):
        return "", "PDF_URL_BODY_NOT_EXTRACTED"
    try:
        req = urllib.request.Request(
            u,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/129 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            },
        )
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=ARTICLE_FETCH_TIMEOUT, context=ctx) as resp:
            raw = resp.read(2_000_000)
            ctype = resp.headers.get("Content-Type", "")
        charset = ""
        m = re.search(r"charset=([\w\-]+)", ctype, re.I)
        if m:
            charset = m.group(1)
        html_text = ""
        for enc in ["utf-8", charset, "cp949", "euc-kr", "latin-1"]:
            if not enc:
                continue
            try:
                html_text = raw.decode(enc, "ignore")
                break
            except Exception:
                continue
        if not html_text:
            return "", "DECODE_FAILED"
        meta = _extract_meta_description(html_text)
        body = _strip_html_to_text(html_text)
        if meta and meta not in body[:500]:
            body = meta + "\n" + body
        body = body[:ARTICLE_MAX_CHARS]
        if len(body) < 120:
            return body, "BODY_TOO_SHORT"
        return body, "FETCHED_URL_BODY"
    except Exception as exc:
        return "", f"FETCH_FAILED:{type(exc).__name__}"

def _fallback_source_body(row: pd.Series, headline: str) -> tuple[str, str]:
    for col in [
        "article_body", "regulation_fallback_body", "full_text", "FullText",
        "content", "Content", "body", "Body", "Summary", "AI Analysis",
        "ClusterHeadlines", "description", "Description",
    ]:
        val = clean(row.get(col, ""))
        if val and not _looks_like_title_only(val, headline) and len(val) >= 80:
            return val[:ARTICLE_MAX_CHARS], f"INPUT_COLUMN:{col}"
    return "", "NO_INPUT_BODY"

def _simple_body_summary(body: str, headline: str) -> str:
    if not body:
        return "본문 확인 불가: 원문 URL에서 본문을 가져오지 못했습니다. 제목만으로 요약하지 않았습니다."
    text = re.sub(r"\s+", " ", body).strip()
    parts = re.split(r"(?<=[.!?。？！])\s+|(?<=다\.)\s+|(?<=니다\.)\s+", text)
    parts = [p.strip() for p in parts if p.strip() and not _looks_like_title_only(p, headline)]
    if not parts:
        return text[:350]
    return " ".join(parts[:3])[:700]

def call_gemini_json(prompt: str) -> dict:
    if not USE_GEMINI or not GEMINI_API_KEY:
        return {}
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
            "topP": 0.8,
            "maxOutputTokens": 1200,
            "responseMimeType": "application/json",
        },
    }
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(endpoint, data=data, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=45) as resp:
            out = json.loads(resp.read().decode("utf-8", "ignore"))
        text = out["candidates"][0]["content"]["parts"][0]["text"]
        return json.loads(text)
    except Exception as exc:
        return {"_error": f"{type(exc).__name__}: {exc}"}

def build_gti_ai_analysis(row: pd.Series, *, headline: str, url: str, issue: str, impact: str, products_text: str, default_action: str, content_type: str) -> dict:
    cache = _ensure_gemini_cache()
    key = _analysis_cache_key(url, headline)
    if key in cache and clean(cache[key].get("Summary")):
        return cache[key]

    body, status = _fallback_source_body(row, headline)
    if not body:
        body, status = fetch_article_body_for_ai(url)

    prompt = f"""
당신은 삼성전자 본사 관세/통상 리스크 분석가입니다.
아래 원문을 읽고 GTI Radar 임원보고용으로 분석하십시오.

절대 금지:
- 제목 반복 금지
- "관련 뉴스입니다", "공식 규제/공지 후보입니다" 같은 템플릿 문장 금지
- 본문에 없는 세율/HS/국가/시행일을 지어내지 말 것
- 본문을 읽을 수 없으면 Summary에 "본문 확인 불가"라고 명시

출력은 JSON만:
{{
  "Summary": "원문 기준 게시물 요약 2~3줄",
  "AI Analysis": "삼성전자 관세업무 영향. 수입통관/수출통관/FTA·원산지/HS/관세비용/수출통제 중 해당 항목을 구체적으로 설명",
  "Action Plan": "즉시조치/1주 내/1개월 내/Owner 형식의 구체적 대응방안",
  "ExecutiveMessage": "임원용 한 문단 핵심 메시지"
}}

기본 정보:
- Content Type: {content_type}
- Issue: {issue}
- Samsung Impact: {impact}
- Affected Products: {products_text}
- URL: {url}
- Headline: {headline}
- Default Action Hint: {default_action}

원문:
{body[:ARTICLE_MAX_CHARS]}
""".strip()

    result = call_gemini_json(prompt)
    if not result or result.get("_error"):
        summary = _simple_body_summary(body, headline)
        if body:
            ai = (
                f"{issue} 이슈입니다. 삼성 영향도는 {impact}입니다. "
                f"관련 제품/키워드는 {products_text or '본문에서 확인 불가'}입니다. "
                "원문 본문 기반 세부 영향은 Summary 내용을 기준으로 대상 국가·품목·HS·세율·시행일을 추가 확인해야 합니다."
            )
        else:
            ai = (
                f"본문 확인 불가로 정밀 영향 분석이 제한됩니다. "
                f"다만 제목/메타 기준 {issue} 이슈이며, 삼성 영향도는 {impact}입니다."
            )
        action_plan = (
            f"즉시조치: 원문 URL 접속 가능 여부 및 본문 확보 상태를 확인하십시오. "
            f"1주 내: 대상 국가·품목·HS·세율·시행일을 검증하십시오. "
            f"Owner: {default_action}"
        )
        executive = summary[:250]
    else:
        summary = clean(result.get("Summary", ""))
        ai = clean(result.get("AI Analysis", ""))
        action_plan = clean(result.get("Action Plan", ""))
        executive = clean(result.get("ExecutiveMessage", ""))

        if _looks_like_title_only(summary, headline):
            summary = _simple_body_summary(body, headline)
        if not ai:
            ai = f"{issue} 관련 삼성전자 관세업무 영향 확인 필요. 영향도: {impact}."
        if not action_plan:
            action_plan = default_action
        if not executive:
            executive = summary[:250]

    final = {
        "Summary": summary[:900],
        "AI Analysis": ai[:1200],
        "Action Plan": action_plan[:1200],
        "ExecutiveMessage": executive[:700],
        "article_extract_status": status if body else status,
    }
    cache[key] = final
    _save_gemini_cache()
    return final

# ======================================================================
# End of GTI STEP4 Gemini Original-URL Analysis Patch v5.0
# ======================================================================

def log(msg: str) -> None:
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}")


def clean(v) -> str:
    if pd.isna(v):
        return ""
    return str(v).strip()


def contains_any(text: str, terms: list[str]) -> bool:
    t = str(text or "").lower()
    return any(term.lower() in t for term in terms)


def has_policy_combo(text: str) -> bool:
    t = str(text or "").lower()
    for left_terms, right_terms in MUST_KEEP_POLICY_COMBOS:
        if contains_any(t, left_terms) and contains_any(t, right_terms):
            return True
    return False


def is_must_keep_policy_news(text: str, topic: str) -> bool:
    if topic not in {"EXPORT_CONTROL", "AD_CVD", "CBAM_CARBON", "ORIGIN_FTA", "HS_CLASSIFICATION", "TARIFF", "CUSTOMS"}:
        return False
    return contains_any(text, MUST_KEEP_POLICY_TERMS) or has_policy_combo(text)


def has_direct_trade_policy_signal(text: str) -> bool:
    return contains_any(text, TRADE_POLICY_DIRECT_TERMS) or has_policy_combo(text)


def is_real_event_noise(text: str) -> bool:
    return contains_any(text, REAL_EVENT_NOISE_TERMS)


def is_samsung_general_noise(text: str, direct_policy_signal: bool, must_keep_policy: bool) -> bool:
    if direct_policy_signal or must_keep_policy:
        if contains_any(text, CONCRETE_TRADE_POLICY_TERMS):
            return False
    return contains_any(text, SAMSUNG_GENERAL_NOISE_TERMS)


def is_financial_or_industry_noise(text: str, direct_policy_signal: bool, must_keep_policy: bool) -> bool:
    if direct_policy_signal or must_keep_policy:
        return False
    return contains_any(text, FINANCIAL_INDUSTRY_NOISE_TERMS)


def is_bilateral_industry_noise(text: str, direct_policy_signal: bool, must_keep_policy: bool) -> bool:
    if direct_policy_signal or must_keep_policy:
        return False
    has_bilateral = contains_any(text, ["summit", " 정상회담", "정상 회담", "cooperation", "협력", "economic security", "경제 안보"])
    has_industry = contains_any(text, PRODUCT_TERMS)
    return has_bilateral and has_industry


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df.loc[:, ~pd.Index(df.columns).duplicated()]


def parse_dt(v):
    try:
        dt = pd.to_datetime(v, errors="coerce")
        if pd.isna(dt):
            return pd.NaT
        if getattr(dt, "tzinfo", None) is not None:
            dt = dt.tz_convert(None)
        return dt
    except Exception:
        return pd.NaT


def normalize_title(s: str) -> str:
    t = clean(s).lower()
    t = re.sub(r"\[[^\]]+\]|\([^\)]+\)", " ", t)
    t = re.sub(r"[^a-z0-9가-힣]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def is_generic_google_main(url: str) -> bool:
    u = clean(url).lower().rstrip("/")
    if u in {"https://news.google.com", "http://news.google.com", "https://www.google.com", "http://www.google.com"}:
        return True
    try:
        p = urlparse(u)
        if "news.google" in p.netloc and p.path in {"", "/", "/home"}:
            return True
        if "google." in p.netloc and p.path in {"", "/", "/search"}:
            return True
    except Exception:
        pass
    return False


def is_real_original_url(url: str) -> bool:
    u = safe_url(url)
    if not is_valid_link(u):
        return False
    if is_google_article_redirect(u):
        return False
    if is_generic_google_main(u):
        return False
    try:
        p = urlparse(u.lower())
        if "news.google" in p.netloc or "google." in p.netloc:
            return False
    except Exception:
        return False
    return True


def is_google_article_redirect(url: str) -> bool:
    u = clean(url).lower()
    if not u.startswith(("http://", "https://")):
        return False
    p = urlparse(u)
    return "news.google" in p.netloc and ("/rss/articles/" in p.path or "/articles/" in p.path)


def safe_url(url: str) -> str:
    u = clean(url).replace("\r", "").replace("\n", "").strip()
    if not u:
        return ""
    # Encode spaces and non-ASCII safely without damaging normal URL separators.
    return quote(unquote(u), safe=":/?#[]@!$&'()*+,;=%")


def google_news_token(url: str) -> str:
    try:
        p = urlparse(url)
        parts = [x for x in p.path.split("/") if x]
        if len(parts) >= 2 and parts[-2] in {"articles", "read"}:
            return parts[-1]
    except Exception:
        pass
    return ""


def load_google_resolve_cache() -> None:
    global GOOGLE_RESOLVE_CACHE_LOADED
    if GOOGLE_RESOLVE_CACHE_LOADED:
        return
    GOOGLE_RESOLVE_CACHE_LOADED = True
    if not GOOGLE_RESOLVE_CACHE_FILE.exists():
        return
    try:
        df = pd.read_csv(GOOGLE_RESOLVE_CACHE_FILE)
        for _, row in df.iterrows():
            google_url = safe_url(row.get("google_url", ""))
            resolved_url = safe_url(row.get("resolved_url", ""))
            if google_url and is_real_original_url(resolved_url):
                GOOGLE_RESOLVE_CACHE[google_url] = resolved_url
    except Exception:
        return


def save_google_resolve_cache() -> None:
    try:
        rows = [
            {"google_url": google_url, "resolved_url": resolved_url, "last_checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            for google_url, resolved_url in GOOGLE_RESOLVE_CACHE.items()
            if google_url and is_real_original_url(resolved_url)
        ]
        if rows:
            pd.DataFrame(rows).drop_duplicates(subset=["google_url"], keep="last").to_csv(
                GOOGLE_RESOLVE_CACHE_FILE, index=False, encoding="utf-8-sig"
            )
    except Exception:
        return


def fetch_google_decode_params(token: str) -> tuple[str, str]:
    ctx = ssl.create_default_context()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/129 Safari/537.36",
    }
    for prefix in ("https://news.google.com/articles/", "https://news.google.com/rss/articles/"):
        req = urllib.request.Request(prefix + token, headers=headers)
        with urllib.request.urlopen(req, timeout=GOOGLE_RESOLVE_TIMEOUT, context=ctx) as resp:
            html = resp.read().decode("utf-8", "ignore")
        sig = re.search(r'data-n-a-sg="([^"]+)"', html)
        ts = re.search(r'data-n-a-ts="([^"]+)"', html)
        if sig and ts:
            return sig.group(1), ts.group(1)
    return "", ""


def decode_google_news_token(token: str, signature: str, timestamp: str) -> str:
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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/129 Safari/537.36",
    }
    ctx = ssl.create_default_context()
    req = urllib.request.Request(endpoint, data=body.encode("utf-8"), headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=GOOGLE_RESOLVE_TIMEOUT, context=ctx) as resp:
        text = resp.read().decode("utf-8", "ignore")
    parsed = json.loads(text.split("\n\n", 1)[1])[:-2]
    return json.loads(parsed[0][2])[1]


def resolve_google_news_url(url: str) -> str:
    u = safe_url(url)
    if not GOOGLE_RESOLVE_ENABLED or not is_google_article_redirect(u):
        return ""
    load_google_resolve_cache()
    if u in GOOGLE_RESOLVE_CACHE:
        return GOOGLE_RESOLVE_CACHE[u]
    token = google_news_token(u)
    if not token:
        GOOGLE_RESOLVE_CACHE[u] = ""
        return ""
    for _ in range(2):
        try:
            signature, timestamp = fetch_google_decode_params(token)
            if signature and timestamp:
                resolved = safe_url(decode_google_news_token(token, signature, timestamp))
                if resolved and is_real_original_url(resolved):
                    GOOGLE_RESOLVE_CACHE[u] = resolved
                    save_google_resolve_cache()
                    if GOOGLE_RESOLVE_INTERVAL > 0:
                        time.sleep(GOOGLE_RESOLVE_INTERVAL)
                    return resolved
        except Exception:
            if GOOGLE_RESOLVE_INTERVAL > 0:
                time.sleep(GOOGLE_RESOLVE_INTERVAL)
    GOOGLE_RESOLVE_CACHE[u] = ""
    return ""


def is_valid_link(url: str) -> bool:
    u = safe_url(url)
    if not u.lower().startswith(("http://", "https://")):
        return False
    if is_generic_google_main(u):
        return False
    low = u.lower()
    return not any(x in low for x in BAD_URL_PATTERNS)


def choose_best_link(row: pd.Series, resolve_google: bool = False) -> tuple[str, str]:
    # Prefer real URL. Allow Google article redirect. Never allow script/font/ad URLs.
    for col, status in [
        ("BestLinkURL", "BEST_LINK"),
        ("OriginalURLCandidate", "ORIGINAL_CANDIDATE"),
        ("URL", "URL"),
        ("GoogleURL", "GOOGLE_ARTICLE_REDIRECT"),
        ("original_url", "ORIGINAL_URL"),
    ]:
        v = safe_url(row.get(col, ""))
        if is_valid_link(v):
            if is_google_article_redirect(v) and resolve_google:
                resolved = resolve_google_news_url(v)
                if resolved:
                    return resolved, "GOOGLE_NEWS_RESOLVED"
            if is_google_article_redirect(v):
                return v, "GOOGLE_ARTICLE_REDIRECT"
            return v, status
    return "", "EMPTY_OR_BAD_LINK"


def content_text(row: pd.Series) -> str:
    # Article-facing text only. Do NOT include Step3 metadata like SelectReason or SamsungSignal,
    # because strings such as "samsung=PRODUCTION_COUNTRY" falsely trigger Samsung relevance.
    # Source/Category can contain collector keywords such as "Google Alert - Export Control";
    # do not treat those as article body signals.
    cols = ["Headline", "Summary", "AI Analysis", "ClusterHeadlines"]
    return " ".join(clean(row.get(c, "")) for c in cols).lower()


def topic_text(row: pd.Series) -> str:
    # Topic can use keyword metadata, but Samsung relevance cannot.
    cols = ["Headline", "Summary", "AI Analysis", "KeywordMatches", "IssueKey", "ClusterHeadlines", "Agency", "Publisher", "Source", "Category"]
    return " ".join(clean(row.get(c, "")) for c in cols).lower()


def row_text(row: pd.Series) -> str:
    return topic_text(row)


def detect_products(text: str) -> list[str]:
    products = []
    if contains_any(text, SEMICONDUCTOR_TERMS): products.append("Semiconductor")
    if contains_any(text, MOBILE_TERMS): products.append("Mobile")
    if contains_any(text, BATTERY_TERMS): products.append("Battery")
    if contains_any(text, DISPLAY_TERMS): products.append("Display")
    if contains_any(text, SAMSUNG_EXACT_TERMS): products.append("Samsung mentioned")
    return sorted(set(products))


def detect_topic(row: pd.Series, text: str) -> str:
    # Re-classify from text. Do not trust previous IssueKey blindly.
    for topic, terms in TOPIC_RULES:
        if contains_any(text, terms):
            return topic
    issue = clean(row.get("IssueKey", "")).upper()
    if issue in TOPIC_KR:
        return issue
    return "TRADE_GENERAL"


def is_official_source(row: pd.Series) -> bool:
    blob = " ".join(clean(row.get(c, "")) for c in ["URL", "BestLinkURL", "Agency", "Source", "Publisher"]).lower()
    return any(x in blob for x in [".gov", "europa.eu", "ustr.gov", "cbp.gov", "bis.gov", "usitc.gov", "wto.org", "wcoomd.org", "customs", "관세청"])


def action_for_topic(topic: str) -> tuple[str, str]:
    if topic == "EXPORT_CONTROL":
        return "수출통제팀", "BIS/Entity List/ECCN/고객·거래 제한 여부를 확인하고 관련 법인에 스크리닝을 요청하십시오."
    if topic == "AD_CVD":
        return "통관운영/관세팀", "대상 HS·공급국·공급자·가격자료를 확인하고 AD/CVD 적용 가능성과 신고가격 영향을 점검하십시오."
    if topic == "CBAM_CARBON":
        return "ESG/구매/통관", "CBAM 대상 품목 및 EU 수출입 법인의 배출량·공급사 자료 제출 의무를 확인하십시오."
    if topic == "ORIGIN_FTA":
        return "FTA팀", "원산지 기준·CO 발급·수입 FTA 적용 영향 여부를 확인하고 증빙자료를 점검하십시오."
    if topic == "HS_CLASSIFICATION":
        return "HS/통관팀", "품목분류 기준 변경 여부와 주요 제품 HS Master 영향 여부를 확인하십시오."
    if topic == "TARIFF":
        return "통관운영/FTA팀", "관세율·시행일·대상국·대상품목을 확인하고 수입원가 및 가격 영향을 점검하십시오."
    return "통관운영", "업무 관련성이 있는지 확인 후 모니터링하십시오."


def score_row(row: pd.Series) -> dict:
    text = topic_text(row)
    ctext = content_text(row)
    headline = clean(row.get("Headline", ""))
    link, link_status = choose_best_link(row)
    dt = parse_dt(row.get("Date", ""))
    cdt = parse_dt(row.get("CollectedAt", ""))
    basis_dt = dt if not pd.isna(dt) else cdt
    now = pd.Timestamp(datetime.now())
    age_hours = None if pd.isna(basis_dt) else (now - basis_dt).total_seconds() / 3600

    topic = detect_topic(row, text)
    products = detect_products(ctext)
    official = is_official_source(row)
    strong_policy = topic in {"EXPORT_CONTROL", "AD_CVD", "CBAM_CARBON", "ORIGIN_FTA", "HS_CLASSIFICATION", "TARIFF"}
    must_keep_policy = is_must_keep_policy_news(ctext, topic)
    direct_policy_signal = has_direct_trade_policy_signal(ctext)
    ai_chip_control_signal = (
        topic == "EXPORT_CONTROL"
        and contains_any(ctext, ["ai chip", "ai chips", "ai칩", "ai 칩", "ai 반도체"])
        and contains_any(ctext, ["china", "chinese", "중국", "中"])
        and contains_any(ctext, ["control", "restriction", "restrict", "ban", "export", "sale", "통제", "제한", "차단", "수출", "판매"])
    )
    if ai_chip_control_signal:
        direct_policy_signal = True
    samsung_mention = contains_any(ctext, SAMSUNG_EXACT_TERMS)
    product_policy = bool([p for p in products if p != "Samsung mentioned"]) and strong_policy

    rejects = []
    reasons = []
    if not link:
        rejects.append("no_valid_url")
    if age_hours is not None and age_hours > MAX_AGE_HOURS:
        rejects.append(f"old_news>{MAX_AGE_HOURS}h")
    if age_hours is not None and age_hours < -12:
        rejects.append("future_date_abnormal")
    if is_real_event_noise(text):
        rejects.append("event_training_tender_noise")
    if contains_any(text, LOW_VALUE_TERMS) and not (must_keep_policy or (strong_policy and (samsung_mention or product_policy))):
        rejects.append("low_value_general_news")
    if contains_any(text, GENERAL_ECONOMY_TERMS) and not (must_keep_policy or (strong_policy and (samsung_mention or product_policy))):
        rejects.append("general_economy_without_samsung_policy")
    if is_financial_or_industry_noise(ctext, direct_policy_signal, must_keep_policy):
        rejects.append("financial_industry_noise_without_trade_policy")
    if is_samsung_general_noise(ctext, direct_policy_signal, must_keep_policy):
        rejects.append("samsung_general_business_noise")
    if is_bilateral_industry_noise(ctext, direct_policy_signal, must_keep_policy):
        rejects.append("bilateral_industry_news_without_trade_policy")
    if topic == "EXPORT_CONTROL" and contains_any(ctext, ["ai chip", "ai chips", "ai칩", "ai 칩"]) and not direct_policy_signal:
        rejects.append("ai_chip_industry_without_control_signal")
    if topic == "EXPORT_CONTROL" and product_policy and not direct_policy_signal:
        rejects.append("export_control_industry_without_control_signal")
    if topic == "TRADE_GENERAL":
        rejects.append("trade_general_not_selected")
    # If only metadata created a policy topic but article text lacks concrete policy terms, reject.
    if strong_policy and not contains_any(text, [term for _, terms in TOPIC_RULES for term in terms]):
        rejects.append("weak_policy_text")

    # Samsung impact: Direct only when Samsung is actually mentioned. Country alone is never Direct.
    if samsung_mention:
        impact = "Direct"
        samsung_score = 100
        reasons.append("samsung_exact_mention")
    elif product_policy:
        impact = "Indirect"
        samsung_score = 78
        reasons.append("product_policy_indirect")
    elif official and topic in {"EXPORT_CONTROL", "AD_CVD", "CBAM_CARBON", "ORIGIN_FTA", "HS_CLASSIFICATION", "TARIFF"}:
        impact = "Watch"
        samsung_score = 58
        reasons.append("official_policy_watch")
    elif must_keep_policy:
        impact = "Watch"
        samsung_score = 62
        reasons.append("policy_watch_must_keep")
    else:
        impact = "Reference"
        samsung_score = 20
        rejects.append("weak_samsung_relevance")

    topic_score = {"EXPORT_CONTROL":100, "AD_CVD":96, "CBAM_CARBON":90, "ORIGIN_FTA":88, "HS_CLASSIFICATION":86, "TARIFF":84, "CUSTOMS":65, "TRADE_GENERAL":25}.get(topic, 25)
    action_score = 90 if topic in {"EXPORT_CONTROL", "AD_CVD", "CBAM_CARBON", "TARIFF"} else 78 if topic in {"ORIGIN_FTA", "HS_CLASSIFICATION"} else 45
    urgency_score = 80 if contains_any(text, ["effective", "takes effect", "시행", "발효", "deadline", "due date", "immediate", "즉시"]) else 55
    recency_score = 100 if age_hours is not None and age_hours <= 24 else 85 if age_hours is not None and age_hours <= 48 else 70 if age_hours is not None and age_hours <= MAX_AGE_HOURS else 0
    final_score = round(topic_score*0.35 + samsung_score*0.30 + action_score*0.15 + urgency_score*0.10 + recency_score*0.10)

    if "event_training_tender_noise" in rejects:
        final_score = min(final_score, 40)
    if "old_news" in " ".join(rejects):
        final_score = min(final_score, 20)
    if "weak_samsung_relevance" in rejects:
        final_score = min(final_score, 55)
    if "financial_industry_noise_without_trade_policy" in rejects:
        final_score = min(final_score, 50)
    if "samsung_general_business_noise" in rejects:
        final_score = min(final_score, 48)
    if "bilateral_industry_news_without_trade_policy" in rejects or "ai_chip_industry_without_control_signal" in rejects or "export_control_industry_without_control_signal" in rejects:
        final_score = min(final_score, 50)

    if must_keep_policy:
        final_score = max(final_score, POLICY_WATCH_MIN_SCORE)
        rejects = [r for r in rejects if r not in {
            "weak_samsung_relevance",
            "general_economy_without_samsung_policy",
            "low_value_general_news",
            "financial_industry_noise_without_trade_policy",
            "samsung_general_business_noise",
        }]

    selected = (not rejects) and final_score >= MIN_SELECT_SCORE
    owner, action = action_for_topic(topic)
    products_text = "; ".join(products) if products else "본문에서 확인 불가"
    issue_kr = TOPIC_KR.get(topic, topic)
    risk = "상" if final_score >= 85 else "중" if final_score >= MIN_SELECT_SCORE else "하"
    analysis = build_gti_ai_analysis(
        row,
        headline=headline,
        url=link,
        issue=issue_kr,
        impact=impact,
        products_text=products_text,
        default_action=action,
        content_type="News",
    )
    summary = analysis.get("Summary", "")
    ai_analysis = analysis.get("AI Analysis", "")
    executive = analysis.get("ExecutiveMessage", "") or f"{issue_kr} 이슈입니다. {action}"
    action = analysis.get("Action Plan", action)
    priority_group = "CORE" if selected and impact == "Direct" and final_score >= 85 else "POLICY_WATCH" if selected and must_keep_policy and impact == "Watch" else "USABLE" if selected else "EXCLUDED"

    return {
        "URL": link, "BestLinkURL": link, "original_url": link, "URL_Quality": link_status, "topic": topic, "topic_score": topic_score,
        "samsung_impact": impact, "samsung_impact_score": samsung_score, "subsidiary_score": 0,
        "action_score": action_score, "urgency_score": urgency_score, "final_score": final_score, "Risk": risk,
        "selected": "Y" if selected else "N", "priority_group": priority_group,
        "mail_section": "News Core" if priority_group == "CORE" else "Policy Watch" if priority_group == "POLICY_WATCH" else "News Usable" if priority_group == "USABLE" else "Excluded",
        "topic_keyword": issue_kr, "topic_reason": "; ".join(reasons), "issue_type": topic,
        "cluster_key": clean(row.get("IssueClusterKey", normalize_title(headline))),
        "affected_subsidiary": "SEC/HQ" if impact == "Direct" else "관련 법인 검토" if impact in {"Indirect", "Watch"} else "",
        "affected_subsidiaries": "SEC/HQ" if impact == "Direct" else "관련 법인 검토" if impact in {"Indirect", "Watch"} else "",
        "affected_products": products_text, "subsidiary_products": products_text, "subsidiary_reason": "; ".join(reasons),
        "impact_production_subsidiaries": "관련 법인 검토" if impact in {"Direct", "Indirect", "Watch"} else "",
        "impact_sales_subsidiaries": "관련 법인 검토" if impact in {"Direct", "Indirect", "Watch"} else "",
        "impact_products": products_text, "fta_impact": "검토 필요" if topic == "ORIGIN_FTA" else "본문에서 확인 불가",
        "export_control_impact": "검토 필요" if topic == "EXPORT_CONTROL" else "본문에서 확인 불가",
        "hs_impact": "검토 필요" if topic == "HS_CLASSIFICATION" else "본문에서 확인 불가",
        "tariff_impact": "검토 필요" if topic in {"TARIFF", "AD_CVD"} else "본문에서 확인 불가",
        "RequiredAction": action, "ActionOwner": owner, "ExecutiveMessage": executive,
        "samsung_score": samsung_score, "samsung_reason": "; ".join(reasons) if reasons else "weak_or_reference",
        "Summary": summary, "AI Analysis": ai_analysis, "Action Plan": action,
        "RejectReason": "; ".join(sorted(set(rejects))), "original_url": link,
        "article_extract_status": analysis.get("article_extract_status", clean(row.get("article_extract_status", "NOT_FETCHED_STEP4_GUARDRAIL"))),
        "article_source_type": clean(row.get("article_source_type", "STEP4_GEMINI_URL_BODY")),
        "effective_date_hint": "본문에서 확인 불가", "change_detail_hint": "본문에서 확인 불가", "hs_hint": "본문에서 확인 불가",
        "tariff_rate_hint": "본문에서 확인 불가", "last_checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def read_input() -> pd.DataFrame:
    candidates = []
    if INPUT_FILE:
        candidates.append(Path(INPUT_FILE))
    for path in [INPUT_ARTICLE_FILE, INPUT_SUMMARY_FILE]:
        if path not in candidates:
            candidates.append(path)

    errors = []
    for path in candidates:
        if not path.exists():
            errors.append(f"{path}: missing")
            continue
        try:
            df = normalize_columns(pd.read_excel(path))
        except Exception as exc:
            errors.append(f"{path}: read_failed:{type(exc).__name__}")
            continue
        if df.empty:
            errors.append(f"{path}: empty")
            continue
        log(f"LOAD {path}: {len(df)} rows")
        if path.name == "3-2.news_article_summary.xlsx":
            log("INPUT MODE: article_summary body-enriched")
        else:
            log("INPUT MODE: summary fallback")
        return df

    raise FileNotFoundError("no valid news input file found / " + " | ".join(errors))


def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)
    defaults = {
        "Date":"", "CollectedAt":"", "Headline":"", "URL":"", "GoogleURL":"", "OriginalURLCandidate":"", "BestLinkURL":"",
        "Country":"Global", "Agency":"", "Publisher":"", "Source":"", "SourceFile":"", "KeywordMatches":"", "ClusterHeadlines":"",
        "RegulationRelated":"N", "RegulationTransferType":"None", "IssueClusterKey":"", "SelectReason":"", "article_body":"",
    }
    for k,v in defaults.items():
        if k not in df.columns:
            df[k] = v
    return df


def append_reason(existing: str, reason: str) -> str:
    parts = [x.strip() for x in clean(existing).split(";") if x.strip()]
    if reason not in parts:
        parts.append(reason)
    return "; ".join(parts)


def report_issue_key(row: pd.Series) -> str:
    topic = clean(row.get("topic", "TRADE_GENERAL")).upper()
    blob = " ".join(clean(row.get(c, "")) for c in [
        "Headline", "Summary", "AI Analysis", "ClusterHeadlines", "topic_reason", "KeywordMatches"
    ]).lower()

    if contains_any(blob, ["ai chip", "ai chips", "ai칩", "ai 칩"]) and contains_any(blob, ["taiwan", "대만"]) and contains_any(blob, ["china", "중국"]):
        return "EXPORT_CONTROL:taiwan_ai_chip_china"
    if contains_any(blob, ["belgium", "belgian", "벨기에"]) and contains_any(blob, ["semiconductor", "반도체", "battery", "배터리"]):
        return "BILATERAL:belgium_semiconductor_battery"
    if contains_any(blob, ["uk", "britain", "영국"]) and contains_any(blob, ["steel", "철강"]) and contains_any(blob, ["tariff", "관세", "quota", "쿼터"]):
        return "TARIFF:uk_steel_tariff_quota"
    if contains_any(blob, ["eu", "european union", "유럽연합"]) and contains_any(blob, ["steel", "철강"]) and contains_any(blob, ["cbam", "carbon", "탄소", "tariff", "관세", "quota", "쿼터"]):
        return "CBAM_TARIFF:eu_steel_cbam_tariff"
    if contains_any(blob, ["cbam certificate", "cbam certificate price", "certificate price"]):
        return "CBAM_CARBON:certificate_price"
    if contains_any(blob, ["section 301", "301조"]):
        return "TARIFF:section_301"
    if contains_any(blob, ["section 232", "232조"]):
        return "TARIFF:section_232"
    if contains_any(blob, ["forced labor", "uflpa", "강제노동"]):
        return "EXPORT_CONTROL:forced_labor"
    cluster = normalize_title(clean(row.get("cluster_key", "")))
    if cluster and len(cluster) >= 8:
        return f"{topic}:{cluster[:80]}"

    title = normalize_title(clean(row.get("Headline", "")))
    title = re.sub(r"\b(reuters|bloomberg|guardian|financial times|news|뉴스|단독|속보)\b", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return f"{topic}:{title[:80]}"


def compress_report_duplicates(audit: pd.DataFrame) -> pd.DataFrame:
    if audit.empty or "selected" not in audit.columns:
        return audit

    audit = audit.copy()
    selected_mask = audit["selected"].eq("Y")
    if not selected_mask.any():
        return audit

    selected = audit[selected_mask].copy()
    selected["_report_issue_key"] = selected.apply(report_issue_key, axis=1)
    selected = selected.sort_values(
        ["final_score", "topic_score", "samsung_impact_score", "Date"],
        ascending=[False, False, False, False],
    )
    duplicate_idx = selected[selected.duplicated("_report_issue_key", keep="first")].index
    if len(duplicate_idx) == 0:
        return audit

    audit.loc[duplicate_idx, "selected"] = "N"
    audit.loc[duplicate_idx, "priority_group"] = "EXCLUDED"
    audit.loc[duplicate_idx, "mail_section"] = "Excluded"
    audit.loc[duplicate_idx, "Risk"] = "하"
    audit.loc[duplicate_idx, "final_score"] = audit.loc[duplicate_idx, "final_score"].apply(lambda v: min(int(v or 0), MIN_SELECT_SCORE - 1))
    audit.loc[duplicate_idx, "RejectReason"] = audit.loc[duplicate_idx, "RejectReason"].apply(
        lambda v: append_reason(v, "report_issue_duplicate_compressed")
    )
    return audit


def resolve_selected_google_links(audit: pd.DataFrame) -> pd.DataFrame:
    if audit.empty or "selected" not in audit.columns:
        return audit
    audit = audit.copy()
    selected_idx = audit.index[audit["selected"].eq("Y")].tolist()
    for idx in selected_idx:
        current = safe_url(audit.at[idx, "BestLinkURL"] if "BestLinkURL" in audit.columns else audit.at[idx, "URL"])
        if not is_google_article_redirect(current):
            fixed = safe_url(current)
            if not is_real_original_url(fixed):
                audit.at[idx, "selected"] = "N"
                audit.at[idx, "priority_group"] = "EXCLUDED"
                audit.at[idx, "mail_section"] = "Excluded"
                audit.at[idx, "Risk"] = "??"
                audit.at[idx, "final_score"] = min(int(audit.at[idx, "final_score"] or 0), MIN_SELECT_SCORE - 1)
                audit.at[idx, "RejectReason"] = append_reason(audit.at[idx, "RejectReason"], "non_original_or_google_home_url")
                continue
            audit.at[idx, "URL"] = fixed
            audit.at[idx, "BestLinkURL"] = fixed
            audit.at[idx, "original_url"] = fixed
            continue

        resolved = resolve_google_news_url(current)
        if resolved and is_real_original_url(resolved):
            audit.at[idx, "URL"] = resolved
            audit.at[idx, "BestLinkURL"] = resolved
            audit.at[idx, "original_url"] = resolved
            audit.at[idx, "URL_Quality"] = "GOOGLE_NEWS_RESOLVED"
        else:
            audit.at[idx, "selected"] = "N"
            audit.at[idx, "priority_group"] = "EXCLUDED"
            audit.at[idx, "mail_section"] = "Excluded"
            audit.at[idx, "Risk"] = "하"
            audit.at[idx, "final_score"] = min(int(audit.at[idx, "final_score"] or 0), MIN_SELECT_SCORE - 1)
            audit.at[idx, "RejectReason"] = append_reason(audit.at[idx, "RejectReason"], "google_news_original_url_unresolved")
    return audit


POLICY_EXPAND_TOPICS = {
    "EXPORT_CONTROL", "AD_CVD", "CBAM_CARBON", "ORIGIN_FTA",
    "HS_CLASSIFICATION", "TARIFF", "CUSTOMS",
}

POLICY_EXPAND_HARD_REJECTS = {
    "no_valid_url",
    "event_training_tender_noise",
    "financial_industry_noise_without_trade_policy",
    "samsung_general_business_noise",
    "general_economy_without_samsung_policy",
    "low_value_general_news",
    "bilateral_industry_news_without_trade_policy",
    "ai_chip_industry_without_control_signal",
    "export_control_industry_without_control_signal",
    "google_news_original_url_unresolved",
    "future_date_abnormal",
}


def policy_expand_text(row: pd.Series) -> str:
    cols = [
        "Headline", "Summary", "ClusterHeadlines",
    ]
    return " ".join(clean(row.get(c, "")) for c in cols).lower()


POLICY_EXPAND_DIRECT_TERMS = [
    "tariff", "customs", "customs duty", "import duty", "anti-dumping", "antidumping",
    "countervailing", "ad/cvd", "section 301", "section 232", "quota", "tariff quota",
    "cbam", "carbon border", "origin", "rules of origin", "fta", "cepa", "usmca",
    "export control", "entity list", "forced labor", "uflpa", "hs code",
    "classification", "clearance", "declaration",
    "관세", "통관", "수입관세", "덤핑방지", "반덤핑", "상계관세", "할당관세",
    "쿼터", "원산지", "자유무역협정", "수출통제", "강제노동", "품목분류",
    "신고", "보세", "관세율",
]


POLICY_EXPAND_CONTEXT_NOISE = [
    "iran", "tehran", "hormuz", "oil facility", "oilfield", "missile", "bombing",
    "attack", "war", "military", "ceasefire", "crude oil", "oil price",
    "이란", "테헤란", "호르무즈", "하르그", "원유", "유전", "석유시설",
    "폭격", "공격", "전쟁", "군사", "미사일", "휴전",
]


def has_direct_policy_terms(blob: str) -> bool:
    return contains_any(blob, POLICY_EXPAND_DIRECT_TERMS)


def is_context_noise_without_policy(row: pd.Series) -> bool:
    blob = policy_expand_text(row)
    return contains_any(blob, POLICY_EXPAND_CONTEXT_NOISE) and not has_direct_policy_terms(blob)


def has_policy_expand_signal(row: pd.Series) -> bool:
    blob = policy_expand_text(row)
    topic = clean(row.get("topic")).upper()
    if topic not in POLICY_EXPAND_TOPICS:
        return False
    if not has_direct_policy_terms(blob):
        return False
    if contains_any(blob, MUST_KEEP_POLICY_TERMS) or contains_any(blob, CONCRETE_TRADE_POLICY_TERMS):
        return True
    if topic == "EXPORT_CONTROL" and contains_any(blob, ["export control", "수출통제", "entity list", "forced labor", "uflpa"]):
        return True
    if topic == "AD_CVD" and contains_any(blob, ["anti-dumping", "antidumping", "countervailing", "반덤핑", "상계관세"]):
        return True
    if topic == "ORIGIN_FTA" and contains_any(blob, ["fta", "usmca", "cepa", "origin", "원산지", "trade agreement", "통상협정"]):
        return True
    if topic == "CBAM_CARBON" and contains_any(blob, ["cbam", "carbon border", "탄소국경", "탄소세"]):
        return True
    if topic == "TARIFF" and contains_any(blob, ["tariff", "customs duty", "import duty", "quota", "관세", "쿼터"]):
        return True
    if topic == "CUSTOMS" and contains_any(blob, ["customs", "clearance", "declaration", "통관", "신고", "보세"]):
        return True
    return False


def can_expand_policy_watch(row: pd.Series) -> bool:
    if clean(row.get("selected")).upper() == "Y":
        return False
    score = int(float(row.get("final_score", 0) or 0))
    if score < NEWS_EXPAND_MIN_SCORE:
        return False
    rr = clean(row.get("RejectReason"))
    reasons = {x.strip() for x in rr.split(";") if x.strip()}
    if "report_issue_duplicate_compressed" in reasons:
        return False
    if reasons & POLICY_EXPAND_HARD_REJECTS:
        return False
    if not is_valid_link(row.get("URL", "")):
        return False
    if is_context_noise_without_policy(row):
        return False
    return has_policy_expand_signal(row)


def expand_policy_watch_selection(audit: pd.DataFrame) -> pd.DataFrame:
    if audit.empty or "selected" not in audit.columns:
        return audit
    audit = audit.copy()
    selected_count = int(audit["selected"].eq("Y").sum())
    target = max(0, min(NEWS_TARGET_MIN, NEWS_TARGET_MAX, TOP_N_MAX))
    if selected_count >= target:
        return audit

    candidates = audit[audit.apply(can_expand_policy_watch, axis=1)].copy()
    if candidates.empty:
        return audit
    candidates = candidates.sort_values(
        ["final_score", "topic_score", "Date"],
        ascending=[False, False, False],
    )
    need = max(0, min(target - selected_count, NEWS_TARGET_MAX - selected_count, TOP_N_MAX - selected_count))
    add_idx = candidates.head(need).index
    if len(add_idx) == 0:
        return audit

    audit.loc[add_idx, "selected"] = "Y"
    audit.loc[add_idx, "priority_group"] = "POLICY_WATCH"
    audit.loc[add_idx, "mail_section"] = "Policy Watch"
    audit.loc[add_idx, "samsung_impact"] = audit.loc[add_idx, "samsung_impact"].replace({"Reference": "Watch", "": "Watch"})
    audit.loc[add_idx, "Risk"] = audit.loc[add_idx, "final_score"].apply(lambda v: "상" if int(v or 0) >= 85 else "중")
    audit.loc[add_idx, "RejectReason"] = audit.loc[add_idx, "RejectReason"].apply(
        lambda v: append_reason(v, "expanded_policy_watch")
    )
    audit.loc[add_idx, "final_score"] = audit.loc[add_idx, "final_score"].apply(lambda v: max(int(v or 0), NEWS_EXPAND_MIN_SCORE))
    return audit


def build(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = ensure_cols(df)
    rows = []
    for _, row in df.iterrows():
        d = row.to_dict()
        d.update(score_row(row))
        rows.append(d)
    audit = normalize_columns(pd.DataFrame(rows))
    # remove cross-source duplicates by title after scoring
    audit["_title_norm"] = audit["Headline"].apply(normalize_title)
    audit = audit.sort_values(["selected", "final_score", "Date"], ascending=[False, False, False])
    audit = audit.drop_duplicates(subset=["_title_norm"], keep="first").drop(columns=["_title_norm"], errors="ignore")
    audit = compress_report_duplicates(audit)
    audit = expand_policy_watch_selection(audit)
    audit = resolve_selected_google_links(audit)
    audit = compress_report_duplicates(audit)

    daily = audit[audit["selected"].eq("Y")].copy()
    daily = daily.sort_values(["final_score", "topic_score", "samsung_impact_score"], ascending=[False, False, False]).reset_index(drop=True)
    if len(daily) > NEWS_TARGET_MAX:
        keep_keys = set(daily.head(NEWS_TARGET_MAX)["Headline"].astype(str))
        drop_mask = audit["selected"].eq("Y") & ~audit["Headline"].astype(str).isin(keep_keys)
        audit.loc[drop_mask, "selected"] = "N"
        audit.loc[drop_mask, "priority_group"] = "EXCLUDED"
        audit.loc[drop_mask, "mail_section"] = "Excluded"
        audit.loc[drop_mask, "RejectReason"] = audit.loc[drop_mask, "RejectReason"].apply(
            lambda v: append_reason(v, "over_news_target_max")
        )
        daily = audit[audit["selected"].eq("Y")].copy().sort_values(
            ["final_score", "topic_score", "samsung_impact_score"],
            ascending=[False, False, False],
        ).reset_index(drop=True)
    daily["rank"] = range(1, len(daily)+1)
    audit = audit.sort_values(["selected", "final_score"], ascending=[False, False]).reset_index(drop=True)
    audit["rank"] = range(1, len(audit)+1)
    excluded = audit[audit["selected"].ne("Y")].copy()

    for frame in [daily, audit, excluded]:
        for col in OUTPUT_COLS:
            if col not in frame.columns:
                frame[col] = ""
    return daily[OUTPUT_COLS], audit[OUTPUT_COLS], excluded[OUTPUT_COLS]


def merge_cumulative(daily: pd.DataFrame) -> pd.DataFrame:
    if OUT_CUMULATIVE.exists():
        try:
            old = normalize_columns(pd.read_excel(OUT_CUMULATIVE))
            log(f"cumulative existing load: {len(old)} rows")
        except Exception as exc:
            log(f"cumulative load failed -> new create: {type(exc).__name__}")
            old = pd.DataFrame(columns=OUTPUT_COLS)
    else:
        log("cumulative file missing -> new create")
        old = pd.DataFrame(columns=OUTPUT_COLS)
    for col in OUTPUT_COLS:
        if col not in old.columns: old[col] = ""
        if col not in daily.columns: daily[col] = ""
    combined = pd.concat([old[OUTPUT_COLS], daily[OUTPUT_COLS]], ignore_index=True, sort=False)
    combined = normalize_columns(combined)
    key = combined["BestLinkURL"].fillna("").astype(str).str.lower().str.strip()
    title = combined["Headline"].fillna("").astype(str).str.lower().str.strip()
    combined["_key"] = key.where(key.ne(""), title)
    combined = combined.drop_duplicates(subset=["_key"], keep="last").drop(columns=["_key"], errors="ignore")
    return combined[OUTPUT_COLS].reset_index(drop=True)


# ======================================================================
# GTI STEP4-2 News Sensing Patch v12 - 2026-06-14
# ----------------------------------------------------------------------
# 목적 기준 보완:
# - 30건을 억지로 채우지 않는다.
# - Reference/무관/일반 산업뉴스는 Step4-2 selected에서 제외한다.
# - selected는 원문 URL, 게시일자, 관세/통상 실행 키워드가 확인되는 기사만 남긴다.
# ======================================================================

def _v12_clean_text(v) -> str:
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return clean(v)


def _v12_news_blob(row) -> str:
    return " ".join(_v12_clean_text(row.get(c, "")) for c in [
        "Headline", "Summary", "AI Analysis", "Action Plan", "OriginalArticle",
        "article_body", "body", "topic_keyword", "Issue", "Publisher", "Source", "URL", "BestLinkURL",
    ]).lower()


def _v12_publish_date(row) -> str:
    for c in ["Publish Date", "Date", "published", "published_at", "collected_at"]:
        v = _v12_clean_text(row.get(c, ""))
        if v and v.lower() not in {"nan", "nat", "none"}:
            return v
    return "확인 필요"


def _v12_news_hard_reject(row) -> bool:
    blob = _v12_news_blob(row)
    title = _v12_clean_text(row.get("Headline", "")).lower()
    url = _v12_clean_text(row.get("BestLinkURL", row.get("URL", "")))
    if _bad_news_url_v11(url):
        return True
    if any(k in blob for k in ["글자크기", "이전 기사보기", "다음 기사보기", "스크롤 이동 상태바"]):
        return True
    weak_title = [
        "청년인턴", "채용", "합격자", "몰카", "범죄", "모닝뉴스", "기억이 행동이 될 때",
        "주가", "증시", "코스피", "환율", "금리", "부동산", "신간", "서평", "bookreview",
        "미토스", "주술", "인류의 살상", "페라리 로마", "첨단산업 협력",
        "finance must be a partner", "industrial ecosystems",
        "보안시장", "인터롭", "순방", "정상회의", "교황", "피렌체",
        "인구 1000만명", "종전 mou", "베트남 새우", "라이스페이퍼",
    ]
    if any(k in title for k in weak_title):
        return True
    impact = _v12_clean_text(row.get("samsung_impact", row.get("Samsung Impact", ""))).lower()
    if impact == "reference":
        strong_title = any(k in title for k in [
            "tariff", "관세", "customs", "anti-dumping", "antidumping", "countervailing",
            "cbam", "fta", "cepa", "origin", "원산지", "export control", "수출통제",
            "section 301", "section 232", "forced labor", "uflpa",
        ])
        if not strong_title:
            return True
    return False


def _v12_news_strong_relevant(row) -> bool:
    blob = _v12_news_blob(row)
    source_focus = " ".join(_v12_clean_text(row.get(c, "")) for c in [
        "Headline", "Summary", "OriginalArticle", "article_body", "body",
    ]).lower()
    strong_terms = [
        "tariff", "tariffs", "customs duty", "import duty", "관세", "관세율", "할당관세", "쿼터", "quota",
        "customs clearance", "customs declaration", "통관", "수입신고", "수출신고", "보세",
        "anti-dumping", "antidumping", "countervailing", "ad/cvd", "반덤핑", "상계관세", "덤핑방지",
        "cbam", "carbon border", "탄소국경",
        "fta", "cepa", "tepa", "rules of origin", "origin", "원산지", "협정세율",
        "export control", "entity list", "forced labor", "uflpa", "수출통제", "전략물자", "강제노동",
        "section 301", "section 232", "301조", "232조",
        "hs code", "classification", "품목분류",
    ]
    if not any(k in source_focus for k in strong_terms):
        return False
    if any(k in blob for k in ["samsung", "삼성", "semiconductor", "반도체", "battery", "배터리", "display", "steel", "철강", "rare earth", "희토류"]):
        return True
    return any(k in source_focus for k in [
        "tariff", "관세", "customs", "통관", "fta", "cepa", "origin", "원산지",
        "anti-dumping", "countervailing", "cbam", "export control", "수출통제",
        "section 301", "section 232", "forced labor", "uflpa",
    ])


try:
    _ORIGINAL_BUILD_V12_NEWS = build

    def build(df: pd.DataFrame):
        daily, excluded, audit = _ORIGINAL_BUILD_V12_NEWS(df)
        if audit is None or audit.empty:
            return daily, excluded, audit

        a = audit.copy()
        for col in ["Summary", "AI Analysis", "Action Plan"]:
            if col in a.columns:
                a[col] = a[col].apply(remove_ui_noise_v11)
        if "BestLinkURL" not in a.columns:
            a["BestLinkURL"] = a.get("URL", "")
        a["Publish Date"] = a.apply(_v12_publish_date, axis=1)
        a["Date"] = a["Publish Date"]
        a["_sensing_score_v12"] = a.apply(_sensing_score_v11, axis=1)
        a["_issue_key_v12"] = a.apply(_issue_key_v11_news, axis=1)
        a["_v12_hard_reject"] = a.apply(_v12_news_hard_reject, axis=1)
        a["_v12_strong_relevant"] = a.apply(_v12_news_strong_relevant, axis=1)

        candidates = a[
            (~a["_v12_hard_reject"])
            & (a["_v12_strong_relevant"])
            & (~a["BestLinkURL"].apply(_bad_news_url_v11))
            & (a["_sensing_score_v12"] >= int(os.getenv("GTI_STEP4_NEWS_V12_MIN_SCORE", "35")))
        ].copy()
        candidates = candidates.sort_values(["_sensing_score_v12", "Date"], ascending=[False, False])
        candidates = candidates.drop_duplicates(subset=["BestLinkURL"], keep="first")
        candidates = candidates.drop_duplicates(subset=["_issue_key_v12"], keep="first")
        selected = candidates.head(NEWS_TARGET_MAX).copy()
        selected["selected"] = "Y"
        selected["final_score"] = selected["_sensing_score_v12"].round().astype(int)
        selected["priority_group"] = selected["final_score"].apply(lambda v: "CORE" if v >= 75 else "USABLE")
        selected["RejectReason"] = selected.get("RejectReason", "").fillna("").astype(str)
        if "samsung_impact" in selected.columns:
            selected["samsung_impact"] = selected["samsung_impact"].replace({"Reference": "Watch", "": "Watch"}).fillna("Watch")
        if "Samsung Impact" in selected.columns:
            selected["Samsung Impact"] = selected["Samsung Impact"].replace({"Reference": "Watch", "": "Watch"}).fillna("Watch")

        selected_keys = set(selected["BestLinkURL"].fillna("").astype(str).str.lower().str.strip())
        audit = a.copy()
        audit["selected"] = audit.apply(lambda r: "Y" if _v12_clean_text(r.get("BestLinkURL", r.get("URL", ""))).lower().strip() in selected_keys else "N", axis=1)
        audit.loc[audit["selected"].ne("Y") & audit["_v12_hard_reject"], "RejectReason"] = audit.loc[audit["selected"].ne("Y") & audit["_v12_hard_reject"], "RejectReason"].apply(lambda v: append_reason(v, "v12_hard_reference_or_noise"))
        audit.loc[audit["selected"].ne("Y") & ~audit["_v12_strong_relevant"], "RejectReason"] = audit.loc[audit["selected"].ne("Y") & ~audit["_v12_strong_relevant"], "RejectReason"].apply(lambda v: append_reason(v, "v12_no_customs_trade_action_signal"))
        excluded = audit[audit["selected"].ne("Y")].copy()
        daily = selected.drop(columns=["_sensing_score_v12", "_issue_key_v12", "_v12_hard_reject", "_v12_strong_relevant"], errors="ignore").reset_index(drop=True)
        return daily, excluded, audit
except Exception:
    pass


def to_legacy(daily: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for i, r in daily.reset_index(drop=True).iterrows():
        rows.append({
            "No": i+1,
            "Content Type": "News",
            "Mail Group": "News - 핵심" if clean(r.get("priority_group")) == "CORE" else "News - 주요/참고",
            "Samsung Impact": clean(r.get("samsung_impact")),
            "Affected Subsidiary": clean(r.get("affected_subsidiary")),
            "Impact Reason": clean(r.get("samsung_reason")),
            "Date": clean(r.get("Date")),
            "Headline": clean(r.get("Headline")),
            "Summary": clean(r.get("Summary")),
            "AI Analysis": clean(r.get("AI Analysis")),
            "Action Plan": clean(r.get("Action Plan")),
            "Country": clean(r.get("Country")),
            "Agency": clean(r.get("Agency")),
            "Risk": clean(r.get("Risk")),
            "Importance Score": int(r.get("final_score", 0) or 0),
            "Priority Group": clean(r.get("priority_group")),
            "Issue": clean(r.get("topic_keyword")),
            "Cluster": clean(r.get("cluster_key")),
            "URL": clean(r.get("BestLinkURL")),
            "Source": clean(r.get("Source")),
            "Source File": clean(r.get("SourceFile")),
        })
    return pd.DataFrame(rows, columns=LEGACY_COLS)


def write_excel(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_excel(path, index=False)
    except PermissionError:
        alt = path.with_name(path.stem + f"_{datetime.now():%Y%m%d_%H%M%S}" + path.suffix)
        df.to_excel(alt, index=False)
        log(f"SAVE fallback due to file lock: {alt}")



# ======================================================================
# GTI STEP4 Gemini Quality Patch v6.0
# ----------------------------------------------------------------------
# v5 보완
# - 기존 gti_step4_gemini_cache.xlsx에 저장된 fallback/일반문구를 무시하고 재분석
# - Gemini API Key가 없거나 호출 실패해도 headline 반복이 아닌 본문 기반 fallback 분석 생성
# - AI Analysis / Action Plan 반복문구 자동 차단
# - 실행 로그에 Gemini 사용 가능 여부 표시
# ======================================================================

_GENERIC_ANALYSIS_MARKERS = [
    "원문 URL 접속 가능 여부",
    "원문 본문 기반 세부 영향은 Summary 내용을 기준으로",
    "본문 확인 불가로 정밀 영향 분석이 제한됩니다",
    "관련 뉴스입니다. 삼성 영향도는",
    "이슈입니다. 삼성 영향도는",
    "관련 공식 규제/공지 후보입니다",
]

def _is_generic_or_bad_analysis(text: str) -> bool:
    t = clean(text)
    if not t:
        return True
    return any(m in t for m in _GENERIC_ANALYSIS_MARKERS)

def _is_bad_cached_analysis(item: dict, headline: str) -> bool:
    if not item:
        return True
    summary = clean(item.get("Summary", ""))
    ai = clean(item.get("AI Analysis", ""))
    action = clean(item.get("Action Plan", ""))
    status = clean(item.get("article_extract_status", ""))
    if _looks_like_title_only(summary, headline):
        return True
    if _is_generic_or_bad_analysis(ai):
        return True
    if _is_generic_or_bad_analysis(action):
        return True
    if status and not status.startswith("GEMINI_OK"):
        # v5 fallback cache. Re-analyze when possible.
        return True
    return False

def _extract_terms_for_analysis(text: str) -> dict:
    t = clean(text)
    hs = sorted(set(re.findall(r"\b\d{4}(?:\.\d{2})?(?:\.\d{2})?\b", t)))[:6]
    rates = sorted(set(re.findall(r"\b\d{1,3}(?:\.\d+)?\s*%", t)))[:6]
    countries = []
    for c in ["미국", "중국", "일본", "EU", "유럽", "베트남", "인도", "모로코", "한국", "영국", "멕시코", "캐나다", "United States", "China", "Japan", "Vietnam", "India", "Morocco", "Korea", "EU"]:
        if c.lower() in t.lower():
            countries.append(c)
    return {"hs": "; ".join(hs) or "본문에서 확인 불가", "rates": "; ".join(rates) or "본문에서 확인 불가", "countries": "; ".join(dict.fromkeys(countries)) or "본문에서 확인 불가"}

def _fallback_gti_analysis_from_body(*, body: str, headline: str, issue: str, impact: str, products_text: str, default_action: str, content_type: str) -> dict:
    summary = _simple_body_summary(body, headline)
    terms = _extract_terms_for_analysis(" ".join([headline, body, summary]))
    issue_l = clean(issue)

    if issue_l in {"반덤핑/상계관세", "AD/CVD"}:
        ai = (
            f"반덤핑/상계관세 이슈입니다. 원문상 대상 국가/지역은 {terms['countries']}, "
            f"확인된 세율 정보는 {terms['rates']}입니다. 삼성전자 관세업무 관점에서는 해당 철강·소재·부품 HS가 "
            f"해외 생산법인 또는 협력사 수입품에 포함되는지 확인해야 하며, 적용 대상이면 추가관세 비용, 원산지 증빙, "
            f"공급자 가격자료 방어 리스크가 발생할 수 있습니다. 영향등급은 {impact}, 관련 제품은 {products_text}입니다."
        )
        action = (
            "즉시조치: 대상 품목명·HS·공급국·공급자 리스트를 수입실적과 매칭하십시오. "
            "1주 내: 최근 12개월 수입금액 기준 잠재 AD/CVD 비용을 산출하십시오. "
            "1개월 내: 원산지/가격자료/공급자 진술서 방어 파일을 구축하고 관세사 신고 기준을 공유하십시오. "
            "Owner: HQ Customs + 구매 + 해당 법인 통관담당"
        )
    elif issue_l == "수출통제":
        ai = (
            f"수출통제 이슈입니다. 원문상 관련 국가/지역은 {terms['countries']}입니다. 삼성전자 관점에서는 반도체, AI칩, "
            f"희토류, 장비·부품 등 전략물자 또는 이중용도 품목과 연결될 수 있는지 확인해야 합니다. "
            f"수출허가, 최종사용자 확인, 우회수출 스크리닝, Item Master의 Export Control Flag 관리가 필요합니다. "
            f"영향등급은 {impact}, 관련 제품은 {products_text}입니다."
        )
        action = (
            "즉시조치: 대상 품목의 ECCN/전략물자 해당 여부와 거래국·최종사용자를 확인하십시오. "
            "1주 내: 관련 법인과 거래처 스크리닝 결과를 재점검하십시오. "
            "1개월 내: Item Master에 수출통제 Flag 및 허가필요 여부를 반영하십시오. "
            "Owner: HQ Export Control + 사업부 + 해외법인"
        )
    elif issue_l == "CBAM":
        ai = (
            f"CBAM/탄소국경조정 이슈입니다. 원문상 관련 지역은 {terms['countries']}입니다. 삼성전자 관점에서는 EU향 수출입 "
            f"품목 중 철강·알루미늄 등 CBAM 대상 원재료/부품 사용 여부, 공급사 배출량 자료 확보, CBAM 신고 및 인증서 비용 "
            f"반영 여부가 핵심입니다. 영향등급은 {impact}, 관련 제품은 {products_text}입니다."
        )
        action = (
            "즉시조치: EU향 품목과 공급사 배출량 자료 보유 여부를 확인하십시오. "
            "1주 내: CBAM 대상 CN/HS와 공급사별 배출량 Gap List를 작성하십시오. "
            "1개월 내: 인증서 비용 산정 및 ESG/통관 공동관리 프로세스를 수립하십시오. "
            "Owner: HQ Customs + ESG + EU 법인"
        )
    elif issue_l in {"FTA/원산지", "ORIGIN_FTA"}:
        ai = (
            f"FTA/원산지 이슈입니다. 원문상 관련 국가/지역은 {terms['countries']}입니다. 삼성전자 관점에서는 대상 협정의 "
            f"원산지 기준, 누적, 직접운송, CO 발급/수취 요건이 기존 FTA Master·HS Master·Item Master와 일치하는지 확인해야 합니다. "
            f"특혜세율 적용 오류 또는 CO 발급 오류가 발생할 수 있습니다. 영향등급은 {impact}, 관련 제품은 {products_text}입니다."
        )
        action = (
            "즉시조치: 대상 협정·국가·품목의 FTA 적용 여부와 CO 발급/수취 현황을 확인하십시오. "
            "1주 내: BOM 원산지, Vendor 원산지확인서, HS 기준 일치 여부를 점검하십시오. "
            "1개월 내: FTA Master·HS Master·Item Master 업데이트를 진행하십시오. "
            "Owner: HQ Customs/FTA + 법인 구매/물류"
        )
    elif issue_l in {"통관/세관", "통관", "CUSTOMS"}:
        ai = (
            f"통관/세관 절차 이슈입니다. 원문상 관련 국가/지역은 {terms['countries']}입니다. 삼성전자 관점에서는 수입신고, "
            f"보세운송, 보세공장, 납세자료 제출, 관세사 신고 프로세스 변경 여부가 중요합니다. 신고 지연, 자동수리 조건 오류, "
            f"세관 제출자료 누락 리스크가 있습니다. 영향등급은 {impact}, 관련 제품은 {products_text}입니다."
        )
        action = (
            "즉시조치: 해당 법인 관세사에 신고절차·제출자료 변경 여부를 확인하십시오. "
            "1주 내: 통관 SOP와 보세/수입신고 체크리스트를 개정하십시오. "
            "1개월 내: ONE-Origin/ERP 반영 필요 필드를 정의하십시오. "
            "Owner: HQ Customs + 법인 통관담당 + 관세사"
        )
    elif issue_l in {"HS/품목분류", "HS_CLASSIFICATION"}:
        ai = (
            f"HS/품목분류 이슈입니다. 원문에서 확인된 HS 후보는 {terms['hs']}입니다. 삼성전자 관점에서는 동일 품목에 대한 "
            f"법인·관세사별 HS 불일치, 관세율·FTA 세율·AD/CVD 적용 오류 가능성을 점검해야 합니다. "
            f"영향등급은 {impact}, 관련 제품은 {products_text}입니다."
        )
        action = (
            "즉시조치: 관련 품목의 HS Master와 실제 신고 HS를 비교하십시오. "
            "1주 내: 불일치 품목의 Root Cause 및 변경 승인자료를 확보하십시오. "
            "1개월 내: HS 변경 Workflow와 관세율 영향표를 반영하십시오. "
            "Owner: HQ Customs + 법인 Master Data 담당"
        )
    else:
        ai = (
            f"{issue_l} 이슈입니다. 원문상 관련 국가/지역은 {terms['countries']}입니다. 삼성전자 관세업무 관점에서는 "
            f"대상 국가·품목·HS·세율·시행일을 기준으로 수입통관, 수출통관, FTA/원산지, 관세비용 영향 여부를 확인해야 합니다. "
            f"영향등급은 {impact}, 관련 제품은 {products_text}입니다."
        )
        action = (
            "즉시조치: 원문 기준 대상 국가·품목·HS·시행일을 확인하십시오. "
            "1주 내: 관련 법인 수입/수출 실적과 매칭하십시오. "
            "1개월 내: 필요 시 Master Data와 관세사 신고 기준을 업데이트하십시오. "
            f"Owner: {default_action}"
        )

    return {
        "Summary": summary[:900],
        "AI Analysis": ai[:1200],
        "Action Plan": action[:1200],
        "ExecutiveMessage": (summary[:220] + " " + ai[:240])[:700],
        "article_extract_status": "FALLBACK_RULE_BODY",
    }

def build_gti_ai_analysis(row: pd.Series, *, headline: str, url: str, issue: str, impact: str, products_text: str, default_action: str, content_type: str) -> dict:
    """v6 override: Gemini first; ignore stale fallback cache; useful fallback if Gemini unavailable."""
    body, status = _fallback_source_body(row, headline)
    if not body:
        body, status = fetch_article_body_for_ai(url)

    cache = _ensure_gemini_cache()
    key = _analysis_cache_key(url, headline)
    cached = cache.get(key)
    if cached and not _is_bad_cached_analysis(cached, headline):
        return cached

    prompt = f"""
당신은 삼성전자 본사 관세/통상 리스크 분석가입니다.
아래 원문을 읽고 GTI Radar 임원보고용으로 분석하십시오.

절대 금지:
- 제목 반복 금지
- "관련 뉴스입니다", "공식 규제/공지 후보입니다" 같은 템플릿 문장 금지
- 본문에 없는 세율/HS/국가/시행일을 지어내지 말 것
- 본문을 읽을 수 없으면 Summary에 "본문 확인 불가"라고 명시

출력은 JSON만:
{{
  "Summary": "원문 기준 게시물 요약 2~3줄",
  "AI Analysis": "삼성전자 관세업무 영향. 수입통관/수출통관/FTA·원산지/HS/관세비용/수출통제 중 해당 항목을 구체적으로 설명",
  "Action Plan": "즉시조치/1주 내/1개월 내/Owner 형식의 구체적 대응방안",
  "ExecutiveMessage": "임원용 한 문단 핵심 메시지"
}}

기본 정보:
- Content Type: {content_type}
- Issue: {issue}
- Samsung Impact: {impact}
- Affected Products: {products_text}
- URL: {url}
- Headline: {headline}
- Default Action Hint: {default_action}

원문:
{body[:ARTICLE_MAX_CHARS]}
""".strip()

    result = call_gemini_json(prompt)
    if result and not result.get("_error"):
        summary = clean(result.get("Summary", ""))
        ai = clean(result.get("AI Analysis", ""))
        action_plan = clean(result.get("Action Plan", ""))
        executive = clean(result.get("ExecutiveMessage", ""))

        if not _looks_like_title_only(summary, headline) and not _is_generic_or_bad_analysis(ai) and not _is_generic_or_bad_analysis(action_plan):
            final = {
                "Summary": summary[:900],
                "AI Analysis": ai[:1200],
                "Action Plan": action_plan[:1200],
                "ExecutiveMessage": (executive or summary)[:700],
                "article_extract_status": f"GEMINI_OK|{status}",
            }
            cache[key] = final
            _save_gemini_cache()
            return final

    final = _fallback_gti_analysis_from_body(
        body=body,
        headline=headline,
        issue=issue,
        impact=impact,
        products_text=products_text,
        default_action=default_action,
        content_type=content_type,
    )
    final["article_extract_status"] = f"{final.get('article_extract_status')}|{status}|GEMINI={'Y' if GEMINI_API_KEY else 'NO_KEY'}"
    # Cache fallback only when Gemini is unavailable, but mark it so later runs with API key can regenerate.
    cache[key] = final
    _save_gemini_cache()
    return final

def _gti_step4_gemini_log_once():
    try:
        log(f"Gemini analysis: enabled={USE_GEMINI}, api_key={'Y' if GEMINI_API_KEY else 'N'}, model={GEMINI_MODEL}, cache={GEMINI_CACHE_FILE}")
    except Exception:
        pass

# ======================================================================
# End of GTI STEP4 Gemini Quality Patch v6.0
# ======================================================================


# ======================================================================
# GTI STEP4 Article Body Extraction Patch v7.0
# ----------------------------------------------------------------------
# v6 보완
# - 원문 본문 확보율 개선: trafilatura / readability-lxml / BeautifulSoup / meta fallback
# - PDF URL 본문 추출: pypdf 또는 PyPDF2 사용 가능 시 처리
# - UNIPASS 등 동적 페이지는 URL별 상세 본문 확보 실패 시 명확한 status 기록
#
# 권장 설치:
#   pip install trafilatura beautifulsoup4 readability-lxml lxml pypdf requests
# ======================================================================

ARTICLE_MIN_CHARS = int(os.getenv("GTI_ARTICLE_MIN_CHARS", "250"))

def _optional_import(module_name: str):
    try:
        return __import__(module_name)
    except Exception:
        return None

def _decode_bytes(raw: bytes, content_type: str = "") -> str:
    charset = ""
    m = re.search(r"charset=([\w\-]+)", content_type or "", re.I)
    if m:
        charset = m.group(1)
    for enc in [charset, "utf-8", "cp949", "euc-kr", "latin-1"]:
        if not enc:
            continue
        try:
            return raw.decode(enc, "ignore")
        except Exception:
            pass
    return raw.decode("utf-8", "ignore")

def _fetch_url_bytes(url: str) -> tuple[bytes, str, str]:
    u = safe_url(url)
    if not u:
        return b"", "", "NO_URL"
    try:
        req = urllib.request.Request(
            u,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/129 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml,application/pdf;q=0.9,*/*;q=0.8",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                "Referer": "https://www.google.com/",
            },
        )
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=ARTICLE_FETCH_TIMEOUT, context=ctx) as resp:
            raw = resp.read(5_000_000)
            ctype = resp.headers.get("Content-Type", "")
        return raw, ctype, "FETCH_OK"
    except Exception as exc:
        return b"", "", f"FETCH_FAILED:{type(exc).__name__}"

def _extract_pdf_text_from_bytes(raw: bytes) -> tuple[str, str]:
    if not raw:
        return "", "PDF_EMPTY"
    try:
        import io
        try:
            from pypdf import PdfReader
            lib = "pypdf"
        except Exception:
            from PyPDF2 import PdfReader
            lib = "PyPDF2"
        reader = PdfReader(io.BytesIO(raw))
        pages = []
        for page in reader.pages[:12]:
            try:
                pages.append(page.extract_text() or "")
            except Exception:
                pass
        text = "\n".join(pages).strip()
        if text:
            return text[:ARTICLE_MAX_CHARS], f"PDF_EXTRACTED:{lib}"
        return "", f"PDF_TEXT_EMPTY:{lib}"
    except Exception as exc:
        return "", f"PDF_EXTRACT_FAILED:{type(exc).__name__}"

def _extract_html_with_trafilatura(html_text: str, url: str) -> tuple[str, str]:
    try:
        import trafilatura
        extracted = trafilatura.extract(
            html_text,
            url=url,
            include_comments=False,
            include_tables=True,
            favor_recall=True,
            output_format="txt",
        )
        if extracted and len(extracted.strip()) >= ARTICLE_MIN_CHARS:
            return extracted.strip()[:ARTICLE_MAX_CHARS], "TRAFILATURA"
    except Exception:
        pass
    return "", "TRAFILATURA_EMPTY"

def _extract_html_with_readability(html_text: str) -> tuple[str, str]:
    try:
        from readability import Document
        doc = Document(html_text)
        summary_html = doc.summary(html_partial=True)
        text = _strip_html_to_text(summary_html)
        if text and len(text) >= ARTICLE_MIN_CHARS:
            return text[:ARTICLE_MAX_CHARS], "READABILITY"
    except Exception:
        pass
    return "", "READABILITY_EMPTY"

def _extract_html_with_bs4(html_text: str) -> tuple[str, str]:
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_text, "html.parser")
        for tag in soup(["script", "style", "noscript", "svg", "footer", "header", "nav", "aside"]):
            tag.decompose()
        candidates = []
        for selector in ["article", "main", "[role=main]", ".article", ".news", ".content", "#article", "#content"]:
            try:
                for node in soup.select(selector):
                    txt = node.get_text("\n", strip=True)
                    if len(txt) >= ARTICLE_MIN_CHARS:
                        candidates.append(txt)
            except Exception:
                pass
        if not candidates:
            ps = [p.get_text(" ", strip=True) for p in soup.find_all(["p", "li"]) if len(p.get_text(" ", strip=True)) >= 30]
            if ps:
                candidates.append("\n".join(ps))
        if candidates:
            text = max(candidates, key=len)
            if text and len(text) >= ARTICLE_MIN_CHARS:
                return text[:ARTICLE_MAX_CHARS], "BS4"
    except Exception:
        pass
    return "", "BS4_EMPTY"

def _extract_structured_data_text(html_text: str) -> tuple[str, str]:
    try:
        import json as _json
        texts = []
        for m in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html_text or "", re.I | re.S):
            raw = _html_unescape(m.group(1)).strip()
            try:
                data = _json.loads(raw)
            except Exception:
                continue
            objs = data if isinstance(data, list) else [data]
            for obj in objs:
                if not isinstance(obj, dict):
                    continue
                for k in ["articleBody", "description", "abstract"]:
                    v = obj.get(k)
                    if isinstance(v, str) and len(v) > 80:
                        texts.append(v)
        if texts:
            text = "\n".join(texts)
            return text[:ARTICLE_MAX_CHARS], "JSON_LD"
    except Exception:
        pass
    return "", "JSON_LD_EMPTY"

def _extract_html_best_text(html_text: str, url: str) -> tuple[str, str]:
    extractors = [
        lambda h: _extract_structured_data_text(h),
        lambda h: _extract_html_with_trafilatura(h, url),
        lambda h: _extract_html_with_readability(h),
        lambda h: _extract_html_with_bs4(h),
    ]
    for func in extractors:
        try:
            text, status = func(html_text)
            if text and len(text.strip()) >= ARTICLE_MIN_CHARS:
                return text.strip()[:ARTICLE_MAX_CHARS], status
        except Exception:
            pass

    meta = _extract_meta_description(html_text)
    stripped = _strip_html_to_text(html_text)
    if meta and len(meta) >= 80:
        if stripped and meta not in stripped[:600]:
            return (meta + "\n" + stripped)[:ARTICLE_MAX_CHARS], "META_PLUS_STRIPPED"
        return meta[:ARTICLE_MAX_CHARS], "META_DESCRIPTION"
    if stripped:
        return stripped[:ARTICLE_MAX_CHARS], "HTML_STRIPPED"
    return "", "HTML_EMPTY"

def fetch_article_body_for_ai(url: str) -> tuple[str, str]:
    """v7 override: robust article/PDF body extraction."""
    u = safe_url(url)
    if not u:
        return "", "NO_URL"

    raw, ctype, fetch_status = _fetch_url_bytes(u)
    if not raw:
        return "", fetch_status

    low_url = u.lower()
    low_ctype = (ctype or "").lower()

    if low_url.endswith(".pdf") or "application/pdf" in low_ctype:
        text, status = _extract_pdf_text_from_bytes(raw)
        if text:
            return text, status
        return "", status

    html_text = _decode_bytes(raw, ctype)
    if not html_text:
        return "", "DECODE_FAILED"

    text, status = _extract_html_best_text(html_text, u)

    lower_text = (text or "").lower()
    dynamic_markers = [
        "javascript", "enable cookies", "access denied", "captcha", "로그인", "권한이 없습니다",
        "통합검색", "페이지를 찾을 수", "browser does not support",
    ]
    if text and len(text) < ARTICLE_MIN_CHARS and any(m.lower() in lower_text for m in dynamic_markers):
        return text, f"BODY_TOO_SHORT_DYNAMIC:{status}"
    if text and len(text) >= 80:
        return text[:ARTICLE_MAX_CHARS], status
    return text, f"BODY_TOO_SHORT:{status}"

def _gti_step4_extractor_log_once():
    try:
        mods = []
        for m in ["trafilatura", "bs4", "readability", "pypdf", "PyPDF2"]:
            mods.append(f"{m}={'Y' if _optional_import(m) else 'N'}")
        log("Article extractor: " + ", ".join(mods))
    except Exception:
        pass

# ======================================================================
# End of GTI STEP4 Article Body Extraction Patch v7.0
# ======================================================================


# ======================================================================
# GTI STEP4 Gemini Call / Output Patch v8.0
# ----------------------------------------------------------------------
# v7 보완
# 1) Gemini 호출 실패 사유를 article_extract_status에 남김
# 2) responseMimeType 미지원/모델 오류에도 JSON 파싱 재시도
# 3) 모델 fallback: 환경변수 모델 → gemini-2.0-flash → gemini-1.5-flash
# 4) Gemini 결과가 정상일 때만 GEMINI_OK 표시, 아니면 FALLBACK_RULE_BODY + 오류 표시
# 5) Regulation 결과에도 article_extract_status 출력
# ======================================================================

GEMINI_MODEL_CANDIDATES = []
for _m in [
    os.getenv("GTI_GEMINI_MODEL", "").strip(),
    "gemini-2.0-flash",
    "gemini-1.5-flash",
]:
    if _m and _m not in GEMINI_MODEL_CANDIDATES:
        GEMINI_MODEL_CANDIDATES.append(_m)

_LAST_GEMINI_ERROR = ""

def _extract_json_object(text: str) -> dict:
    txt = clean(text)
    if not txt:
        return {}
    txt = re.sub(r"^```(?:json)?\s*", "", txt.strip(), flags=re.I)
    txt = re.sub(r"\s*```$", "", txt.strip())
    try:
        return json.loads(txt)
    except Exception:
        pass
    start = txt.find("{")
    end = txt.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(txt[start:end+1])
        except Exception:
            return {}
    return {}

def call_gemini_json(prompt: str) -> dict:
    """v8 override: robust Gemini call with error details and model fallback."""
    global _LAST_GEMINI_ERROR
    _LAST_GEMINI_ERROR = ""

    if not USE_GEMINI:
        _LAST_GEMINI_ERROR = "DISABLED"
        return {"_error": _LAST_GEMINI_ERROR}
    if not GEMINI_API_KEY:
        _LAST_GEMINI_ERROR = "NO_API_KEY"
        return {"_error": _LAST_GEMINI_ERROR}

    last_error = ""
    for model in GEMINI_MODEL_CANDIDATES:
        endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}"
        payloads = [
            {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.1,
                    "topP": 0.8,
                    "maxOutputTokens": 1600,
                    "responseMimeType": "application/json",
                },
            },
            {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.1,
                    "topP": 0.8,
                    "maxOutputTokens": 1600,
                },
            },
        ]
        for idx, payload in enumerate(payloads, start=1):
            try:
                data = json.dumps(payload).encode("utf-8")
                req = urllib.request.Request(endpoint, data=data, headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=60) as resp:
                    raw = resp.read().decode("utf-8", "ignore")
                out = json.loads(raw)

                if "error" in out:
                    msg = out.get("error", {}).get("message", str(out.get("error")))
                    last_error = f"{model}/payload{idx}:API_ERROR:{msg[:250]}"
                    continue

                cand = (out.get("candidates") or [{}])[0]
                finish = cand.get("finishReason", "")
                parts = cand.get("content", {}).get("parts", [])
                text = "\n".join(clean(p.get("text", "")) for p in parts if isinstance(p, dict))
                parsed = _extract_json_object(text)
                if parsed:
                    parsed["_gemini_model"] = model
                    parsed["_gemini_finish"] = finish
                    _LAST_GEMINI_ERROR = ""
                    return parsed
                last_error = f"{model}/payload{idx}:NO_JSON finish={finish} text={text[:180]}"
            except Exception as exc:
                last_error = f"{model}/payload{idx}:{type(exc).__name__}:{str(exc)[:250]}"

    _LAST_GEMINI_ERROR = last_error or "UNKNOWN_GEMINI_ERROR"
    return {"_error": _LAST_GEMINI_ERROR}

def _is_useful_gemini_result(summary: str, ai: str, action_plan: str, headline: str) -> bool:
    if _looks_like_title_only(summary, headline):
        return False
    if not summary or len(summary) < 30:
        return False
    if not ai or len(ai) < 80:
        return False
    if not action_plan or len(action_plan) < 50:
        return False
    bad_ai = [
        "관련 뉴스입니다",
        "공식 규제/공지 후보입니다",
        "본문 확인 불가로 정밀 영향 분석이 제한됩니다",
        "원문 본문 기반 세부 영향은 Summary 내용을 기준으로",
    ]
    if any(x in ai for x in bad_ai):
        return False
    return True

def build_gti_ai_analysis(row: pd.Series, *, headline: str, url: str, issue: str, impact: str, products_text: str, default_action: str, content_type: str) -> dict:
    """v8 override: Gemini first, useful fallback second, with diagnostic status."""
    body, body_status = _fallback_source_body(row, headline)
    if not body:
        body, body_status = fetch_article_body_for_ai(url)

    cache = _ensure_gemini_cache()
    key = _analysis_cache_key(url, headline)
    cached = cache.get(key)
    if cached and not _is_bad_cached_analysis(cached, headline):
        return cached

    body_for_prompt = body if body else f"본문 확보 실패. Headline: {headline}. URL: {url}"

    prompt = f"""
당신은 삼성전자 본사 관세/통상 리스크 분석가입니다.
아래 원문을 읽고 GTI Radar 임원보고용으로 분석하십시오.

절대 금지:
- 제목 반복 금지
- "관련 뉴스입니다", "공식 규제/공지 후보입니다" 같은 템플릿 문장 금지
- 본문에 없는 세율/HS/국가/시행일을 지어내지 말 것
- 본문을 읽을 수 없으면 Summary에 "본문 확인 불가"라고 명시

반드시 아래 JSON 형식만 출력하십시오:
{{
  "Summary": "원문 기준 게시물 요약 2~3줄",
  "AI Analysis": "삼성전자 관세업무 영향. 수입통관/수출통관/FTA·원산지/HS/관세비용/수출통제 중 해당 항목을 구체적으로 설명",
  "Action Plan": "즉시조치/1주 내/1개월 내/Owner 형식의 구체적 대응방안",
  "ExecutiveMessage": "임원용 한 문단 핵심 메시지"
}}

기본 정보:
- Content Type: {content_type}
- Issue: {issue}
- Samsung Impact: {impact}
- Affected Products: {products_text}
- URL: {url}
- Headline: {headline}
- Default Action Hint: {default_action}

원문:
{body_for_prompt[:ARTICLE_MAX_CHARS]}
""".strip()

    result = call_gemini_json(prompt)
    if result and not result.get("_error"):
        summary = clean(result.get("Summary", ""))
        ai = clean(result.get("AI Analysis", ""))
        action_plan = clean(result.get("Action Plan", ""))
        executive = clean(result.get("ExecutiveMessage", ""))

        if _is_useful_gemini_result(summary, ai, action_plan, headline):
            final = {
                "Summary": summary[:900],
                "AI Analysis": ai[:1400],
                "Action Plan": action_plan[:1400],
                "ExecutiveMessage": (executive or summary)[:800],
                "article_extract_status": f"GEMINI_OK|model={result.get('_gemini_model','')}|body={body_status}",
            }
            cache[key] = final
            _save_gemini_cache()
            return final

    final = _fallback_gti_analysis_from_body(
        body=body,
        headline=headline,
        issue=issue,
        impact=impact,
        products_text=products_text,
        default_action=default_action,
        content_type=content_type,
    )
    gem_err = clean(result.get("_error", "")) if isinstance(result, dict) else clean(_LAST_GEMINI_ERROR)
    final["article_extract_status"] = f"FALLBACK_RULE_BODY|body={body_status}|gemini_error={gem_err[:220]}|api_key={'Y' if GEMINI_API_KEY else 'N'}"
    cache[key] = final
    _save_gemini_cache()
    return final

def _ensure_output_cols_article_status():
    try:
        for col in ["article_extract_status", "ExecutiveMessage"]:
            if "OUTPUT_COLS" in globals() and col not in OUTPUT_COLS:
                OUTPUT_COLS.append(col)
    except Exception:
        pass

_ensure_output_cols_article_status()

# ======================================================================
# End of GTI STEP4 Gemini Call / Output Patch v8.0
# ======================================================================


# ======================================================================
# GTI STEP4 Article Cleanup Patch v9.0
# ----------------------------------------------------------------------
# 보완사항
# 1) 언론사 UI 문구 제거: 글자크기/이전기사/다음기사/공유/스크롤 등
# 2) Google 뉴스/제목 수준 요약 차단
# 3) routine 환율공지/농수산물/일반산업 기사의 과대 분석 방지
# 4) Gemini 입력 전 본문 정제
# ======================================================================

ARTICLE_UI_NOISE_PATTERNS = [
    "이전 기사보기", "다음 기사보기", "기사의 본문 내용은 이 글자크기로 변경됩니다",
    "본문 글씨 키우기", "본문 글씨 줄이기", "스크롤 이동 상태바", "바로가기 복사하기",
    "공유 이메일에 공유하기", "카카오톡에 공유하기", "페이스북에 공유하기", "트위터에 공유하기",
    "링크 복사하기", "닫기", "번역 ENG JPN CHN", "편의기능", "AI기능", "추천질문",
    "관련종목", "AI해설", "에디터 픽", "추천기사", "본문영역", "기사원문",
    "댓글 0", "인쇄", "즐겨찾기", "가 가",
]

LOW_VALUE_ARTICLE_TERMS = [
    "rate of exchange", "exchange rate", "과세환율", "wheat", "밀 수출", "새우", "라이스페이퍼",
    "염소산업", "혈통관리", "스포츠", "맛집", "여행", "주가", "증시", "브랜드",
]

def clean_article_text_for_gti(text: str) -> str:
    t = clean(text)
    if not t:
        return ""
    t = re.sub(r"\s+", " ", t).strip()
    for p in ARTICLE_UI_NOISE_PATTERNS:
        t = t.replace(p, " ")
    # Remove short UI lines/fragments
    t = re.sub(r"(?<![가-힣])가\s+가(?![가-힣])", " ", t)
    t = re.sub(r"(공유|닫기|인쇄|즐겨찾기|댓글|추천기사|AI해설|관련종목)\s*", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def article_text_quality(text: str, headline: str) -> str:
    t = clean_article_text_for_gti(text)
    h = clean(headline)
    if not t:
        return "EMPTY"
    if t in {"Google 뉴스", "Google News"}:
        return "GOOGLE_NEWS_ONLY"
    if h and (t == h or t.replace(" ", "") == h.replace(" ", "")):
        return "TITLE_ONLY"
    if len(t) < 120:
        return "TOO_SHORT"
    if any(p in text for p in ARTICLE_UI_NOISE_PATTERNS) and len(t) < 250:
        return "UI_NOISE_ONLY"
    return "OK"

def _fallback_source_body(row: pd.Series, headline: str) -> tuple[str, str]:
    """v9 override: use input body only when it is not UI garbage."""
    for col in [
        "article_body", "regulation_fallback_body", "full_text", "FullText",
        "content", "Content", "body", "Body", "Summary", "AI Analysis",
        "ClusterHeadlines", "description", "Description",
    ]:
        raw = clean(row.get(col, ""))
        val = clean_article_text_for_gti(raw)
        quality = article_text_quality(val, headline)
        if val and quality == "OK":
            return val[:ARTICLE_MAX_CHARS], f"INPUT_COLUMN:{col}"
    return "", "NO_INPUT_BODY"

def _simple_body_summary(body: str, headline: str) -> str:
    body = clean_article_text_for_gti(body)
    if not body:
        return "본문 확인 불가: 원문 URL에서 본문을 가져오지 못했습니다. 제목만으로 요약하지 않았습니다."
    quality = article_text_quality(body, headline)
    if quality != "OK":
        return f"본문 확인 불가: 본문 추출 결과가 {quality} 상태입니다. 제목만으로 요약하지 않았습니다."
    text = re.sub(r"\s+", " ", body).strip()
    parts = re.split(r"(?<=[.!?。？！])\s+|(?<=다\.)\s+|(?<=니다\.)\s+", text)
    parts = [clean_article_text_for_gti(p.strip()) for p in parts if clean_article_text_for_gti(p.strip())]
    parts = [p for p in parts if not _looks_like_title_only(p, headline) and len(p) >= 25]
    if not parts:
        return text[:350]
    return " ".join(parts[:3])[:700]

def fetch_article_body_for_ai(url: str) -> tuple[str, str]:
    """v9 override: call existing extractor if available, then clean UI noise."""
    # Use v7 extractor internals if present.
    try:
        raw, ctype, fetch_status = _fetch_url_bytes(safe_url(url))
        if not raw:
            return "", fetch_status
        low_url = safe_url(url).lower()
        low_ctype = (ctype or "").lower()
        if low_url.endswith(".pdf") or "application/pdf" in low_ctype:
            text, status = _extract_pdf_text_from_bytes(raw)
            return clean_article_text_for_gti(text), status
        html_text = _decode_bytes(raw, ctype)
        text, status = _extract_html_best_text(html_text, safe_url(url))
        cleaned = clean_article_text_for_gti(text)
        q = article_text_quality(cleaned, "")
        if cleaned and q == "OK":
            return cleaned[:ARTICLE_MAX_CHARS], status
        return cleaned, f"BODY_BAD_QUALITY:{q}:{status}"
    except Exception:
        # fallback to old simple fetch if v7 helpers are not available
        try:
            u = safe_url(url)
            req = urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0", "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8"})
            ctx = ssl.create_default_context()
            with urllib.request.urlopen(req, timeout=ARTICLE_FETCH_TIMEOUT, context=ctx) as resp:
                raw = resp.read(2_000_000)
                ctype = resp.headers.get("Content-Type", "")
            html_text = _decode_bytes(raw, ctype) if "_decode_bytes" in globals() else raw.decode("utf-8", "ignore")
            text = _strip_html_to_text(html_text)
            cleaned = clean_article_text_for_gti(text)
            return cleaned[:ARTICLE_MAX_CHARS], "HTML_STRIPPED_CLEANED"
        except Exception as exc:
            return "", f"FETCH_FAILED:{type(exc).__name__}"

def is_low_value_for_ai(headline: str, body: str, issue: str) -> bool:
    blob = f"{headline} {body} {issue}".lower()
    return any(k.lower() in blob for k in LOW_VALUE_ARTICLE_TERMS)

def _fallback_gti_analysis_from_body(*, body: str, headline: str, issue: str, impact: str, products_text: str, default_action: str, content_type: str) -> dict:
    """v9 override: avoid over-analysis for routine/low-value items."""
    body = clean_article_text_for_gti(body)
    summary = _simple_body_summary(body, headline)
    issue_l = clean(issue)

    if is_low_value_for_ai(headline, body, issue_l):
        ai = (
            "Reference 수준의 참고 정보입니다. 삼성전자 주요 제품·부품·원재료 또는 관세비용에 직접 영향을 주는 "
            "HS·세율·원산지·수출통제 변경은 확인되지 않았습니다."
        )
        action = "즉시조치 불필요. 동일 국가에서 전자부품·전략물자·관세율 관련 후속 공지가 있을 경우 재검토하십시오."
        return {
            "Summary": summary[:900],
            "AI Analysis": ai,
            "Action Plan": action,
            "ExecutiveMessage": summary[:300],
            "article_extract_status": "FALLBACK_REFERENCE_LOW_VALUE",
        }

    # call previous rule function body by reimplementing concise issue-specific fallback
    terms_text = f"{headline} {body}"
    rates = "; ".join(sorted(set(re.findall(r"\b\d{1,3}(?:\.\d+)?\s*%", terms_text)))[:6]) or "본문에서 확인 불가"

    if issue_l in {"AD/CVD", "반덤핑/상계관세", "AD_CVD"}:
        ai = f"반덤핑/상계관세 이슈입니다. 확인된 세율 정보는 {rates}입니다. 대상 HS·공급국·벤더 기준 수입실적을 매칭하여 추가관세 비용과 원산지/가격자료 방어 리스크를 점검해야 합니다."
        action = "즉시조치: 대상 HS·공급국·벤더 매핑. 1주 내: 최근 12개월 수입금액 기준 잠재 비용 산출. 1개월 내: 원산지·가격자료 방어파일 구축. Owner: HQ Customs + 구매 + 해당 법인"
    elif issue_l in {"수출통제", "EXPORT_CONTROL"}:
        ai = "수출통제 이슈입니다. AI·반도체·전략기술 또는 관련 서비스 접근 제한이 수출통제 범위로 확대될 가능성이 있어 ECCN/전략물자 분류와 최종사용자 스크리닝이 필요합니다."
        action = "즉시조치: 대상 기술/제품의 수출통제 해당 여부 확인. 1주 내: 거래처·목적지 스크리닝. 1개월 내: Item Master Export Control Flag 반영. Owner: HQ Export Control"
    elif issue_l in {"CBAM", "CBAM_CARBON"}:
        ai = "CBAM 이슈입니다. EU향 품목의 내재배출량 자료, 공급사 데이터, 인증서 비용 반영 여부가 관세·준조세 비용 리스크로 연결될 수 있습니다."
        action = "즉시조치: EU향 대상품목 및 공급사 배출량 자료 확인. 1주 내: CN/HS별 Gap List 작성. 1개월 내: CBAM 비용 산정 프로세스 반영. Owner: HQ Customs + ESG"
    elif issue_l in {"FTA/원산지", "ORIGIN_FTA"}:
        ai = "FTA/원산지 이슈입니다. 협정 적용 가능성, CO 발급/수취 요건, BOM 원산지 및 FTA Master 정합성 점검이 필요합니다."
        action = "즉시조치: 대상 협정·품목·법인 확인. 1주 내: BOM/Vendor 원산지확인서/HS 일치 점검. 1개월 내: FTA Master 업데이트. Owner: HQ Customs/FTA"
    else:
        ai = f"{issue_l} 이슈입니다. 삼성전자 관세업무 관점에서는 대상 국가·품목·HS·세율·시행일 기준으로 수입통관, 수출통관, FTA/원산지, 관세비용 영향 여부를 확인해야 합니다."
        action = f"즉시조치: 원문 기준 대상 국가·품목·시행일 확인. 1주 내: 관련 법인 수입/수출 실적 매칭. Owner: {default_action}"

    return {
        "Summary": summary[:900],
        "AI Analysis": ai[:1200],
        "Action Plan": action[:1200],
        "ExecutiveMessage": (summary[:220] + " " + ai[:240])[:700],
        "article_extract_status": "FALLBACK_RULE_BODY_CLEANED",
    }

def build_gti_ai_analysis(row: pd.Series, *, headline: str, url: str, issue: str, impact: str, products_text: str, default_action: str, content_type: str) -> dict:
    """v9 override: clean body before Gemini and prevent garbage summaries."""
    body, body_status = _fallback_source_body(row, headline)
    if not body:
        body, body_status = fetch_article_body_for_ai(url)
    body = clean_article_text_for_gti(body)
    q = article_text_quality(body, headline)

    cache = _ensure_gemini_cache()
    key = _analysis_cache_key(url, headline)
    cached = cache.get(key)
    if cached and not _is_bad_cached_analysis(cached, headline):
        # Do not reuse cached output with UI garbage.
        if "글자크기" not in clean(cached.get("Summary", "")) and "이전 기사보기" not in clean(cached.get("Summary", "")):
            return cached

    body_for_prompt = body if body and q == "OK" else f"본문 추출 품질 불량({q}). Headline: {headline}. URL: {url}"

    prompt = f"""
당신은 삼성전자 본사 관세/통상 리스크 분석가입니다.
아래 원문을 읽고 GTI Radar 임원보고용으로 분석하십시오.

절대 금지:
- 제목 반복 금지
- 글자크기/이전기사/다음기사/공유/스크롤 같은 웹페이지 UI 문구 출력 금지
- "관련 뉴스입니다", "공식 규제/공지 후보입니다" 같은 템플릿 문장 금지
- 본문에 없는 세율/HS/국가/시행일을 지어내지 말 것
- 본문을 읽을 수 없으면 Summary에 "본문 확인 불가"라고 명시

JSON만 출력:
{{
  "Summary": "원문 기준 게시물 요약 2~3줄",
  "AI Analysis": "삼성전자 관세업무 영향. 수입통관/수출통관/FTA·원산지/HS/관세비용/수출통제 중 해당 항목을 구체적으로 설명",
  "Action Plan": "즉시조치/1주 내/1개월 내/Owner 형식의 구체적 대응방안",
  "ExecutiveMessage": "임원용 한 문단 핵심 메시지"
}}

기본 정보:
- Content Type: {content_type}
- Issue: {issue}
- Samsung Impact: {impact}
- Affected Products: {products_text}
- URL: {url}
- Headline: {headline}
- Default Action Hint: {default_action}

원문:
{body_for_prompt[:ARTICLE_MAX_CHARS]}
""".strip()

    result = call_gemini_json(prompt)
    if result and not result.get("_error"):
        summary = clean_article_text_for_gti(result.get("Summary", ""))
        ai = clean_article_text_for_gti(result.get("AI Analysis", ""))
        action_plan = clean_article_text_for_gti(result.get("Action Plan", ""))
        executive = clean_article_text_for_gti(result.get("ExecutiveMessage", ""))
        if summary and len(summary) >= 30 and ai and len(ai) >= 80 and "글자크기" not in summary and "이전 기사보기" not in summary:
            final = {
                "Summary": summary[:900],
                "AI Analysis": ai[:1400],
                "Action Plan": action_plan[:1400] if action_plan else default_action,
                "ExecutiveMessage": (executive or summary)[:800],
                "article_extract_status": f"GEMINI_OK|body={body_status}|quality={q}",
            }
            cache[key] = final
            _save_gemini_cache()
            return final

    final = _fallback_gti_analysis_from_body(
        body=body,
        headline=headline,
        issue=issue,
        impact=impact,
        products_text=products_text,
        default_action=default_action,
        content_type=content_type,
    )
    gem_err = clean(result.get("_error", "")) if isinstance(result, dict) else ""
    final["article_extract_status"] = f"{final.get('article_extract_status')}|body={body_status}|quality={q}|gemini_error={gem_err[:180]}"
    cache[key] = final
    _save_gemini_cache()
    return final

# ======================================================================
# End of GTI STEP4 Article Cleanup Patch v9.0
# ======================================================================


# ======================================================================
# GTI STEP4 Gemini Endpoint & Selection Patch v10.0
# ----------------------------------------------------------------------
# 보완사항
# 1) Gemini 404 대응: v1beta/v1 endpoint + 최신 모델 후보 자동 시도
# 2) Gemini 오류 전체를 article_extract_status에 기록
# 3) Google News only / 본문 부족 결과는 selected에서 제외 또는 후순위
# 4) 동일 이슈 반복 기사 중복 축소
# ======================================================================

GEMINI_API_VERSIONS = []
for _v in [os.getenv("GTI_GEMINI_API_VERSION", "").strip(), "v1beta", "v1"]:
    if _v and _v not in GEMINI_API_VERSIONS:
        GEMINI_API_VERSIONS.append(_v)

GEMINI_MODEL_CANDIDATES = []
for _m in [
    os.getenv("GTI_GEMINI_MODEL", "").strip(),
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.0-flash",
    "gemini-2.0-flash-001",
    "gemini-1.5-flash-latest",
    "gemini-1.5-pro-latest",
    "gemini-1.5-flash",
]:
    if _m and _m not in GEMINI_MODEL_CANDIDATES:
        GEMINI_MODEL_CANDIDATES.append(_m)

_LAST_GEMINI_ERROR = ""

def _extract_json_object(text: str) -> dict:
    txt = clean(text)
    if not txt:
        return {}
    txt = re.sub(r"^```(?:json)?\s*", "", txt.strip(), flags=re.I)
    txt = re.sub(r"\s*```$", "", txt.strip())
    try:
        return json.loads(txt)
    except Exception:
        pass
    start = txt.find("{")
    end = txt.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(txt[start:end+1])
        except Exception:
            return {}
    return {}

def call_gemini_json(prompt: str) -> dict:
    """v10 override: try API versions + current Gemini model names."""
    global _LAST_GEMINI_ERROR
    _LAST_GEMINI_ERROR = ""

    if not USE_GEMINI:
        _LAST_GEMINI_ERROR = "DISABLED"
        return {"_error": _LAST_GEMINI_ERROR}
    if not GEMINI_API_KEY:
        _LAST_GEMINI_ERROR = "NO_API_KEY"
        return {"_error": _LAST_GEMINI_ERROR}

    errors = []
    payloads = [
        lambda: {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "topP": 0.8,
                "maxOutputTokens": 1600,
                "responseMimeType": "application/json",
            },
        },
        lambda: {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "topP": 0.8,
                "maxOutputTokens": 1600,
            },
        },
    ]

    for api_ver in GEMINI_API_VERSIONS:
        for model in GEMINI_MODEL_CANDIDATES:
            endpoint = f"https://generativelanguage.googleapis.com/{api_ver}/models/{model}:generateContent?key={GEMINI_API_KEY}"
            for pidx, make_payload in enumerate(payloads, start=1):
                try:
                    payload = make_payload()
                    data = json.dumps(payload).encode("utf-8")
                    req = urllib.request.Request(endpoint, data=data, headers={"Content-Type": "application/json"}, method="POST")
                    with urllib.request.urlopen(req, timeout=60) as resp:
                        raw = resp.read().decode("utf-8", "ignore")
                    out = json.loads(raw)

                    if "error" in out:
                        msg = out.get("error", {}).get("message", str(out.get("error")))
                        errors.append(f"{api_ver}/{model}/p{pidx}:API_ERROR:{msg[:180]}")
                        continue

                    candidates = out.get("candidates") or []
                    if not candidates:
                        errors.append(f"{api_ver}/{model}/p{pidx}:NO_CANDIDATE")
                        continue

                    cand = candidates[0]
                    finish = cand.get("finishReason", "")
                    parts = cand.get("content", {}).get("parts", [])
                    text = "\n".join(clean(part.get("text", "")) for part in parts if isinstance(part, dict))
                    parsed = _extract_json_object(text)
                    if parsed:
                        parsed["_gemini_model"] = model
                        parsed["_gemini_api_version"] = api_ver
                        parsed["_gemini_finish"] = finish
                        _LAST_GEMINI_ERROR = ""
                        return parsed

                    errors.append(f"{api_ver}/{model}/p{pidx}:NO_JSON finish={finish} text={text[:120]}")
                except urllib.error.HTTPError as exc:
                    try:
                        err_body = exc.read().decode("utf-8", "ignore")[:180]
                    except Exception:
                        err_body = ""
                    errors.append(f"{api_ver}/{model}/p{pidx}:HTTP{exc.code}:{err_body}")
                except Exception as exc:
                    errors.append(f"{api_ver}/{model}/p{pidx}:{type(exc).__name__}:{str(exc)[:180]}")

    _LAST_GEMINI_ERROR = " | ".join(errors[-8:]) if errors else "UNKNOWN_GEMINI_ERROR"
    return {"_error": _LAST_GEMINI_ERROR}

def _bad_selected_body_status(row: pd.Series) -> bool:
    st = clean(row.get("article_extract_status", ""))
    s = clean(row.get("Summary", ""))
    bad = [
        "GOOGLE_NEWS_ONLY", "TITLE_ONLY", "TOO_SHORT", "UI_NOISE_ONLY",
        "본문 확인 불가", "글자크기", "이전 기사보기", "다음 기사보기",
    ]
    return any(x in st or x in s for x in bad)

def _similar_news_key(row: pd.Series) -> str:
    title = clean(row.get("Headline", "")).lower()
    topic = clean(row.get("topic", ""))
    if any(k in title for k in ["anthropic", "앤트로픽", "미토스", "mythos", "페이블", "fable"]):
        return "EXPORT_CONTROL_ANTHROPIC_AI_MODEL"
    if any(k in title for k in ["cbam", "탄소국경"]):
        return "CBAM"
    if any(k in title for k in ["anti-dumping", "antidumping", "덤핑방지", "반덤핑"]):
        return "AD_CVD"
    title = re.sub(r"[-–—|].*$", "", title)
    title = re.sub(r"[^a-z0-9가-힣]+", " ", title)
    return f"{topic}|{title[:80].strip()}"

def _postprocess_daily_quality(daily: pd.DataFrame, audit: pd.DataFrame, excluded: pd.DataFrame):
    """v10: reduce poor-body and duplicate articles in selected daily."""
    if daily is None or daily.empty:
        return daily, audit, excluded

    d = daily.copy()
    if "article_extract_status" not in d.columns:
        d["article_extract_status"] = ""
    if "Summary" not in d.columns:
        d["Summary"] = ""

    d["_bad_body"] = d.apply(_bad_selected_body_status, axis=1)
    d["_sim_key"] = d.apply(_similar_news_key, axis=1)

    sort_cols, asc = [], []
    for col, ascending in [("_bad_body", True), ("final_score", False), ("samsung_impact_score", False), ("urgency_score", False)]:
        if col in d.columns:
            sort_cols.append(col); asc.append(ascending)
    if sort_cols:
        d = d.sort_values(sort_cols, ascending=asc)

    kept = []
    key_counts = {}
    for _, r in d.iterrows():
        key = clean(r.get("_sim_key"))
        bad = bool(r.get("_bad_body"))
        max_per_key = 1 if bad else (2 if key == "EXPORT_CONTROL_ANTHROPIC_AI_MODEL" else 1)
        if key_counts.get(key, 0) >= max_per_key:
            continue
        kept.append(r)
        key_counts[key] = key_counts.get(key, 0) + 1

    out = pd.DataFrame(kept).drop(columns=["_bad_body", "_sim_key"], errors="ignore")
    out = out.head(NEWS_TARGET_MAX).reset_index(drop=True)
    if "rank" in out.columns:
        out["rank"] = range(1, len(out) + 1)
    return out, audit, excluded

try:
    _ORIGINAL_BUILD_V10 = build
    def build(df: pd.DataFrame):
        daily, audit, excluded = _ORIGINAL_BUILD_V10(df)
        return _postprocess_daily_quality(daily, audit, excluded)
except Exception:
    pass

def _gti_step4_v10_log_once():
    try:
        log(f"Gemini API versions: {GEMINI_API_VERSIONS}")
        log(f"Gemini model candidates v10: {GEMINI_MODEL_CANDIDATES}")
    except Exception:
        pass

# ======================================================================
# End of GTI STEP4 Gemini Endpoint & Selection Patch v10.0
# ======================================================================

# ======================================================================
# GTI STEP4-2 News Sensing Patch v11 - 2026-06-14
# ----------------------------------------------------------------------
# 목적: 네이버/구글/RSS 뉴스 중 삼성전자 관세업무 관련성이 있는 뉴스를
#      30건 이하로 선별한다.
# 점수 기준:
# - 관세/통상 법규성 뉴스 30%
# - 관세/통상 정책 변화 20%
# - 당사 관세업무 직접영향 40%
# - 당사 관세업무 간접영향 10%
# 기본조건:
# - Publish Date 보존
# - 원문 URL 유지
# - UI 문구 제거
# - 동일 이슈/동일 URL 중복 제거
# ======================================================================

NEWS_TARGET_MAX = min(int(globals().get("NEWS_TARGET_MAX", 30)), 30)
NEWS_TARGET_MIN = min(int(globals().get("NEWS_TARGET_MIN", 30)), NEWS_TARGET_MAX)

try:
    if "Publish Date" not in LEGACY_COLS:
        LEGACY_COLS.insert(LEGACY_COLS.index("Date") + 1, "Publish Date")
except Exception:
    pass


_UI_NOISE_V11 = [
    "이전 기사보기", "다음 기사보기", "기사의 본문 내용은 이 글자크기로 변경됩니다",
    "본문 글씨 키우기", "본문 글씨 줄이기", "스크롤 이동 상태바", "가 가",
    "바로가기 복사하기", "공유하기", "관련기사", "추천기사", "본문영역", "기사원문",
]


def remove_ui_noise_v11(text: str) -> str:
    t = clean(text)
    for phrase in _UI_NOISE_V11:
        t = t.replace(phrase, " ")
    t = re.sub(r"\b[가]\s+[가]\b", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _news_blob_v11(row: pd.Series) -> str:
    return " ".join(clean(row.get(c, "")) for c in [
        "Headline", "Summary", "AI Analysis", "Action Plan", "topic_keyword",
        "topic_reason", "KeywordMatches", "Country", "Agency", "Source"
    ]).lower()


def _bad_news_url_v11(url: str) -> bool:
    low = clean(url).lower()
    return not low or "news.google.com" in low or low in {
        "https://google.com", "https://www.google.com", "https://news.google.com", "https://news.google.com/",
    }


def _direct_impact_score_v11(row: pd.Series) -> int:
    blob = _news_blob_v11(row)
    if clean(row.get("samsung_impact")).lower() == "direct":
        return 100
    if any(k in blob for k in ["samsung", "삼성전자", "삼성", "sec/hq"]):
        if any(k in blob for k in ["tariff", "관세", "customs", "통관", "export control", "수출통제", "fta", "origin", "원산지", "cbam", "anti-dumping", "덤핑"]):
            return 90
    if any(k in blob for k in ["semiconductor", "반도체", "battery", "배터리", "display", "oled", "steel", "철강", "rare earth", "희토류", "smartphone", "mobile", "가전"]):
        return 70
    return 20


def _indirect_impact_score_v11(row: pd.Series) -> int:
    blob = _news_blob_v11(row)
    if any(k in blob for k in ["china", "중국", "vietnam", "베트남", "india", "인도", "mexico", "멕시코", "eu", "미국", "usa", "korea", "한국"]):
        return 65
    if any(k in blob for k in ["supply chain", "공급망", "raw material", "원자재", "component", "부품"]):
        return 60
    return 20


def _law_news_score_v11(row: pd.Series) -> int:
    blob = _news_blob_v11(row)
    return 100 if any(k in blob for k in [
        "law", "regulation", "rule", "notice", "federal register", "관보", "고시", "공고", "법령",
        "anti-dumping", "antidumping", "countervailing", "ad/cvd", "반덤핑", "상계관세",
    ]) else 0


def _policy_news_score_v11(row: pd.Series) -> int:
    blob = _news_blob_v11(row)
    return 100 if any(k in blob for k in [
        "tariff", "관세", "quota", "쿼터", "customs", "통관", "fta", "cepa", "origin", "원산지",
        "export control", "수출통제", "entity list", "forced labor", "uflpa", "cbam", "section 301", "section 232",
    ]) else 0


def _hard_news_reference_v11(row: pd.Series) -> bool:
    blob = _news_blob_v11(row)
    if any(k in blob for k in ["글자크기", "이전 기사보기", "다음 기사보기", "스크롤 이동 상태바"]):
        return True
    weak = ["신간", "서평", "bookreview", "문화", "주가", "증시", "코스피", "환율", "금리", "부동산", "미토스", "주술", "혈통관리"]
    policy = ["관세", "통관", "fta", "원산지", "cbam", "수출통제", "anti-dumping", "덤핑", "상계관세", "tariff", "customs"]
    return any(k in blob for k in weak) and not any(k in blob for k in policy)


def _sensing_score_v11(row: pd.Series) -> float:
    score = (
        _law_news_score_v11(row) * 0.30
        + _policy_news_score_v11(row) * 0.20
        + _direct_impact_score_v11(row) * 0.40
        + _indirect_impact_score_v11(row) * 0.10
    )
    score += min(10, safe_int(row.get("final_score", 0)) / 10) if "safe_int" in globals() else 0
    if _bad_news_url_v11(row.get("BestLinkURL", row.get("URL", ""))):
        score -= 80
    if _hard_news_reference_v11(row):
        score -= 100
    return score


def normalize_title_for_cluster(title: str) -> str:
    t = clean(title).lower()
    t = re.sub(r"\([^)]*\)|\[[^]]*\]", " ", t)
    t = re.sub(r"[-|].*$", " ", t)
    t = re.sub(r"[^0-9a-z가-힣]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _issue_key_v11_news(row: pd.Series) -> str:
    blob = _news_blob_v11(row)
    topic = clean(row.get("topic", row.get("issue_type", ""))).upper()
    if any(k in blob for k in ["anti-dumping", "antidumping", "countervailing", "반덤핑", "상계관세"]):
        return "AD_CVD:" + normalize_title_for_cluster(clean(row.get("Headline", "")))[:60]
    if "cbam" in blob:
        return "CBAM:" + normalize_title_for_cluster(clean(row.get("Headline", "")))[:60]
    if any(k in blob for k in ["fta", "cepa", "origin", "원산지"]):
        return "FTA_ORIGIN:" + normalize_title_for_cluster(clean(row.get("Headline", "")))[:60]
    if any(k in blob for k in ["export control", "entity list", "수출통제", "forced labor", "uflpa"]):
        return "EXPORT_CONTROL:" + normalize_title_for_cluster(clean(row.get("Headline", "")))[:60]
    return f"{topic}:{normalize_title_for_cluster(clean(row.get('Headline', '')))[:80]}"


try:
    _ORIGINAL_BUILD_V11_NEWS = build

    def build(df: pd.DataFrame):
        daily, excluded, audit = _ORIGINAL_BUILD_V11_NEWS(df)
        if audit is None or audit.empty:
            return daily, excluded, audit

        a = audit.copy()
        if "BestLinkURL" not in a.columns:
            a["BestLinkURL"] = a.get("URL", "")
        for col in ["Summary", "AI Analysis", "Action Plan"]:
            if col in a.columns:
                a[col] = a[col].apply(remove_ui_noise_v11)
        a["_sensing_score_v11"] = a.apply(_sensing_score_v11, axis=1)
        a["_issue_key_v11"] = a.apply(_issue_key_v11_news, axis=1)
        a = a[~a["BestLinkURL"].apply(_bad_news_url_v11)].copy()
        a = a.sort_values(["_sensing_score_v11", "Date"], ascending=[False, False])
        a = a.drop_duplicates(subset=["BestLinkURL"], keep="first")
        a = a.drop_duplicates(subset=["_issue_key_v11"], keep="first")
        selected = a.head(NEWS_TARGET_MAX).copy()
        selected["selected"] = "Y"
        selected["final_score"] = selected["_sensing_score_v11"].round().astype(int)
        selected["priority_group"] = selected["final_score"].apply(lambda v: "CORE" if v >= 75 else "USABLE")
        selected["RejectReason"] = selected.get("RejectReason", "").fillna("").astype(str)

        selected_keys = set(selected["BestLinkURL"].fillna("").astype(str).str.lower().str.strip())
        audit = audit.copy()
        audit["selected"] = audit.apply(lambda r: "Y" if clean(r.get("BestLinkURL", r.get("URL", ""))).lower().strip() in selected_keys else "N", axis=1)
        excluded = audit[audit["selected"].ne("Y")].copy()
        daily = selected.drop(columns=["_sensing_score_v11", "_issue_key_v11"], errors="ignore").reset_index(drop=True)
        return daily, excluded, audit
except Exception:
    pass


def to_legacy(daily: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for i, r in daily.reset_index(drop=True).iterrows():
        pub_date = _v12_publish_date(r)
        impact_value = clean(r.get("samsung_impact")) or clean(r.get("Samsung Impact")) or "Watch"
        if impact_value == "Reference":
            impact_value = "Watch"
        rows.append({
            "No": i + 1,
            "Content Type": "News",
            "Mail Group": "News - 주요",
            "Samsung Impact": impact_value,
            "Affected Subsidiary": clean(r.get("affected_subsidiary")) or "관련 법인 검토",
            "Impact Reason": clean(r.get("samsung_reason")),
            "Date": pub_date,
            "Publish Date": pub_date,
            "Headline": clean(r.get("Headline")),
            "Summary": remove_ui_noise_v11(r.get("Summary")),
            "AI Analysis": remove_ui_noise_v11(r.get("AI Analysis")),
            "Action Plan": remove_ui_noise_v11(r.get("Action Plan")),
            "Country": clean(r.get("Country")),
            "Agency": clean(r.get("Agency")),
            "Risk": clean(r.get("Risk")),
            "Importance Score": int(float(r.get("final_score", 0) or 0)),
            "Priority Group": clean(r.get("priority_group")),
            "Issue": clean(r.get("topic_keyword")),
            "Cluster": clean(r.get("cluster_key")),
            "URL": clean(r.get("BestLinkURL")),
            "Source": clean(r.get("Source")),
            "Source File": clean(r.get("SourceFile")),
        })
    return pd.DataFrame(rows, columns=LEGACY_COLS)


# ----------------------------------------------------------------------
# v12 final override: this must stay immediately before main().
# Earlier patches in this file redefine build() multiple times.  The final
# override builds the candidate pool from all returned frames and returns in
# the order expected by main(): daily, audit, excluded.
# ----------------------------------------------------------------------
try:
    _PRE_MAIN_BUILD_V12_NEWS = build

    def build(df: pd.DataFrame):
        first, second, third = _PRE_MAIN_BUILD_V12_NEWS(df)
        frames = []
        for part in [first, second, third]:
            if isinstance(part, pd.DataFrame) and not part.empty:
                frames.append(part.copy())
        if not frames:
            empty = pd.DataFrame()
            return empty, empty, empty

        a = pd.concat(frames, ignore_index=True, sort=False)
        a = a.drop_duplicates(subset=[c for c in ["Headline", "URL", "BestLinkURL"] if c in a.columns], keep="first")
        for col in ["Summary", "AI Analysis", "Action Plan"]:
            if col in a.columns:
                a[col] = a[col].apply(remove_ui_noise_v11)
        if "BestLinkURL" not in a.columns:
            a["BestLinkURL"] = a.get("URL", "")
        a["BestLinkURL"] = a["BestLinkURL"].where(a["BestLinkURL"].astype(str).str.strip().ne(""), a.get("URL", ""))
        a["Publish Date"] = a.apply(_v12_publish_date, axis=1)
        a["Date"] = a["Publish Date"]
        a["_sensing_score_v12"] = a.apply(_sensing_score_v11, axis=1)
        a["_issue_key_v12"] = a.apply(_issue_key_v11_news, axis=1)
        a["_v12_hard_reject"] = a.apply(_v12_news_hard_reject, axis=1)
        a["_v12_strong_relevant"] = a.apply(_v12_news_strong_relevant, axis=1)

        min_score = int(os.getenv("GTI_STEP4_NEWS_V12_MIN_SCORE", "35"))
        candidates = a[
            (~a["_v12_hard_reject"])
            & (a["_v12_strong_relevant"])
            & (~a["BestLinkURL"].apply(_bad_news_url_v11))
            & (a["_sensing_score_v12"] >= min_score)
        ].copy()
        candidates = candidates.sort_values(["_sensing_score_v12", "Date"], ascending=[False, False])
        candidates = candidates.drop_duplicates(subset=["BestLinkURL"], keep="first")
        candidates = candidates.drop_duplicates(subset=["_issue_key_v12"], keep="first")
        selected = candidates.head(NEWS_TARGET_MAX).copy()
        selected["selected"] = "Y"
        selected["final_score"] = selected["_sensing_score_v12"].round().astype(int)
        selected["priority_group"] = selected["final_score"].apply(lambda v: "CORE" if v >= 75 else "USABLE")
        selected["RejectReason"] = selected.get("RejectReason", "").fillna("").astype(str)

        selected_keys = set(selected["BestLinkURL"].fillna("").astype(str).str.lower().str.strip())
        audit = a.copy()
        audit["selected"] = audit.apply(lambda r: "Y" if _v12_clean_text(r.get("BestLinkURL", r.get("URL", ""))).lower().strip() in selected_keys else "N", axis=1)
        audit.loc[audit["selected"].ne("Y") & audit["_v12_hard_reject"], "RejectReason"] = audit.loc[audit["selected"].ne("Y") & audit["_v12_hard_reject"], "RejectReason"].apply(lambda v: append_reason(v, "v12_hard_reference_or_noise"))
        audit.loc[audit["selected"].ne("Y") & ~audit["_v12_strong_relevant"], "RejectReason"] = audit.loc[audit["selected"].ne("Y") & ~audit["_v12_strong_relevant"], "RejectReason"].apply(lambda v: append_reason(v, "v12_no_customs_trade_action_signal"))
        excluded = audit[audit["selected"].ne("Y")].copy()
        drop_cols = ["_sensing_score_v12", "_issue_key_v12", "_v12_hard_reject", "_v12_strong_relevant"]
        daily = selected.drop(columns=drop_cols, errors="ignore").reset_index(drop=True)
        audit = audit.drop(columns=drop_cols, errors="ignore").reset_index(drop=True)
        excluded = excluded.drop(columns=drop_cols, errors="ignore").reset_index(drop=True)
        return daily, audit, excluded
except Exception:
    pass


def _v15_text(value):
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _v15_blob(row):
    cols = [
        "Headline", "Summary", "AI Analysis", "Action Plan", "Samsung Impact",
        "samsung_impact", "topic", "issue_type", "topic_keyword",
        "topic_reason", "Agency", "Publisher", "Source", "URL",
        "BestLinkURL", "RejectReason", "Risk", "mail_section",
    ]
    return " ".join(_v15_text(row.get(c)) for c in cols if c in row.index).lower()


def _v15_parse_date(value):
    text = _v15_text(value)
    if not text or text.lower() in {"nan", "nat", "none"}:
        return pd.NaT
    if "확인" in text or "?" in text:
        return pd.NaT
    return pd.to_datetime(text, errors="coerce")


def _v15_has_trade_signal(row):
    blob = _v15_blob(row)
    strong_terms = [
        "tariff", "customs", "duty", "duties", "customs clearance",
        "import declaration", "export declaration", "hs code",
        "rules of origin", "certificate of origin", "fta", "cepa", "tepa",
        "export control", "entity list", "forced labor", "uflpa",
        "cbam", "carbon border", "anti-dumping", "antidumping",
        "countervailing", "safeguard", "quota", "관세", "통관",
        "수입신고", "수출신고", "원산지", "자유무역협정", "수출통제",
        "전략물자", "강제노동", "탄소국경", "반덤핑", "상계관세",
        "덤핑방지", "쿼터", "무역안보",
    ]
    return any(k in blob for k in strong_terms)


def _v15_has_samsung_work_signal(row):
    blob = _v15_blob(row)
    terms = [
        "samsung", "semiconductor", "hbm", "memory", "display",
        "battery", "electronics", "mobile", "tv", "appliance",
        "vietnam", "india", "china", "mexico", "brazil", "eu",
        "usa", "미국", "중국", "베트남", "인도", "멕시코", "브라질",
        "삼성", "반도체", "메모리", "디스플레이", "배터리", "휴대폰",
        "가전", "생산법인", "판매법인",
    ]
    return any(k in blob for k in terms)


def _v15_bad_news(row):
    blob = _v15_blob(row)
    headline = _v15_text(row.get("Headline")).lower()
    bad_terms = [
        "청년인턴", "채용", "모닝뉴스", "호크니", "살상", "종전 mou",
        "인구 1000만", "페라리", "finance must be", "low tariff coverage",
        "american eagle", "hubspot", "crm purchase", "retailer stocks",
        "unemployment rate", "ai search", "노사 갈등", "임협",
    ]
    if any(k in headline for k in bad_terms):
        return True
    bad_phrases = [
        "직접적인 영향은 확인되지",
        "직접적인 관련성은 낮",
        "직접적인 연관성은 확인되지",
        "관세/통상 업무와 직접적인 연관성은 확인되지",
        "업무 관련성이 있는지 확인",
        "요약이 불가능합니다",
        "본문 내용 확인 불가",
        "pdf 파일의 바이너리 데이터",
    ]
    return any(k in blob for k in bad_phrases)


def _v15_reportable_news(row):
    blob = _v15_blob(row)
    impact = _v15_text(row.get("samsung_impact", row.get("Samsung Impact", "")))
    mail_section = _v15_text(row.get("mail_section"))
    risk = _v15_text(row.get("Risk"))

    if _v15_bad_news(row):
        return False
    if not _v15_has_trade_signal(row):
        return False
    if not _v15_has_samsung_work_signal(row):
        # Customs/trade law and policy can still be reported when the action signal is strong.
        if not any(k in blob for k in [
            "customs clearance", "import declaration", "export declaration",
            "rules of origin", "certificate of origin", "anti-dumping",
            "countervailing", "export control", "forced labor", "cbam",
            "통관", "수입신고", "수출신고", "원산지", "반덤핑", "상계관세",
            "수출통제", "강제노동", "탄소국경",
        ]):
            return False
    if impact == "Reference":
        return False
    if mail_section == "Excluded":
        # Recover only if the row has real Direct/Watch customs-work signal.
        if impact not in {"Direct", "Indirect", "Watch"}:
            return False
        if not (_v15_has_trade_signal(row) and _v15_has_samsung_work_signal(row)):
            return False
    if risk == "하" and impact not in {"Direct", "Indirect", "Watch"}:
        return False

    pub = _v15_parse_date(row.get("Publish Date", row.get("Date", "")))
    if pd.isna(pub):
        return False
    cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=int(os.getenv("GTI_STEP4_NEWS_MAX_AGE_DAYS", "3")))
    if pub.normalize() < cutoff:
        return False
    return True


def _v15_news_score(row):
    score = 0
    try:
        score += int(float(row.get("final_score", 0) or 0))
    except Exception:
        pass
    blob = _v15_blob(row)
    impact = _v15_text(row.get("samsung_impact", row.get("Samsung Impact", "")))
    if impact == "Direct":
        score += 80
    elif impact == "Indirect":
        score += 55
    elif impact == "Watch":
        score += 35
    for k, v in [
        ("anti-dumping", 35), ("countervailing", 35), ("반덤핑", 35), ("상계관세", 35),
        ("export control", 35), ("수출통제", 35), ("forced labor", 30), ("uflpa", 30),
        ("cbam", 25), ("탄소국경", 25), ("customs clearance", 25), ("통관", 25),
        ("rules of origin", 25), ("원산지", 25), ("tariff", 20), ("관세", 20),
        ("samsung", 20), ("삼성", 20), ("hbm", 20), ("semiconductor", 15), ("반도체", 15),
    ]:
        if k in blob:
            score += v
    pub = _v15_parse_date(row.get("Publish Date", row.get("Date", "")))
    if not pd.isna(pub):
        age = (pd.Timestamp.now().normalize() - pub.normalize()).days
        score += max(0, 20 - age * 5)
    return score


def _v15_issue_key(row):
    text = _v15_text(row.get("Headline"))
    text = re.sub(r"https?://\S+", " ", text.lower())
    text = re.sub(r"[^0-9a-z가-힣]+", " ", text)
    words = [w for w in text.split() if len(w) > 1]
    return " ".join(words[:10]) or _v15_text(row.get("BestLinkURL", row.get("URL", ""))).lower()


def _v15_final_news_filter(daily, audit, excluded):
    frames = []
    for part in [daily, audit]:
        if isinstance(part, pd.DataFrame) and not part.empty:
            frames.append(part.copy())
    if not frames:
        return daily, audit, excluded

    pool = pd.concat(frames, ignore_index=True, sort=False)
    if "BestLinkURL" not in pool.columns:
        pool["BestLinkURL"] = pool.get("URL", "")
    pool["BestLinkURL"] = pool["BestLinkURL"].where(pool["BestLinkURL"].astype(str).str.strip().ne(""), pool.get("URL", ""))
    pool["Publish Date"] = pool.apply(lambda r: _v15_text(r.get("Publish Date")) or _v15_text(r.get("Date")), axis=1)
    pool["Date"] = pool["Publish Date"]
    pool["_v15_keep"] = pool.apply(_v15_reportable_news, axis=1)
    pool["_v15_score"] = pool.apply(_v15_news_score, axis=1)
    pool["_v15_key"] = pool.apply(_v15_issue_key, axis=1)

    selected = pool[pool["_v15_keep"]].copy()
    selected = selected.sort_values(["_v15_score", "Publish Date"], ascending=[False, False])
    selected = selected.drop_duplicates(subset=["BestLinkURL"], keep="first")
    selected = selected.drop_duplicates(subset=["_v15_key"], keep="first")
    selected = selected.head(min(int(os.getenv("GTI_STEP4_NEWS_TARGET_MAX", str(NEWS_TARGET_MAX))), 30)).copy()
    selected["selected"] = "Y"
    selected["mail_section"] = "News"
    if "samsung_impact" in selected.columns:
        selected["samsung_impact"] = selected["samsung_impact"].replace({"Reference": "Watch", "": "Watch"})
    if "Samsung Impact" in selected.columns:
        selected["Samsung Impact"] = selected["Samsung Impact"].replace({"Reference": "Watch", "": "Watch"})
    selected["final_score"] = selected["_v15_score"].round().astype(int)
    selected["rank"] = range(1, len(selected) + 1)

    selected_urls = set(selected["BestLinkURL"].fillna("").astype(str).str.lower().str.strip())
    full_audit = pool.drop_duplicates(subset=[c for c in ["Headline", "BestLinkURL", "URL"] if c in pool.columns], keep="first").copy()
    full_audit["selected"] = full_audit["BestLinkURL"].fillna("").astype(str).str.lower().str.strip().apply(lambda u: "Y" if u in selected_urls else "N")
    full_audit.loc[full_audit["selected"].ne("Y"), "mail_section"] = "Excluded"
    full_audit.loc[full_audit["selected"].eq("Y"), "mail_section"] = "News"
    full_audit.loc[full_audit["selected"].ne("Y"), "RejectReason"] = full_audit.loc[full_audit["selected"].ne("Y"), "RejectReason"].fillna("").astype(str).apply(
        lambda v: (v + "; " if v else "") + "v15_final_news_filter"
    )
    new_excluded = full_audit[full_audit["selected"].ne("Y")].copy()

    drop_cols = ["_v15_keep", "_v15_score", "_v15_key"]
    return (
        selected.drop(columns=drop_cols, errors="ignore").reset_index(drop=True),
        full_audit.drop(columns=drop_cols, errors="ignore").reset_index(drop=True),
        new_excluded.drop(columns=drop_cols, errors="ignore").reset_index(drop=True),
    )


def _v16_text(value):
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _v16_blob(row):
    cols = [
        "Headline", "Summary", "AI Analysis", "Action Plan", "Samsung Impact",
        "samsung_impact", "topic", "issue_type", "topic_keyword", "topic_reason",
        "RegulationRelated", "RegulationTransferType", "affected_subsidiary",
        "Agency", "Publisher", "Source", "URL", "BestLinkURL", "RejectReason",
        "Risk", "mail_section", "priority_group",
    ]
    return " ".join(_v16_text(row.get(c)) for c in cols if c in row.index).lower()


def _v16_parse_date(value):
    text = _v16_text(value)
    if not text or text.lower() in {"nan", "nat", "none"}:
        return pd.NaT
    if "확인" in text or "?" in text:
        return pd.NaT
    return pd.to_datetime(text, errors="coerce")


def _v16_hard_noise(row):
    blob = _v16_blob(row)
    headline = _v16_text(row.get("Headline")).lower()
    hard_terms = [
        "청년인턴", "채용", "모닝뉴스", "호크니", "현대미술", "살상",
        "인구 1000만", "스위스 인구", "종전 mou", "미 이란",
        "페라리", "finance must be", "american eagle", "hubspot",
        "crm purchase", "retailer stocks", "unemployment rate",
        "ai search", "보안시장", "주가", "증시", "노사 갈등", "임협",
    ]
    if any(k in headline for k in hard_terms):
        return True
    hard_phrases = [
        "요약이 불가능합니다",
        "pdf 파일의 바이너리 데이터",
        "본문 내용 확인 불가",
        "업무 관련성이 있는지 확인",
        "해당 없음",
    ]
    return any(k in blob for k in hard_phrases)


def _v16_customs_trade_law_score(row):
    blob = _v16_blob(row)
    score = 0
    regulation_terms = [
        "anti-dumping", "antidumping", "countervailing", "safeguard",
        "customs duty", "import duty", "tariff rate", "tariff quota",
        "rules of origin", "certificate of origin", "export obligation",
        "advance authorization", "epcg", "cbam", "uflpa", "forced labor",
        "federal register", "customs notice", "public notice", "trade notice",
        "반덤핑", "상계관세", "덤핑방지", "세이프가드", "관세율", "관세쿼터",
        "원산지 기준", "원산지증명", "수출의무", "수입규제", "탄소국경",
        "강제노동", "고시", "공고", "입법예고", "행정예고",
    ]
    if any(k in blob for k in regulation_terms):
        score = 30
    if _v16_text(row.get("RegulationRelated")).upper() == "Y":
        score = max(score, 18)
    return score


def _v16_customs_trade_policy_score(row):
    blob = _v16_blob(row)
    policy_terms = [
        "tariff", "customs", "fta", "cepa", "tepa", "trade agreement",
        "export control", "entity list", "section 301", "customs clearance",
        "import declaration", "export declaration", "supply chain",
        "trade security", "quota", "관세", "통관", "fta", "cepa", "tepa",
        "자유무역협정", "수출통제", "전략물자", "무역안보", "공급망",
        "쿼터", "수입신고", "수출신고", "무역협정",
    ]
    return 20 if any(k in blob for k in policy_terms) else 0


def _v16_direct_impact_score(row):
    blob = _v16_blob(row)
    impact = _v16_text(row.get("samsung_impact", row.get("Samsung Impact", "")))
    if impact == "Direct":
        return 40
    direct_terms = [
        "samsung", "삼성", "hbm", "semiconductor", "반도체", "memory",
        "메모리", "display", "디스플레이", "mobile", "휴대폰",
        "tv", "appliance", "가전", "battery", "배터리",
    ]
    work_terms = [
        "tariff", "customs", "export control", "anti-dumping",
        "countervailing", "cbam", "forced labor", "rules of origin",
        "관세", "통관", "수출통제", "반덤핑", "상계관세", "탄소국경",
        "강제노동", "원산지",
    ]
    return 40 if any(k in blob for k in direct_terms) and any(k in blob for k in work_terms) else 0


def _v16_indirect_impact_score(row):
    blob = _v16_blob(row)
    impact = _v16_text(row.get("samsung_impact", row.get("Samsung Impact", "")))
    if impact in {"Indirect", "Watch"}:
        return 10
    country_terms = [
        "vietnam", "india", "china", "mexico", "brazil", "eu", "usa",
        "korea", "japan", "베트남", "인도", "중국", "멕시코", "브라질",
        "미국", "eu", "유럽", "한국", "일본", "생산법인", "판매법인",
    ]
    trade_terms = [
        "tariff", "customs", "fta", "cepa", "export control", "quota",
        "supply chain", "관세", "통관", "fta", "cepa", "수출통제",
        "쿼터", "공급망", "원산지",
    ]
    return 10 if any(k in blob for k in country_terms) and any(k in blob for k in trade_terms) else 0


def _v16_weighted_score(row):
    law = _v16_customs_trade_law_score(row)
    policy = _v16_customs_trade_policy_score(row)
    direct = _v16_direct_impact_score(row)
    indirect = _v16_indirect_impact_score(row)
    score = law + policy + direct + indirect

    # Small tie-breakers only. The four weights above remain the main score.
    try:
        score += min(int(float(row.get("final_score", 0) or 0)) // 20, 5)
    except Exception:
        pass
    pub = _v16_parse_date(row.get("Publish Date", row.get("Date", "")))
    if not pd.isna(pub):
        age = (pd.Timestamp.now().normalize() - pub.normalize()).days
        score += max(0, 3 - age)

    return score


def _v16_issue_key(row):
    text = _v16_text(row.get("Headline"))
    text = re.sub(r"https?://\S+", " ", text.lower())
    text = re.sub(r"[^0-9a-z가-힣]+", " ", text)
    words = [w for w in text.split() if len(w) > 1]
    return " ".join(words[:10]) or _v16_text(row.get("BestLinkURL", row.get("URL", ""))).lower()


def _v16_final_weighted_top30(daily, audit, excluded):
    frames = []
    for part in [daily, audit, excluded]:
        if isinstance(part, pd.DataFrame) and not part.empty:
            frames.append(part.copy())
    if not frames:
        return daily, audit, excluded

    pool = pd.concat(frames, ignore_index=True, sort=False)
    if "BestLinkURL" not in pool.columns:
        pool["BestLinkURL"] = pool.get("URL", "")
    pool["BestLinkURL"] = pool["BestLinkURL"].where(pool["BestLinkURL"].astype(str).str.strip().ne(""), pool.get("URL", ""))
    pool["Publish Date"] = pool.apply(lambda r: _v16_text(r.get("Publish Date")) or _v16_text(r.get("Date")), axis=1)
    pool["Date"] = pool["Publish Date"]

    pool["_v16_law30"] = pool.apply(_v16_customs_trade_law_score, axis=1)
    pool["_v16_policy20"] = pool.apply(_v16_customs_trade_policy_score, axis=1)
    pool["_v16_direct40"] = pool.apply(_v16_direct_impact_score, axis=1)
    pool["_v16_indirect10"] = pool.apply(_v16_indirect_impact_score, axis=1)
    pool["_v16_score"] = pool.apply(_v16_weighted_score, axis=1)
    pool["_v16_key"] = pool.apply(_v16_issue_key, axis=1)
    pool["_v16_noise"] = pool.apply(_v16_hard_noise, axis=1)

    max_age = int(os.getenv("GTI_STEP4_NEWS_MAX_AGE_DAYS", "3"))
    cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=max_age)
    pub = pool["Publish Date"].apply(_v16_parse_date)
    candidates = pool[
        (~pool["_v16_noise"])
        & (pool["_v16_score"] > 0)
        & pub.notna()
        & (pub.dt.normalize() >= cutoff)
    ].copy()

    candidates = candidates.sort_values(["_v16_score", "Publish Date"], ascending=[False, False])
    candidates = candidates.drop_duplicates(subset=["BestLinkURL"], keep="first")
    candidates = candidates.drop_duplicates(subset=["_v16_key"], keep="first")
    top_n = min(int(os.getenv("GTI_STEP4_NEWS_TARGET_MAX", "50")), 50)
    selected = candidates.head(top_n).copy()

    selected["selected"] = "Y"
    selected["mail_section"] = "News"
    selected["final_score"] = selected["_v16_score"].round().astype(int)
    selected["priority_group"] = selected["final_score"].apply(lambda v: "CORE" if v >= 70 else "USABLE")
    if "samsung_impact" in selected.columns:
        selected["samsung_impact"] = selected.apply(
            lambda r: "Direct" if r["_v16_direct40"] >= 40 else ("Indirect" if r["_v16_indirect10"] >= 10 else "Reference"),
            axis=1,
        )
    if "Samsung Impact" in selected.columns:
        selected["Samsung Impact"] = selected.apply(
            lambda r: "Direct" if r["_v16_direct40"] >= 40 else ("Indirect" if r["_v16_indirect10"] >= 10 else "Reference"),
            axis=1,
        )
    selected["rank"] = range(1, len(selected) + 1)
    selected["ScoreBreakdown"] = selected.apply(
        lambda r: f"law30={int(r['_v16_law30'])}; policy20={int(r['_v16_policy20'])}; direct40={int(r['_v16_direct40'])}; indirect10={int(r['_v16_indirect10'])}",
        axis=1,
    )

    selected_urls = set(selected["BestLinkURL"].fillna("").astype(str).str.lower().str.strip())
    full_audit = pool.drop_duplicates(subset=[c for c in ["Headline", "BestLinkURL", "URL"] if c in pool.columns], keep="first").copy()
    full_audit["ScoreBreakdown"] = full_audit.apply(
        lambda r: f"law30={int(r['_v16_law30'])}; policy20={int(r['_v16_policy20'])}; direct40={int(r['_v16_direct40'])}; indirect10={int(r['_v16_indirect10'])}",
        axis=1,
    )
    full_audit["selected"] = full_audit["BestLinkURL"].fillna("").astype(str).str.lower().str.strip().apply(lambda u: "Y" if u in selected_urls else "N")
    full_audit.loc[full_audit["selected"].eq("Y"), "mail_section"] = "News"
    full_audit.loc[full_audit["selected"].ne("Y"), "mail_section"] = "Excluded"
    full_audit.loc[full_audit["selected"].ne("Y"), "RejectReason"] = full_audit.loc[full_audit["selected"].ne("Y"), "RejectReason"].fillna("").astype(str).apply(
        lambda v: (v + "; " if v else "") + "v16_weighted_below_topN_or_noise"
    )
    new_excluded = full_audit[full_audit["selected"].ne("Y")].copy()

    drop_cols = ["_v16_law30", "_v16_policy20", "_v16_direct40", "_v16_indirect10", "_v16_score", "_v16_key", "_v16_noise"]
    return (
        selected.drop(columns=drop_cols, errors="ignore").reset_index(drop=True),
        full_audit.drop(columns=drop_cols, errors="ignore").reset_index(drop=True),
        new_excluded.drop(columns=drop_cols, errors="ignore").reset_index(drop=True),
    )



# ======================================================================
# GTI STEP4-2 Weighted Score Patch v18
# ----------------------------------------------------------------------
# Scoring basis requested by user:
# - Customs/Trade Law       30%
# - Customs/Trade Policy    20%
# - Samsung Direct Impact   40%
# - Samsung Indirect Impact 10%
#
# v18 fixes v17 issue:
# - Do NOT give all rows 100 points.
# - Samsung mention alone is not Direct Impact unless customs/trade relevance exists.
# - General culture, marketing, finance, stock, event, pharma, politics, lifestyle
#   articles are downgraded even if they contain Samsung/product words.
# - Select Top 30 by WeightedScore after hard-noise exclusion.
# ======================================================================

GTI_WEIGHTED_TOP_N = int(os.getenv("GTI_WEIGHTED_TOP_N", "50"))

V18_HARD_NOISE_TERMS = [
    "마케팅", "고객 이탈", "리더가 사는 곳", "신약", "빅파마", "예술", "시드니",
    "비비드", "축제", "브랜드", "주가", "증시", "선거", "국민투표", "스포츠",
    "올림픽공원", "게임", "인벤", "피날레", "여성 예술가", "칼럼", "기고",
    "정유업계", "최고가격제", "손실 범위", "고환율", "외화유동성",
    "marketing", "brand", "stock", "shares", "election", "festival", "pharma",
    "sports", "opinion", "column", "culture", "art",
]

V18_LAW_STRONG_TERMS = [
    "법령안", "고시", "공고", "규칙", "관세법", "customs law", "federal register",
    "regulation", "rule", "notice", "anti-dumping", "antidumping", "countervailing",
    "ad/cvd", "덤핑방지", "반덤핑", "상계관세", "cbam", "carbon border",
    "수출통제", "export control", "entity list", "uflpa", "forced labor",
    "fta", "cepa", "rules of origin", "원산지", "hs code", "품목분류",
    "customs", "clearance", "declaration", "통관", "세관", "관세청",
]

V18_POLICY_STRONG_TERMS = [
    "관세", "관세율", "추가관세", "상호관세", "section 301", "301조",
    "section 232", "232조", "tariff", "tariffs", "customs duty", "import duty",
    "quota", "쿼터", "무관세", "safeguard", "세이프가드", "trade policy",
    "통상 정책", "무역정책", "수출통제", "export control", "entity list",
    "제재", "sanction", "cbam", "탄소국경", "fta", "cepa", "usmca",
    "원산지", "덤핑방지", "반덤핑", "상계관세",
]

V18_DIRECT_COMPANY_TERMS = [
    "samsung electronics", "samsung sdi", "samsung display", "sec", "삼성전자",
    "삼성sdi", "삼성디스플레이", "삼성전기", "삼성바이오로직스",
]

V18_CORE_PRODUCT_TERMS = [
    "semiconductor", "semiconductors", "chip", "chips", "ai chip", "ai chips",
    "hbm", "memory", "메모리", "반도체", "칩", "ai칩", "ai 칩",
    "battery", "batteries", "배터리", "이차전지", "ev battery",
    "display", "oled", "디스플레이", "smartphone", "mobile", "galaxy", "스마트폰", "갤럭시",
]

V18_SUPPLYCHAIN_TERMS = [
    "steel", "aluminum", "copper", "lithium", "nickel", "rare earth", "graphite",
    "철강", "알루미늄", "구리", "리튬", "니켈", "희토류", "흑연", "도금강판",
    "냉간압연", "스테인리스강", "합판", "pcb", "wafer", "웨이퍼",
]

V18_KEY_COUNTRY_TERMS = [
    "korea", "한국", "중국", "china", "미국", "united states", "us ", "u.s.",
    "eu", "european union", "유럽", "베트남", "vietnam", "인도", "india",
    "멕시코", "mexico", "폴란드", "poland", "일본", "japan", "태국", "thailand",
    "말레이시아", "malaysia", "브라질", "brazil", "헝가리", "hungary",
]

def _v18_blob(row: pd.Series) -> str:
    return " ".join(clean(row.get(c, "")) for c in [
        "Headline", "Summary", "AI Analysis", "Action Plan", "KeywordMatches",
        "ClusterHeadlines", "topic", "topic_keyword", "Issue", "Publisher",
        "Agency", "Country", "article_body"
    ]).lower()

def _v18_contains(blob: str, terms: list[str]) -> bool:
    return any(t.lower() in blob for t in terms)

def _v18_noise_hit(row: pd.Series) -> bool:
    blob = _v18_blob(row)
    if _v18_contains(blob, V18_HARD_NOISE_TERMS):
        # Do not mark as hard noise if it also contains very strong actionable customs terms.
        very_strong = ["반덤핑", "덤핑방지", "상계관세", "cbam", "수출통제", "entity list", "section 301", "section 232", "customs law"]
        return not _v18_contains(blob, very_strong)
    return False

def _v18_customs_trade_law_score(row: pd.Series) -> int:
    blob = _v18_blob(row)
    topic = clean(row.get("topic", "")).upper()
    url = clean(row.get("URL", "")).lower()
    agency = clean(row.get("Agency", "")).lower()
    if _v18_noise_hit(row):
        return 0
    if topic in {"AD_CVD", "CBAM_CARBON", "ORIGIN_FTA", "HS_CLASSIFICATION"}:
        return 30
    if any(x in url + " " + agency for x in [".gov", "federalregister", "customs", "관세청", "law.go.kr", "europa.eu", "dgft", "cbp.gov", "ustr.gov", "bis.gov"]):
        if _v18_contains(blob, V18_LAW_STRONG_TERMS):
            return 30
    if _v18_contains(blob, ["법령안", "고시", "규칙", "federal register", "regulation", "customs law"]):
        return 30
    if _v18_contains(blob, V18_LAW_STRONG_TERMS):
        return 20
    if topic in {"CUSTOMS", "TARIFF", "EXPORT_CONTROL"}:
        return 15
    return 0

def _v18_customs_trade_policy_score(row: pd.Series) -> int:
    blob = _v18_blob(row)
    topic = clean(row.get("topic", "")).upper()
    if _v18_noise_hit(row):
        return 0
    if topic in {"EXPORT_CONTROL", "TARIFF", "AD_CVD", "CBAM_CARBON"}:
        return 20
    if _v18_contains(blob, ["section 301", "301조", "section 232", "232조", "수출통제", "export control", "entity list", "cbam", "반덤핑", "상계관세"]):
        return 20
    if _v18_contains(blob, ["fta", "cepa", "usmca", "통상협정", "무역협상", "trade agreement"]):
        return 15
    if _v18_contains(blob, V18_POLICY_STRONG_TERMS):
        return 12
    if _v18_contains(blob, ["경제협력", "정상회담", "협의", "consultation"]):
        return 5
    return 0

def _v18_direct_impact_score(row: pd.Series) -> int:
    blob = _v18_blob(row)
    law_policy = _v18_customs_trade_law_score(row) + _v18_customs_trade_policy_score(row)
    if _v18_noise_hit(row):
        return 0
    if law_policy == 0:
        return 0
    # Direct means Samsung/company or core Samsung product affected by concrete customs/trade issue.
    if _v18_contains(blob, V18_DIRECT_COMPANY_TERMS):
        return 40
    if _v18_contains(blob, V18_CORE_PRODUCT_TERMS) and _v18_contains(blob, V18_POLICY_STRONG_TERMS):
        return 35
    if _v18_contains(blob, V18_CORE_PRODUCT_TERMS):
        return 28
    if _v18_contains(blob, V18_SUPPLYCHAIN_TERMS) and _v18_contains(blob, ["관세", "tariff", "반덤핑", "anti-dumping", "cbam", "원산지", "fta"]):
        return 20
    return 0

def _v18_indirect_impact_score(row: pd.Series) -> int:
    blob = _v18_blob(row)
    law_policy = _v18_customs_trade_law_score(row) + _v18_customs_trade_policy_score(row)
    if _v18_noise_hit(row):
        return 0
    if law_policy == 0:
        return 0
    score = 0
    if _v18_contains(blob, V18_KEY_COUNTRY_TERMS):
        score += 4
    if _v18_contains(blob, V18_SUPPLYCHAIN_TERMS):
        score += 4
    if _v18_contains(blob, ["supply chain", "공급망", "cost", "원가", "조달", "수입가격", "수출", "수입"]):
        score += 2
    return min(score, 10)

def _v18_weighted_score(row: pd.Series) -> int:
    return (
        _v18_customs_trade_law_score(row)
        + _v18_customs_trade_policy_score(row)
        + _v18_direct_impact_score(row)
        + _v18_indirect_impact_score(row)
    )

def _v18_is_reportable(row: pd.Series) -> bool:
    if _v18_noise_hit(row):
        return False
    if _v18_weighted_score(row) < int(os.getenv("GTI_WEIGHTED_MIN_SCORE", "30")):
        return False
    # Need at least one customs/trade law or policy score.
    if (_v18_customs_trade_law_score(row) + _v18_customs_trade_policy_score(row)) <= 0:
        return False
    return True

def _v18_reclassify_impact(row: pd.Series) -> str:
    direct = _v18_direct_impact_score(row)
    indirect = _v18_indirect_impact_score(row)
    if direct >= 35:
        return "Direct"
    if direct >= 20 or indirect >= 6:
        return "Indirect"
    if _v18_customs_trade_law_score(row) + _v18_customs_trade_policy_score(row) > 0:
        return "Watch"
    return "Reference"

def _v18_apply_weighted_top30(audit: pd.DataFrame) -> pd.DataFrame:
    if audit is None or audit.empty:
        return audit
    audit = audit.copy()

    audit["CustomsTradeLawScore"] = audit.apply(_v18_customs_trade_law_score, axis=1)
    audit["CustomsTradePolicyScore"] = audit.apply(_v18_customs_trade_policy_score, axis=1)
    audit["DirectImpactScore"] = audit.apply(_v18_direct_impact_score, axis=1)
    audit["IndirectImpactScore"] = audit.apply(_v18_indirect_impact_score, axis=1)
    audit["WeightedScore"] = audit.apply(_v18_weighted_score, axis=1)
    audit["ScoreBreakdown"] = audit.apply(
        lambda r: f"법규30={int(r['CustomsTradeLawScore'])}; 정책20={int(r['CustomsTradePolicyScore'])}; 직접40={int(r['DirectImpactScore'])}; 간접10={int(r['IndirectImpactScore'])}",
        axis=1,
    )
    audit["samsung_impact"] = audit.apply(_v18_reclassify_impact, axis=1)

    candidates = audit[audit.apply(_v18_is_reportable, axis=1)].copy()
    candidates = candidates.sort_values(["WeightedScore", "final_score", "Date"], ascending=[False, False, False])
    top_n = min(GTI_WEIGHTED_TOP_N, len(candidates))

    selected_keys = set(candidates.head(top_n).index)
    audit["selected"] = "N"
    audit.loc[list(selected_keys), "selected"] = "Y"
    audit.loc[audit["selected"].eq("Y"), "priority_group"] = "CORE"
    audit.loc[audit["selected"].eq("Y"), "mail_section"] = "News Core"
    audit.loc[audit["selected"].ne("Y"), "priority_group"] = "EXCLUDED"
    audit.loc[audit["selected"].ne("Y"), "mail_section"] = "Excluded"
    audit.loc[audit["selected"].ne("Y"), "RejectReason"] = audit.loc[audit["selected"].ne("Y"), "RejectReason"].apply(
        lambda v: append_reason(v, "weighted_v18_not_topN_or_noise")
    )

    audit["final_score"] = audit["WeightedScore"]
    audit["Risk"] = audit["WeightedScore"].apply(lambda s: "상" if s >= 70 else "중" if s >= 45 else "하")
    log(f"Weighted v18 TOPN selected={int(audit['selected'].eq('Y').sum())} / candidates={len(candidates)} / basis=law30+policy20+direct40+indirect10")
    return audit

try:
    _ORIGINAL_BUILD_WEIGHTED_V18 = build
    def build(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        daily, audit, excluded = _ORIGINAL_BUILD_WEIGHTED_V18(df)
        # Rebuild selection from audit so v17/v12 previous selection cannot dominate.
        audit = _v18_apply_weighted_top30(audit)
        daily = audit[audit["selected"].eq("Y")].copy()
        daily = daily.sort_values(["WeightedScore", "final_score", "Date"], ascending=[False, False, False]).reset_index(drop=True)
        daily["rank"] = range(1, len(daily) + 1)
        audit = audit.sort_values(["selected", "WeightedScore", "final_score"], ascending=[False, False, False]).reset_index(drop=True)
        audit["rank"] = range(1, len(audit) + 1)
        excluded = audit[audit["selected"].ne("Y")].copy()

        for frame in [daily, audit, excluded]:
            for col in ["CustomsTradeLawScore", "CustomsTradePolicyScore", "DirectImpactScore", "IndirectImpactScore", "WeightedScore", "ScoreBreakdown"]:
                if col not in frame.columns:
                    frame[col] = ""
            for col in OUTPUT_COLS:
                if col not in frame.columns:
                    frame[col] = ""

        extra_cols = ["Publish Date", "CustomsTradeLawScore", "CustomsTradePolicyScore", "DirectImpactScore", "IndirectImpactScore", "WeightedScore", "ScoreBreakdown"]
        out_cols = list(dict.fromkeys(OUTPUT_COLS + extra_cols))
        return daily[out_cols], audit[out_cols], excluded[out_cols]
except Exception:
    pass

# ======================================================================
# End of GTI STEP4-2 Weighted Score Patch v18
# ======================================================================


# ======================================================================
# GTI STEP4-2 Final-50 Gemini Patch v19 - 2026-06-17
# ----------------------------------------------------------------------
# Goal
# - Do fast rule/weighted scoring for all news rows first.
# - Do NOT fetch original article or call Gemini during row scoring.
# - After final selection, enrich only final selected rows, max 50, with
#   original URL body + Gemini analysis.
# - Avoid long hangs by using a single Gemini model and short timeout.
# ======================================================================

GTI_FINAL_GEMINI_MAX = int(os.getenv("GTI_FINAL_GEMINI_MAX", "50"))
GTI_GEMINI_TIMEOUT = int(os.getenv("GTI_GEMINI_TIMEOUT", "20"))
GTI_FAST_SCORING_ONLY = os.getenv("GTI_FAST_SCORING_ONLY", "1").strip().upper() not in {"0", "N", "NO", "FALSE"}

# Keep the last rich analyzer before replacing it with a fast analyzer.
_ORIGINAL_FINAL50_BUILD_GTI_AI_ANALYSIS = build_gti_ai_analysis
_FINAL50_GEMINI_PHASE = False

# Normalize candidate list so Gemini does not try 7 models x 2 API versions x 2 payloads.
_GTI_SINGLE_MODEL = (os.getenv("GTI_GEMINI_MODEL", "").strip() or "gemini-2.5-flash-lite")
GEMINI_MODEL_CANDIDATES = [_GTI_SINGLE_MODEL]
GEMINI_API_VERSIONS = [os.getenv("GTI_GEMINI_API_VERSION", "v1beta").strip() or "v1beta"]

def call_gemini_json(prompt: str) -> dict:
    """v19 override: single model, single API version, short timeout."""
    global _LAST_GEMINI_ERROR
    _LAST_GEMINI_ERROR = ""

    if not USE_GEMINI:
        _LAST_GEMINI_ERROR = "DISABLED"
        return {"_error": _LAST_GEMINI_ERROR}
    if not GEMINI_API_KEY:
        _LAST_GEMINI_ERROR = "NO_API_KEY"
        return {"_error": _LAST_GEMINI_ERROR}

    api_ver = GEMINI_API_VERSIONS[0]
    model = GEMINI_MODEL_CANDIDATES[0]
    endpoint = f"https://generativelanguage.googleapis.com/{api_ver}/models/{model}:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "topP": 0.8,
            "maxOutputTokens": 1400,
            "responseMimeType": "application/json",
        },
    }
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(endpoint, data=data, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=GTI_GEMINI_TIMEOUT) as resp:
            raw = resp.read().decode("utf-8", "ignore")
        out = json.loads(raw)
        if "error" in out:
            msg = out.get("error", {}).get("message", str(out.get("error")))
            _LAST_GEMINI_ERROR = f"{api_ver}/{model}:API_ERROR:{msg[:200]}"
            return {"_error": _LAST_GEMINI_ERROR}
        candidates = out.get("candidates") or []
        if not candidates:
            _LAST_GEMINI_ERROR = f"{api_ver}/{model}:NO_CANDIDATE"
            return {"_error": _LAST_GEMINI_ERROR}
        parts = candidates[0].get("content", {}).get("parts", [])
        text = "\n".join(clean(part.get("text", "")) for part in parts if isinstance(part, dict))
        parsed = _extract_json_object(text)
        if parsed:
            parsed["_gemini_model"] = model
            parsed["_gemini_api_version"] = api_ver
            parsed["_gemini_finish"] = candidates[0].get("finishReason", "")
            return parsed
        _LAST_GEMINI_ERROR = f"{api_ver}/{model}:NO_JSON text={text[:160]}"
        return {"_error": _LAST_GEMINI_ERROR}
    except urllib.error.HTTPError as exc:
        try:
            err_body = exc.read().decode("utf-8", "ignore")[:220]
        except Exception:
            err_body = ""
        _LAST_GEMINI_ERROR = f"{api_ver}/{model}:HTTP{exc.code}:{err_body}"
        return {"_error": _LAST_GEMINI_ERROR}
    except Exception as exc:
        _LAST_GEMINI_ERROR = f"{api_ver}/{model}:{type(exc).__name__}:{str(exc)[:220]}"
        return {"_error": _LAST_GEMINI_ERROR}


def _v19_fast_summary_from_row(row: pd.Series, headline: str) -> str:
    for col in ["Summary", "description", "Description", "ClusterHeadlines", "AI Analysis"]:
        val = clean(row.get(col, ""))
        if val and not _looks_like_title_only(val, headline):
            return val[:900]
    return headline[:500]


def build_gti_ai_analysis(row: pd.Series, *, headline: str, url: str, issue: str, impact: str, products_text: str, default_action: str, content_type: str) -> dict:
    """v19 fast analyzer during scoring; rich analyzer only in final Gemini phase."""
    if _FINAL50_GEMINI_PHASE:
        return _ORIGINAL_FINAL50_BUILD_GTI_AI_ANALYSIS(
            row,
            headline=headline,
            url=url,
            issue=issue,
            impact=impact,
            products_text=products_text,
            default_action=default_action,
            content_type=content_type,
        )

    summary = _v19_fast_summary_from_row(row, headline)
    ai = (
        f"{issue} 이슈입니다. 현재 단계는 전체 후보 빠른 선별 단계이므로 원문/Gemini 분석은 최종 선정 후 수행합니다. "
        f"삼성 영향도는 {impact}, 관련 제품/키워드는 {products_text or '본문에서 확인 불가'}입니다."
    )
    action = default_action or "대상 국가·품목·HS·세율·시행일을 확인하십시오."
    return {
        "Summary": summary,
        "AI Analysis": ai[:1200],
        "Action Plan": action[:1200],
        "ExecutiveMessage": summary[:700],
        "article_extract_status": "FAST_SCORING_NO_GEMINI",
    }


def _v19_selected_match_key(row: pd.Series) -> str:
    url = safe_url(row.get("BestLinkURL", "") or row.get("URL", "") or row.get("original_url", ""))
    if url:
        return "url:" + url.lower().strip()
    return "title:" + normalize_title(clean(row.get("Headline", "")))


def _v19_enrich_selected_with_gemini(daily: pd.DataFrame, audit: pd.DataFrame, excluded: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Enrich only final selected rows with original URL/Gemini analysis."""
    global _FINAL50_GEMINI_PHASE
    if daily is None or daily.empty:
        log("Final Gemini enrichment skipped: no selected news")
        return daily, audit, excluded
    if not USE_GEMINI:
        log("Final Gemini enrichment skipped: GTI_STEP4_USE_GEMINI=N")
        return daily, audit, excluded

    daily = daily.copy()
    audit = audit.copy() if isinstance(audit, pd.DataFrame) else pd.DataFrame()
    excluded = excluded.copy() if isinstance(excluded, pd.DataFrame) else pd.DataFrame()

    limit = min(GTI_FINAL_GEMINI_MAX, len(daily))
    log(f"Final Gemini enrichment start: selected={len(daily)}, enrich_limit={limit}, model={GEMINI_MODEL_CANDIDATES[0]}, timeout={GTI_GEMINI_TIMEOUT}s")

    _FINAL50_GEMINI_PHASE = True
    try:
        for pos, idx in enumerate(daily.index[:limit], start=1):
            row = daily.loc[idx]
            headline = clean(row.get("Headline", ""))
            url, _status = choose_best_link(row, resolve_google=False)
            issue = clean(row.get("topic_keyword", "")) or clean(row.get("topic", "")) or clean(row.get("Issue", "")) or "무역/통관"
            impact = clean(row.get("samsung_impact", "")) or clean(row.get("Samsung Impact", "")) or "Watch"
            products_text = clean(row.get("affected_products", "")) or clean(row.get("impact_products", "")) or "본문에서 확인 불가"
            default_action = clean(row.get("RequiredAction", "")) or clean(row.get("Action Plan", "")) or "대상 국가·품목·HS·세율·시행일을 확인하십시오."

            log(f"Final Gemini {pos}/{limit}: {headline[:90]}")
            analysis = _ORIGINAL_FINAL50_BUILD_GTI_AI_ANALYSIS(
                row,
                headline=headline,
                url=url,
                issue=issue,
                impact=impact,
                products_text=products_text,
                default_action=default_action,
                content_type="News",
            )
            for col, key in [
                ("Summary", "Summary"),
                ("AI Analysis", "AI Analysis"),
                ("Action Plan", "Action Plan"),
                ("ExecutiveMessage", "ExecutiveMessage"),
                ("article_extract_status", "article_extract_status"),
            ]:
                if col not in daily.columns:
                    daily[col] = ""
                daily.at[idx, col] = analysis.get(key, daily.at[idx, col] if col in daily.columns else "")
            if "URL" in daily.columns and url:
                daily.at[idx, "URL"] = url
            if "BestLinkURL" in daily.columns and url:
                daily.at[idx, "BestLinkURL"] = url

            match_key = _v19_selected_match_key(daily.loc[idx])
            if not audit.empty:
                if "_v19_match_key" not in audit.columns:
                    audit["_v19_match_key"] = audit.apply(_v19_selected_match_key, axis=1)
                mask = audit["_v19_match_key"].eq(match_key)
                for col in ["Summary", "AI Analysis", "Action Plan", "ExecutiveMessage", "article_extract_status", "URL", "BestLinkURL"]:
                    if col not in audit.columns:
                        audit[col] = ""
                    if col in daily.columns:
                        audit.loc[mask, col] = daily.at[idx, col]
    finally:
        _FINAL50_GEMINI_PHASE = False
        try:
            _save_gemini_cache()
            log("Final Gemini cache saved")
        except Exception as exc:
            log(f"Final Gemini cache save failed: {type(exc).__name__}: {exc}")

    if not audit.empty:
        audit = audit.drop(columns=["_v19_match_key"], errors="ignore")
    log("Final Gemini enrichment done")
    return daily, audit, excluded

# ======================================================================
# End of GTI STEP4-2 Final-50 Gemini Patch v19
# ======================================================================



# ======================================================================
# GTI STEP4-2 Major-News Title Keyword Patch v20 - 2026-06-17
# ----------------------------------------------------------------------
# Goal
# - Separate articles whose HEADLINE contains high-value customs/trade
#   keywords into Major News before normal weighted Top50 fill.
# - Major News still respects hard noise/date/url gates, but does not get
#   lost only because Samsung relevance score is weak.
# - Gemini enrichment remains limited to final selected rows, max 50.
# ======================================================================

GTI_MAJOR_TITLE_ENABLED = os.getenv("GTI_MAJOR_TITLE_ENABLED", "1").strip().upper() not in {"0", "N", "NO", "FALSE"}
GTI_MAJOR_TITLE_MAX = int(os.getenv("GTI_MAJOR_TITLE_MAX", "20"))
GTI_MAJOR_TITLE_BONUS = int(os.getenv("GTI_MAJOR_TITLE_BONUS", "35"))

MAJOR_TITLE_KEYWORD_GROUPS = {
    "AD_CVD": [
        "anti-dumping", "anti dumping", "antidumping", "countervailing", "countervailing duty",
        "ad/cvd", "cvd", "dumping margin", "덤핑방지", "반덤핑", "상계관세", "무역구제",
    ],
    "TARIFF_QUOTA": [
        "tariff", "tariffs", "customs duty", "import duty", "reciprocal tariff",
        "section 301", "301조", "section 232", "232조", "tariff quota", "tariff-rate quota",
        "quota", "duty-free quota", "safeguard", "관세", "관세율", "추가관세", "상호관세",
        "쿼터", "할당관세", "무관세", "세이프가드",
    ],
    "EXPORT_CONTROL": [
        "export control", "export controls", "entity list", "denied persons", "bis rule",
        "forced labor", "uflpa", "sanction", "sanctions", "수출통제", "수출 통제",
        "전략물자", "제재", "강제노동", "거래제한",
    ],
    "FTA_ORIGIN": [
        "fta", "cepa", "tepa", "usmca", "rules of origin", "rule of origin",
        "certificate of origin", "origin certificate", "country of origin",
        "origin verification", "origin determination", "원산지", "원산지증명", "원산지 증명",
        "원산지검증", "원산지 검증", "자유무역협정", "통상협정",
    ],
    "CUSTOMS_HS": [
        "customs clearance", "customs declaration", "import declaration", "export declaration",
        "hs code", "tariff classification", "classification ruling", "customs valuation",
        "valuation", "bonded", "deferment", "통관", "수입신고", "수출신고", "세관",
        "품목분류", "hs코드", "과세가격", "관세평가", "보세", "납부유예",
    ],
    "CBAM_CARBON": [
        "cbam", "carbon border", "carbon border adjustment", "탄소국경", "탄소국경조정",
    ],
    "NOTICE_POLICY": [
        "public notice", "notice", "notification", "regulation", "final rule", "interim rule",
        "effective date", "implementation", "시행", "발효", "고시", "공고", "입법예고", "행정예고",
    ],
}

# Optional user extension: powershell example
# $env:GTI_MAJOR_TITLE_EXTRA_KEYWORDS="PN 51|export obligation|advance authorization|EPCG"
def _v20_extra_major_keywords() -> list[str]:
    raw = os.getenv("GTI_MAJOR_TITLE_EXTRA_KEYWORDS", "")
    return [x.strip().lower() for x in re.split(r"[|,;]", raw) if x.strip()]


def _v20_title_blob(row: pd.Series) -> str:
    title = clean(row.get("Headline", ""))
    title = _html_unescape(title)
    return re.sub(r"\s+", " ", title.lower()).strip()


def _v20_major_title_hit(row: pd.Series) -> tuple[bool, str, str]:
    """Return (hit, group, keyword) based on headline only."""
    title = _v20_title_blob(row)
    if not title:
        return False, "", ""

    # Avoid broad false positives: notice/regulation alone is not enough unless
    # the headline also contains a trade/customs/policy action term.
    action_terms = [
        "tariff", "customs", "duty", "quota", "origin", "fta", "cepa", "usmca",
        "export control", "entity list", "anti-dumping", "antidumping", "countervailing",
        "safeguard", "cbam", "hs code", "classification", "valuation", "관세", "통관",
        "쿼터", "원산지", "수출통제", "반덤핑", "상계관세", "품목분류", "탄소국경",
    ]

    for group, keywords in MAJOR_TITLE_KEYWORD_GROUPS.items():
        for kw in keywords:
            k = kw.lower()
            if k and k in title:
                if group == "NOTICE_POLICY" and not contains_any(title, action_terms):
                    continue
                return True, group, kw

    for kw in _v20_extra_major_keywords():
        if kw and kw in title:
            return True, "USER_EXTRA", kw
    return False, "", ""


def _v20_major_candidate_gate(row: pd.Series) -> bool:
    """Major News still needs basic quality gates."""
    hit, _group, _kw = _v20_major_title_hit(row)
    if not hit:
        return False
    if _v16_hard_noise(row):
        return False
    url = safe_url(row.get("BestLinkURL", "") or row.get("URL", ""))
    if not is_valid_link(url):
        return False
    max_age = int(os.getenv("GTI_STEP4_NEWS_MAX_AGE_DAYS", "3"))
    pub = _v16_parse_date(row.get("Publish Date", row.get("Date", "")))
    if pd.isna(pub):
        return False
    cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=max_age)
    if pub.normalize() < cutoff:
        return False
    return True


_ORIGINAL_V20_FINAL_WEIGHTED_TOP30 = _v16_final_weighted_top30

def _v16_final_weighted_top30(daily, audit, excluded):
    """v20 override: Major title keyword bucket first, then weighted Top50 fill."""
    if not GTI_MAJOR_TITLE_ENABLED:
        return _ORIGINAL_V20_FINAL_WEIGHTED_TOP30(daily, audit, excluded)

    frames = []
    for part in [daily, audit, excluded]:
        if isinstance(part, pd.DataFrame) and not part.empty:
            frames.append(part.copy())
    if not frames:
        return daily, audit, excluded

    pool = pd.concat(frames, ignore_index=True, sort=False)
    if "BestLinkURL" not in pool.columns:
        pool["BestLinkURL"] = pool.get("URL", "")
    pool["BestLinkURL"] = pool["BestLinkURL"].where(pool["BestLinkURL"].astype(str).str.strip().ne(""), pool.get("URL", ""))
    pool["Publish Date"] = pool.apply(lambda r: _v16_text(r.get("Publish Date")) or _v16_text(r.get("Date")), axis=1)
    pool["Date"] = pool["Publish Date"]

    pool["_v16_law30"] = pool.apply(_v16_customs_trade_law_score, axis=1)
    pool["_v16_policy20"] = pool.apply(_v16_customs_trade_policy_score, axis=1)
    pool["_v16_direct40"] = pool.apply(_v16_direct_impact_score, axis=1)
    pool["_v16_indirect10"] = pool.apply(_v16_indirect_impact_score, axis=1)
    pool["_v16_score"] = pool.apply(_v16_weighted_score, axis=1)
    pool["_v16_key"] = pool.apply(_v16_issue_key, axis=1)
    pool["_v16_noise"] = pool.apply(_v16_hard_noise, axis=1)
    pool[["MajorNewsFlag", "MajorNewsGroup", "MajorNewsKeyword"]] = pool.apply(
        lambda r: pd.Series(("Y", _v20_major_title_hit(r)[1], _v20_major_title_hit(r)[2])) if _v20_major_candidate_gate(r) else pd.Series(("N", "", "")),
        axis=1,
    )
    pool["_v20_major_bonus_score"] = pool["MajorNewsFlag"].apply(lambda v: GTI_MAJOR_TITLE_BONUS if v == "Y" else 0)
    pool["_v20_select_score"] = pool["_v16_score"] + pool["_v20_major_bonus_score"]

    max_age = int(os.getenv("GTI_STEP4_NEWS_MAX_AGE_DAYS", "3"))
    cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=max_age)
    pub = pool["Publish Date"].apply(_v16_parse_date)
    base_candidates = pool[
        (~pool["_v16_noise"])
        & (pool["_v16_score"] > 0)
        & pub.notna()
        & (pub.dt.normalize() >= cutoff)
    ].copy()

    major = pool[pool["MajorNewsFlag"].eq("Y")].copy()
    major = major.sort_values(["_v20_select_score", "_v16_score", "Publish Date"], ascending=[False, False, False])
    major = major.drop_duplicates(subset=["BestLinkURL"], keep="first")
    major = major.drop_duplicates(subset=["_v16_key"], keep="first")
    major = major.head(max(0, GTI_MAJOR_TITLE_MAX))

    top_n = min(int(os.getenv("GTI_STEP4_NEWS_TARGET_MAX", "50")), 50)
    major_urls = set(major["BestLinkURL"].fillna("").astype(str).str.lower().str.strip())
    fill = base_candidates[~base_candidates["BestLinkURL"].fillna("").astype(str).str.lower().str.strip().isin(major_urls)].copy()
    fill = fill.sort_values(["_v16_score", "Publish Date"], ascending=[False, False])
    fill = fill.drop_duplicates(subset=["BestLinkURL"], keep="first")
    fill = fill.drop_duplicates(subset=["_v16_key"], keep="first")
    selected = pd.concat([major, fill], ignore_index=True, sort=False).head(top_n).copy()
    selected = selected.sort_values(["MajorNewsFlag", "_v20_select_score", "_v16_score", "Publish Date"], ascending=[False, False, False, False]).reset_index(drop=True)

    selected["selected"] = "Y"
    selected["mail_section"] = selected["MajorNewsFlag"].apply(lambda v: "Major News" if v == "Y" else "News")
    selected["final_score"] = selected["_v20_select_score"].round().astype(int)
    selected["priority_group"] = selected.apply(
        lambda r: "MAJOR_NEWS" if r.get("MajorNewsFlag") == "Y" else ("CORE" if int(r.get("final_score", 0) or 0) >= 70 else "USABLE"),
        axis=1,
    )
    selected["Risk"] = selected["final_score"].apply(lambda v: "상" if int(v or 0) >= 70 else "중" if int(v or 0) >= 45 else "하")
    if "samsung_impact" in selected.columns:
        selected["samsung_impact"] = selected.apply(
            lambda r: "Watch" if r.get("MajorNewsFlag") == "Y" and _v16_text(r.get("samsung_impact")) in {"", "Reference"}
            else ("Direct" if r["_v16_direct40"] >= 40 else ("Indirect" if r["_v16_indirect10"] >= 10 else _v16_text(r.get("samsung_impact")) or "Watch")),
            axis=1,
        )
    if "Samsung Impact" in selected.columns:
        selected["Samsung Impact"] = selected.get("samsung_impact", "Watch")
    selected["rank"] = range(1, len(selected) + 1)
    selected["ScoreBreakdown"] = selected.apply(
        lambda r: f"major={r.get('MajorNewsFlag','N')}:{r.get('MajorNewsKeyword','')}; law30={int(r['_v16_law30'])}; policy20={int(r['_v16_policy20'])}; direct40={int(r['_v16_direct40'])}; indirect10={int(r['_v16_indirect10'])}; title_bonus={int(r['_v20_major_bonus_score'])}",
        axis=1,
    )
    selected.loc[selected["MajorNewsFlag"].eq("Y"), "SelectReason"] = selected.loc[selected["MajorNewsFlag"].eq("Y")].apply(
        lambda r: append_reason(r.get("SelectReason", ""), f"major_title_keyword:{r.get('MajorNewsGroup','')}:{r.get('MajorNewsKeyword','')}"),
        axis=1,
    )

    selected_urls = set(selected["BestLinkURL"].fillna("").astype(str).str.lower().str.strip())
    full_audit = pool.drop_duplicates(subset=[c for c in ["Headline", "BestLinkURL", "URL"] if c in pool.columns], keep="first").copy()
    full_audit["ScoreBreakdown"] = full_audit.apply(
        lambda r: f"major={r.get('MajorNewsFlag','N')}:{r.get('MajorNewsKeyword','')}; law30={int(r['_v16_law30'])}; policy20={int(r['_v16_policy20'])}; direct40={int(r['_v16_direct40'])}; indirect10={int(r['_v16_indirect10'])}; title_bonus={int(r['_v20_major_bonus_score'])}",
        axis=1,
    )
    full_audit["selected"] = full_audit["BestLinkURL"].fillna("").astype(str).str.lower().str.strip().apply(lambda u: "Y" if u in selected_urls else "N")
    full_audit.loc[full_audit["selected"].eq("Y") & full_audit["MajorNewsFlag"].eq("Y"), "mail_section"] = "Major News"
    full_audit.loc[full_audit["selected"].eq("Y") & full_audit["MajorNewsFlag"].ne("Y"), "mail_section"] = "News"
    full_audit.loc[full_audit["selected"].ne("Y"), "mail_section"] = "Excluded"
    full_audit.loc[full_audit["selected"].ne("Y"), "RejectReason"] = full_audit.loc[full_audit["selected"].ne("Y"), "RejectReason"].fillna("").astype(str).apply(
        lambda v: append_reason(v, "v20_major_title_or_weighted_below_topN_or_noise")
    )
    new_excluded = full_audit[full_audit["selected"].ne("Y")].copy()

    major_count = int(selected["MajorNewsFlag"].eq("Y").sum()) if "MajorNewsFlag" in selected.columns else 0
    log(f"Major-title v20 selected={len(selected)} / major={major_count} / fill={len(selected)-major_count} / target={top_n}")

    drop_cols = ["_v16_law30", "_v16_policy20", "_v16_direct40", "_v16_indirect10", "_v16_score", "_v16_key", "_v16_noise", "_v20_major_bonus_score", "_v20_select_score"]
    return (
        selected.drop(columns=drop_cols, errors="ignore").reset_index(drop=True),
        full_audit.drop(columns=drop_cols, errors="ignore").reset_index(drop=True),
        new_excluded.drop(columns=drop_cols, errors="ignore").reset_index(drop=True),
    )

# ======================================================================
# End of GTI STEP4-2 Major-News Title Keyword Patch v20
# ======================================================================


# ======================================================================
# GTI STEP4-2 Original URL Fallback Search Patch v21 - 2026-06-17
# ----------------------------------------------------------------------
# Goal
# - Check whether upstream raw files already provide original URLs.
# - If Step4 selected row still has only Google News home/redirect or no
#   usable original URL, search by Headline + Source/Publisher/Agency to
#   recover the original article URL.
# - Search fallback is used only for final selected rows, so 180-row scoring
#   remains fast.
# ======================================================================

GTI_ORIGINAL_URL_SEARCH_ENABLED = os.getenv("GTI_ORIGINAL_URL_SEARCH", "1").strip().upper() not in {"0", "N", "NO", "FALSE"}
GTI_ORIGINAL_URL_SEARCH_TIMEOUT = int(os.getenv("GTI_ORIGINAL_URL_SEARCH_TIMEOUT", "10"))
GTI_ORIGINAL_URL_SEARCH_MAX_CANDIDATES = int(os.getenv("GTI_ORIGINAL_URL_SEARCH_MAX_CANDIDATES", "8"))
GTI_ORIGINAL_URL_SEARCH_ENGINE = os.getenv("GTI_ORIGINAL_URL_SEARCH_ENGINE", "bing").strip().lower()

_BAD_SEARCH_RESULT_DOMAINS = [
    "google.com", "news.google.com", "bing.com", "microsoft.com", "duckduckgo.com",
    "facebook.com", "twitter.com", "x.com", "linkedin.com", "youtube.com",
    "instagram.com", "tiktok.com", "pinterest.com",
]

def _v21_domain_from_text(text: str) -> str:
    blob = clean(text)
    if not blob:
        return ""
    if not blob.lower().startswith(("http://", "https://")):
        blob = "https://" + blob
    try:
        p = urlparse(blob)
        host = (p.netloc or "").lower().strip()
        host = host[4:] if host.startswith("www.") else host
        if "." in host and not any(x in host for x in ["google", "bing", "duckduckgo"]):
            return host
    except Exception:
        return ""
    return ""

def _v21_domain_of_url(url: str) -> str:
    try:
        host = urlparse(safe_url(url)).netloc.lower().strip()
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return ""

def _v21_is_search_bad_url(url: str) -> bool:
    u = safe_url(url)
    if not is_valid_link(u):
        return True
    if is_google_article_redirect(u) or is_generic_google_main(u):
        return True
    host = _v21_domain_of_url(u)
    if not host:
        return True
    return any(bad == host or host.endswith("." + bad) for bad in _BAD_SEARCH_RESULT_DOMAINS)

def _v21_title_tokens(title: str) -> set[str]:
    t = normalize_title(title)
    toks = [x for x in re.split(r"\s+", t) if len(x) >= 3]
    return set(toks[:18])

def _v21_score_search_candidate(candidate_url: str, title: str, preferred_domain: str = "") -> int:
    if _v21_is_search_bad_url(candidate_url):
        return -999
    score = 10
    host = _v21_domain_of_url(candidate_url)
    if preferred_domain and (host == preferred_domain or host.endswith("." + preferred_domain) or preferred_domain.endswith("." + host)):
        score += 70
    url_norm = normalize_title(unquote(candidate_url))
    title_tokens = _v21_title_tokens(title)
    if title_tokens:
        hit = sum(1 for tok in title_tokens if tok in url_norm)
        score += min(30, hit * 5)
    if any(x in candidate_url.lower() for x in ["/article", "articleview", "news", "view", "html"]):
        score += 5
    return score

def _v21_extract_links_from_search_html(html_text: str) -> list[str]:
    links = []
    for m in re.finditer(r'<a[^>]+href=["\']([^"\']+)["\']', html_text or "", re.I):
        href = _html_unescape(m.group(1))
        if not href:
            continue
        if href.startswith("/url?") or "://www.google." in href and "/url?" in href:
            try:
                qs = urlparse(href).query
                for part in qs.split("&"):
                    if part.startswith("q=") or part.startswith("url="):
                        href = unquote(part.split("=", 1)[1])
                        break
            except Exception:
                pass
        if href.startswith("http://") or href.startswith("https://"):
            links.append(safe_url(href))
    # de-duplicate preserving order
    out = []
    seen = set()
    for u in links:
        key = u.lower().strip()
        if key not in seen:
            seen.add(key)
            out.append(u)
    return out

def _v21_search_web_for_original_url(title: str, source_hint: str = "", publisher_hint: str = "", agency_hint: str = "") -> tuple[str, str]:
    if not GTI_ORIGINAL_URL_SEARCH_ENABLED:
        return "", "SEARCH_DISABLED"
    title = clean(title)
    if not title:
        return "", "NO_TITLE_FOR_SEARCH"

    preferred_domain = ""
    for hint in [source_hint, publisher_hint, agency_hint]:
        preferred_domain = _v21_domain_from_text(hint)
        if preferred_domain:
            break

    query_parts = [title]
    if preferred_domain:
        query_parts.append(f"site:{preferred_domain}")
    else:
        for hint in [publisher_hint, agency_hint, source_hint]:
            h = clean(hint)
            if h and not h.lower().startswith(("http://", "https://")) and len(h) <= 60:
                query_parts.append(h)
                break
    query = " ".join(query_parts)

    try:
        if GTI_ORIGINAL_URL_SEARCH_ENGINE == "duckduckgo":
            search_url = "https://duckduckgo.com/html/?q=" + quote(query)
        else:
            search_url = "https://www.bing.com/search?q=" + quote(query)

        req = urllib.request.Request(
            search_url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/129 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            },
        )
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=GTI_ORIGINAL_URL_SEARCH_TIMEOUT, context=ctx) as resp:
            raw = resp.read(800_000)
        html_text = raw.decode("utf-8", "ignore")
        candidates = _v21_extract_links_from_search_html(html_text)[: max(1, GTI_ORIGINAL_URL_SEARCH_MAX_CANDIDATES * 3)]
        scored = []
        for u in candidates:
            score = _v21_score_search_candidate(u, title, preferred_domain)
            if score > 0:
                scored.append((score, u))
        scored.sort(reverse=True, key=lambda x: x[0])
        if scored:
            best = scored[0][1]
            return best, f"TITLE_SOURCE_WEB_SEARCH:{GTI_ORIGINAL_URL_SEARCH_ENGINE}:score={scored[0][0]}"
        return "", "SEARCH_NO_GOOD_RESULT"
    except Exception as exc:
        return "", f"SEARCH_FAILED:{type(exc).__name__}"

def _v21_best_original_url_from_row(row: pd.Series, allow_search: bool = True) -> tuple[str, str]:
    """Return original URL if available; otherwise optionally recover using title+source search."""
    # 1) Direct original/canonical URL columns first.
    for col, status in [
        ("BestLinkURL", "BEST_LINK"),
        ("canonical_url", "CANONICAL_URL"),
        ("original_url", "ORIGINAL_URL"),
        ("OriginalURLCandidate", "ORIGINAL_CANDIDATE"),
        ("original_url_candidate", "ORIGINAL_CANDIDATE"),
        ("URL", "URL"),
        ("url", "URL"),
    ]:
        v = safe_url(row.get(col, ""))
        if is_real_original_url(v):
            return v, status

    # 2) Try Google News redirect decode.
    for col in ["GoogleURL", "google_url", "URL", "url", "BestLinkURL"]:
        v = safe_url(row.get(col, ""))
        if is_google_article_redirect(v):
            resolved = resolve_google_news_url(v)
            if is_real_original_url(resolved):
                return resolved, "GOOGLE_NEWS_RESOLVED"

    # 3) Title + source/publisher/agency web search, only for final selected rows.
    if allow_search:
        title = clean(row.get("Headline", "")) or clean(row.get("title", ""))
        source_hint = clean(row.get("Source", "")) or clean(row.get("source", "")) or clean(row.get("rss_url", ""))
        publisher_hint = clean(row.get("Publisher", "")) or clean(row.get("publisher", ""))
        agency_hint = clean(row.get("Agency", "")) or clean(row.get("agency", ""))
        found, status = _v21_search_web_for_original_url(title, source_hint, publisher_hint, agency_hint)
        if is_real_original_url(found):
            return found, status
        return "", status

    return "", "NO_ORIGINAL_URL"

def _v21_log_input_url_coverage(df: pd.DataFrame) -> None:
    try:
        if df is None or df.empty:
            return
        cols = [c for c in ["URL", "url", "BestLinkURL", "canonical_url", "original_url", "OriginalURLCandidate", "original_url_candidate", "GoogleURL", "google_url"] if c in df.columns]
        if not cols:
            log("URL coverage: no URL-like columns found")
            return
        total = len(df)
        any_http = 0
        any_original = 0
        google_redirect = 0
        google_home = 0
        for _, r in df.iterrows():
            vals = [safe_url(r.get(c, "")) for c in cols]
            if any(is_valid_link(v) for v in vals):
                any_http += 1
            if any(is_real_original_url(v) for v in vals):
                any_original += 1
            if any(is_google_article_redirect(v) for v in vals):
                google_redirect += 1
            if any(is_generic_google_main(v) for v in vals):
                google_home += 1
        log(f"URL coverage: rows={total}, url_cols={cols}, any_valid_url={any_http}, original_url_ready={any_original}, google_redirect_rows={google_redirect}, google_home_rows={google_home}")
    except Exception as exc:
        log(f"URL coverage check failed: {type(exc).__name__}: {exc}")

# Wrap read_input only to log URL coverage without changing input data.
_ORIGINAL_V21_READ_INPUT = read_input
def read_input() -> pd.DataFrame:
    df = _ORIGINAL_V21_READ_INPUT()
    _v21_log_input_url_coverage(df)
    return df

# Wrap final Gemini enrichment so selected rows recover original URLs before body fetch/Gemini.
_ORIGINAL_V21_ENRICH_SELECTED_WITH_GEMINI = _v19_enrich_selected_with_gemini
def _v19_enrich_selected_with_gemini(daily: pd.DataFrame, audit: pd.DataFrame, excluded: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if isinstance(daily, pd.DataFrame) and not daily.empty:
        daily = daily.copy()
        limit = min(GTI_FINAL_GEMINI_MAX, len(daily))
        log(f"Original URL recovery start: selected={len(daily)}, check_limit={limit}, search_enabled={GTI_ORIGINAL_URL_SEARCH_ENABLED}")
        recovered = 0
        unresolved = 0
        for pos, idx in enumerate(daily.index[:limit], start=1):
            row = daily.loc[idx]
            url, status = _v21_best_original_url_from_row(row, allow_search=True)
            if url:
                recovered += 1
                for col in ["URL", "BestLinkURL", "original_url"]:
                    if col not in daily.columns:
                        daily[col] = ""
                    daily.at[idx, col] = url
                if "URL_Quality" not in daily.columns:
                    daily["URL_Quality"] = ""
                daily.at[idx, "URL_Quality"] = status
            else:
                unresolved += 1
                if "URL_Quality" not in daily.columns:
                    daily["URL_Quality"] = ""
                daily.at[idx, "URL_Quality"] = status
            if pos == 1 or pos % 10 == 0 or pos == limit:
                log(f"Original URL recovery {pos}/{limit}: recovered={recovered}, unresolved={unresolved}")
        if isinstance(audit, pd.DataFrame) and not audit.empty:
            audit = audit.copy()
            # Push recovered URLs back to audit by title key or existing URL key.
            if "_v21_title_key" not in daily.columns:
                daily["_v21_title_key"] = daily["Headline"].apply(normalize_title) if "Headline" in daily.columns else ""
            audit["_v21_title_key"] = audit["Headline"].apply(normalize_title) if "Headline" in audit.columns else ""
            map_url = daily.set_index("_v21_title_key")["BestLinkURL"].to_dict() if "BestLinkURL" in daily.columns else {}
            map_quality = daily.set_index("_v21_title_key")["URL_Quality"].to_dict() if "URL_Quality" in daily.columns else {}
            for col in ["URL", "BestLinkURL", "original_url", "URL_Quality"]:
                if col not in audit.columns:
                    audit[col] = ""
            audit["BestLinkURL"] = audit.apply(lambda r: map_url.get(r.get("_v21_title_key", ""), r.get("BestLinkURL", "")), axis=1)
            audit["URL"] = audit.apply(lambda r: map_url.get(r.get("_v21_title_key", ""), r.get("URL", "")), axis=1)
            audit["original_url"] = audit.apply(lambda r: map_url.get(r.get("_v21_title_key", ""), r.get("original_url", "")), axis=1)
            audit["URL_Quality"] = audit.apply(lambda r: map_quality.get(r.get("_v21_title_key", ""), r.get("URL_Quality", "")), axis=1)
            audit = audit.drop(columns=["_v21_title_key"], errors="ignore")
        daily = daily.drop(columns=["_v21_title_key"], errors="ignore")
        log(f"Original URL recovery done: recovered={recovered}, unresolved={unresolved}")
    return _ORIGINAL_V21_ENRICH_SELECTED_WITH_GEMINI(daily, audit, excluded)

# ======================================================================
# End of GTI STEP4-2 Original URL Fallback Search Patch v21
# ======================================================================



# ======================================================================
# GTI STEP4-2 Strict Final Guard v22 - 2026-06-18
# ----------------------------------------------------------------------
# Purpose
# - After weighted/major-title selection, re-apply hard rejects.
# - Recalibrate Samsung impact: country/policy keywords alone cannot be Direct.
# - Exclude weak URL recovery, Google home, search failure, YouTube/digest/noise.
# - Limit same report issue cluster to max 2.
# - Keep only high-quality final selected rows for Step5.
# ======================================================================

GTI_STRICT_FINAL_ENABLED = os.getenv("GTI_STRICT_FINAL_ENABLED", "1").strip().upper() not in {"0", "N", "NO", "FALSE"}
GTI_STRICT_NEWS_TARGET_MAX = int(os.getenv("GTI_STRICT_NEWS_TARGET_MAX", "30"))
GTI_STRICT_MAX_PER_ISSUE = int(os.getenv("GTI_STRICT_MAX_PER_ISSUE", "2"))

STRICT_URL_BAD_STATUSES = [
    "SEARCH_NO_GOOD_RESULT", "NO_ORIGINAL_URL", "GOOGLE_NEWS_REDIRECT_UNRESOLVED",
    "GOOGLE_UNRESOLVED", "GOOGLE_HOME", "EMPTY_OR_BAD_LINK", "non_original_or_google_home_url",
]

STRICT_HARD_REJECT_REASONS = [
    "event_training_tender_noise",
    "financial_industry_noise_without_trade_policy",
    "samsung_general_business_noise",
    "general_economy_without_samsung_policy",
    "low_value_general_news",
    "bilateral_industry_news_without_trade_policy",
    "ai_chip_industry_without_control_signal",
    "export_control_industry_without_control_signal",
    "google_news_original_url_unresolved",
    "future_date_abnormal",
    "no_valid_url",
    "v12_hard_reference_or_noise",
    "v12_no_customs_trade_action_signal",
]

STRICT_DIGEST_OR_LOWVALUE_TITLES = [
    "손바닥뉴스", "시장동향", "경제 아카데미", "포항상의", "염전 노예",
    "교황", "대통령", "순방", "호르무즈", "사설", "칼럼", "opinion", "editorial",
    "youtube", "뉴스) - youtube", "운임 인상", "기자회견", "정상회담", "외교",
    "business trip", "g7 정상", "기업 해결사", "한반도 구상",
    "돼지고기", "고등어", "오징어", "농산물", "쇠고기", "쌀", "설탕",
]

STRICT_STRONG_POLICY_TERMS = list(dict.fromkeys([
    "section 301", "301조", "section 232", "232조", "anti-dumping", "anti dumping", "antidumping",
    "countervailing", "ad/cvd", "safeguard", "cbam", "carbon border", "tariff-rate quota",
    "tariff quota", "duty-free quota", "export control", "entity list", "uflpa", "forced labor",
    "rules of origin", "hs code", "classification", "customs duty", "import duty", "melt and pour",
    "반덤핑", "상계관세", "무역구제", "세이프가드", "탄소국경", "수출통제", "강제노동",
    "원산지", "품목분류", "할당관세", "관세율", "무관세", "쿼터", "통관", "보세", "환급",
]))

STRICT_PRODUCT_TERMS = list(dict.fromkeys(PRODUCT_TERMS + [
    "steel", "aluminum", "battery", "semiconductor", "chip", "display", "mobile", "smartphone",
    "철강", "알루미늄", "배터리", "반도체", "디스플레이", "모바일", "스마트폰",
]))


def _strict_blob(row: pd.Series) -> str:
    return " ".join(clean(row.get(c, "")) for c in [
        "Headline", "Summary", "AI Analysis", "Action Plan", "ClusterHeadlines",
        "topic", "topic_keyword", "KeywordMatches", "Agency", "Publisher", "Source", "URL", "URL_Quality",
    ]).lower()

def _strict_original_signal_blob(row: pd.Series) -> str:
    # Do not use generated AI Analysis/Summary for Samsung Direct judgment,
    # because templates often contain "삼성전자" even when article does not.
    return " ".join(clean(row.get(c, "")) for c in [
        "Headline", "ClusterHeadlines", "article_body", "KeywordMatches", "Agency", "Publisher", "Source", "URL",
    ]).lower()


def _strict_has_any(text: str, terms: list[str]) -> bool:
    return any(str(t).lower() in text for t in terms if str(t).strip())


def _strict_reject_reason_set(row: pd.Series) -> set[str]:
    rr = clean(row.get("RejectReason", ""))
    return {x.strip() for x in rr.split(";") if x.strip()}


def _strict_is_bad_url(row: pd.Series) -> bool:
    urlq = clean(row.get("URL_Quality", "")).upper()
    url = safe_url(row.get("BestLinkURL", "")) or safe_url(row.get("URL", ""))
    if not is_valid_link(url):
        return True
    if is_generic_google_main(url):
        return True
    if "YOUTUBE.COM" in url.upper() or "YOUTU.BE" in url.upper():
        return True
    return any(x.upper() in urlq for x in STRICT_URL_BAD_STATUSES)


def _strict_recalibrate_impact(row: pd.Series) -> str:
    # Use only original article/title/source signals for impact recalibration.
    # Generated Summary/AI Analysis often contains generic "Samsung" wording and should not
    # create Direct/Indirect impact or hard policy signal by itself.
    original_blob = _strict_original_signal_blob(row)
    strong_policy = _strict_has_any(original_blob, STRICT_STRONG_POLICY_TERMS)
    samsung_exact = _strict_has_any(original_blob, SAMSUNG_EXACT_TERMS)
    product = _strict_has_any(original_blob, STRICT_PRODUCT_TERMS)
    official = is_official_source(row)
    if samsung_exact and strong_policy:
        return "Direct"
    if product and strong_policy:
        return "Indirect"
    if official and strong_policy:
        return "Watch"
    if strong_policy:
        return "Watch"
    return "Reference"


def _strict_issue_key(row: pd.Series) -> str:
    # Prefer existing report issue key logic when available; normalize broad repeated topics.
    try:
        key = report_issue_key(row)
    except Exception:
        key = clean(row.get("cluster_key", "")) or clean(row.get("Headline", ""))
    blob = _strict_blob(row)
    if "india" in blob and ("uk" in blob or "britain" in blob) and ("fta" in blob or "ceta" in blob or "trade agreement" in blob):
        return "ORIGIN_FTA:india_uk_fta"
    if "eu" in blob and ("steel" in blob or "철강" in blob) and ("safeguard" in blob or "세이프가드" in blob or "quota" in blob or "쿼터" in blob):
        return "TARIFF:eu_steel_safeguard"
    if "section 301" in blob or "301조" in blob:
        return "TARIFF:section_301"
    if "cbam" in blob or "탄소국경" in blob:
        return "CBAM:cbam"
    if "korea" in blob and ("mongol" in blob or "몽골" in blob) and "cepa" in blob:
        return "ORIGIN_FTA:korea_mongolia_cepa"
    return normalize_title(key)[:100]


def _strict_apply_final_guard(daily: pd.DataFrame, audit: pd.DataFrame, excluded: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not GTI_STRICT_FINAL_ENABLED or audit is None or audit.empty:
        return daily, audit, excluded
    audit = audit.copy()
    for c in ["selected", "priority_group", "mail_section", "RejectReason", "samsung_impact", "samsung_impact_score", "final_score", "Risk"]:
        if c not in audit.columns:
            audit[c] = ""

    # Recalibrate all impacts first.
    audit["samsung_impact"] = audit.apply(_strict_recalibrate_impact, axis=1)
    audit["samsung_impact_score"] = audit["samsung_impact"].map({"Direct": 100, "Indirect": 78, "Watch": 58, "Reference": 20}).fillna(20)

    reject_idx = []
    for idx, row in audit.iterrows():
        # Reject decisions must be based on original article signals, not Gemini-generated prose.
        blob = _strict_original_signal_blob(row)
        rr = _strict_reject_reason_set(row)
        reasons = []
        if _strict_is_bad_url(row):
            reasons.append("strict_bad_or_unresolved_url")
        if rr and any(h in rr for h in STRICT_HARD_REJECT_REASONS):
            # Older ranking/weak-Samsung reasons are broad screening hints, not final hard rejects.
            # Keep rows when the article still has concrete customs/trade law signal and a reportable
            # Samsung impact class (Direct/Indirect/Watch). This preserves Samsung customs-work impact
            # sensing without mailing pure Reference/noise items.
            broad_prior_reasons = {
                "v12_hard_reference_or_noise",
                "v12_no_customs_trade_action_signal",
                "weighted_v18_not_topN_or_noise",
                "v20_major_title_or_weighted_below_topN_or_noise",
                "weak_samsung_relevance",
                "report_issue_duplicate_compressed",
                "expanded_policy_watch",
                "strict_existing_hard_reject",
            }
            reportable_trade_signal = (
                _strict_has_any(blob, STRICT_STRONG_POLICY_TERMS)
                and clean(row.get("samsung_impact")) in {"Direct", "Indirect", "Watch"}
            )
            if (not rr <= broad_prior_reasons) or (not reportable_trade_signal):
                reasons.append("strict_existing_hard_reject")
        if _strict_has_any(blob, STRICT_DIGEST_OR_LOWVALUE_TITLES):
            reasons.append("strict_digest_politics_market_noise")
        if not _strict_has_any(blob, STRICT_STRONG_POLICY_TERMS):
            reasons.append("strict_no_concrete_customs_trade_signal")
        if clean(row.get("samsung_impact")) == "Reference":
            reasons.append("strict_reference_not_reportable")
        if reasons:
            reject_idx.append((idx, reasons))

    for idx, reasons in reject_idx:
        audit.at[idx, "selected"] = "N"
        audit.at[idx, "priority_group"] = "EXCLUDED"
        audit.at[idx, "mail_section"] = "Excluded"
        audit.at[idx, "Risk"] = "하"
        try:
            audit.at[idx, "final_score"] = min(int(float(audit.at[idx, "final_score"] or 0)), MIN_SELECT_SCORE - 1)
        except Exception:
            audit.at[idx, "final_score"] = MIN_SELECT_SCORE - 1
        for reason in reasons:
            audit.at[idx, "RejectReason"] = append_reason(audit.at[idx, "RejectReason"], reason)

    # Keep selected rows only after final guard, then max 2 per issue.
    audit["_strict_issue_key"] = audit.apply(_strict_issue_key, axis=1)
    selected = audit[audit["selected"].eq("Y")].copy()
    if not selected.empty:
        selected = selected.sort_values(["samsung_impact_score", "WeightedScore" if "WeightedScore" in selected.columns else "final_score", "final_score", "Date"], ascending=[False, False, False, False])
        dup_mask = selected.groupby("_strict_issue_key").cumcount() >= GTI_STRICT_MAX_PER_ISSUE
        dup_idx = selected[dup_mask].index
        if len(dup_idx):
            audit.loc[dup_idx, "selected"] = "N"
            audit.loc[dup_idx, "priority_group"] = "EXCLUDED"
            audit.loc[dup_idx, "mail_section"] = "Excluded"
            audit.loc[dup_idx, "RejectReason"] = audit.loc[dup_idx, "RejectReason"].apply(lambda v: append_reason(v, "strict_issue_duplicate_max2"))

    # Final target cap.
    selected = audit[audit["selected"].eq("Y")].copy()
    if not selected.empty:
        sort_cols = [c for c in ["samsung_impact_score", "WeightedScore", "final_score", "topic_score", "Date"] if c in selected.columns]
        selected = selected.sort_values(sort_cols, ascending=[False] * len(sort_cols))
        keep_idx = set(selected.head(GTI_STRICT_NEWS_TARGET_MAX).index)
        over_idx = [idx for idx in selected.index if idx not in keep_idx]
        if over_idx:
            audit.loc[over_idx, "selected"] = "N"
            audit.loc[over_idx, "priority_group"] = "EXCLUDED"
            audit.loc[over_idx, "mail_section"] = "Excluded"
            audit.loc[over_idx, "RejectReason"] = audit.loc[over_idx, "RejectReason"].apply(lambda v: append_reason(v, "strict_over_news_target_max"))

    daily = audit[audit["selected"].eq("Y")].copy()
    if not daily.empty:
        sort_cols = [c for c in ["samsung_impact_score", "WeightedScore", "final_score", "topic_score", "Date"] if c in daily.columns]
        daily = daily.sort_values(sort_cols, ascending=[False] * len(sort_cols)).reset_index(drop=True)
        daily["rank"] = range(1, len(daily) + 1)
        daily["priority_group"] = daily["samsung_impact"].apply(lambda x: "CORE" if x == "Direct" else "POLICY_WATCH" if x == "Watch" else "USABLE")
        daily["mail_section"] = daily["samsung_impact"].apply(lambda x: "News Core" if x == "Direct" else "Policy Watch" if x == "Watch" else "News Usable")
    audit = audit.drop(columns=["_strict_issue_key"], errors="ignore")
    audit = audit.sort_values(["selected", "samsung_impact_score", "final_score"], ascending=[False, False, False]).reset_index(drop=True)
    audit["rank"] = range(1, len(audit) + 1)
    excluded = audit[audit["selected"].ne("Y")].copy()

    for frame in [daily, audit, excluded]:
        for col in OUTPUT_COLS:
            if col not in frame.columns:
                frame[col] = ""
    log(f"Strict final guard v22: selected={len(daily)} / audit={len(audit)} / excluded={len(excluded)} / target_max={GTI_STRICT_NEWS_TARGET_MAX}")
    extra_cols = [c for c in ["CustomsTradeLawScore", "CustomsTradePolicyScore", "DirectImpactScore", "IndirectImpactScore", "WeightedScore", "ScoreBreakdown"] if c in audit.columns]
    out_cols = list(dict.fromkeys(OUTPUT_COLS + extra_cols))
    return daily[out_cols], audit[out_cols], excluded[out_cols]



# ======================================================================
# GTI STEP4-2 TITLE KEYWORD STRICT GUARD v23
# - Selected news must have a customs/trade keyword in the original title.
# - This prevents summary/Gemini-only relevance from pushing weak items into Top news.
# ======================================================================

GTI_NEWS_TITLE_KEYWORD_REQUIRED = os.getenv("GTI_NEWS_TITLE_KEYWORD_REQUIRED", "Y").strip().upper() not in {"N", "NO", "0", "FALSE"}

_V23_NEWS_TITLE_KEYWORDS = [
    "관세", "통관", "세관", "수입", "수출", "수출입", "무역", "통상", "관세율", "추가관세",
    "반덤핑", "덤핑", "상계관세", "무역구제", "세이프가드", "쿼터", "무관세",
    "원산지", "품목분류", "hs", "hs코드", "fta", "cepa", "usmca", "협정", "특혜관세", "관세환급", "환급",
    "보세", "수입신고", "수출신고", "전략물자", "수출통제", "제재", "강제노동", "cbam", "탄소국경",
    "customs", "tariff", "tariffs", "duty", "duties", "import", "export", "trade", "section 301", "section 232",
    "anti-dumping", "anti dumping", "antidumping", "countervailing", "ad/cvd", "safeguard", "quota", "duty-free",
    "rules of origin", "origin", "hs code", "classification", "fta", "cepa", "usmca", "cbam", "carbon border",
    "export control", "entity list", "forced labor", "uflpa", "federal register", "notice", "regulation",
]


def _v23_news_title_keywords() -> list[str]:
    extra = os.getenv("GTI_TITLE_KEYWORDS", "").strip()
    terms = list(_V23_NEWS_TITLE_KEYWORDS)
    if extra:
        terms.extend([x.strip() for x in re.split(r"[;,|]", extra) if x.strip()])
    out = []
    seen = set()
    for t in terms:
        k = clean(t).lower()
        if k and k not in seen:
            out.append(k)
            seen.add(k)
    return sorted(out, key=len, reverse=True)


def _v23_title_has_keyword(row: pd.Series) -> bool:
    title = clean(row.get("Headline")).lower()
    if not title:
        return False
    return any(kw in title for kw in _v23_news_title_keywords())


_v23_strict_apply_final_guard_base = _strict_apply_final_guard


def _strict_apply_final_guard(daily: pd.DataFrame, audit: pd.DataFrame, excluded: pd.DataFrame):
    daily, audit, excluded = _v23_strict_apply_final_guard_base(daily, audit, excluded)
    if not GTI_NEWS_TITLE_KEYWORD_REQUIRED or audit.empty:
        return daily, audit, excluded

    audit = audit.copy()
    selected_mask = audit.get("selected", "").astype(str).str.upper().eq("Y") if "selected" in audit.columns else pd.Series(False, index=audit.index)
    no_title_kw = audit.apply(lambda r: not _v23_title_has_keyword(r), axis=1)
    drop_idx = audit[selected_mask & no_title_kw].index
    if len(drop_idx):
        audit.loc[drop_idx, "selected"] = "N"
        audit.loc[drop_idx, "priority_group"] = "EXCLUDED"
        audit.loc[drop_idx, "mail_section"] = "Excluded"
        audit.loc[drop_idx, "RejectReason"] = audit.loc[drop_idx, "RejectReason"].apply(lambda v: append_reason(v, "v23_no_title_keyword"))
        log(f"Title keyword strict guard v23 removed selected news without title keyword: {len(drop_idx)}")

    # If strict filtering leaves too few items, backfill with reportable Samsung customs-impact
    # candidates. Backfill still enforces original URL quality, title keyword, original article
    # customs/trade signal, and excludes event/finance/general-noise rows.
    current_selected = audit["selected"].astype(str).str.upper().eq("Y")
    if current_selected.sum() < GTI_STRICT_NEWS_TARGET_MAX:
        hard_backfill_tokens = (
            "event_training_tender_noise",
            "financial_industry_noise_without_trade_policy",
            "samsung_general_business_noise",
            "general_economy_without_samsung_policy",
            "low_value_general_news",
            "strict_digest_politics_market_noise",
            "strict_no_concrete_customs_trade_signal",
            "strict_reference_not_reportable",
            "strict_bad_or_unresolved_url",
            "google_news_original_url_unresolved",
            "no_valid_url",
            "old_news>",
        )

        def _v23_backfill_keep(row: pd.Series) -> bool:
            rr_low = clean(row.get("RejectReason")).lower()
            if any(tok in rr_low for tok in hard_backfill_tokens):
                return False
            if _strict_is_bad_url(row):
                return False
            if GTI_NEWS_TITLE_KEYWORD_REQUIRED and not _v23_title_has_keyword(row):
                return False
            blob = _strict_original_signal_blob(row)
            if not _strict_has_any(blob, STRICT_STRONG_POLICY_TERMS):
                return False
            impact = clean(row.get("samsung_impact")) or _strict_recalibrate_impact(row)
            return impact in {"Direct", "Indirect", "Watch"}

        fill = audit[~current_selected].copy()
        if not fill.empty:
            fill = fill[fill.apply(_v23_backfill_keep, axis=1)].copy()
        if not fill.empty:
            if "WeightedScore" not in fill.columns:
                fill["WeightedScore"] = 0

            def _strict_num(value: object) -> float:
                try:
                    if pd.isna(value):
                        return 0.0
                    return float(value)
                except Exception:
                    return 0.0

            fill["_strict_backfill_score"] = fill.apply(
                lambda r: _strict_num(r.get("samsung_impact_score")) * 1000
                + _strict_num(r.get("WeightedScore")) * 10
                + _strict_num(r.get("final_score"))
                + _strict_num(r.get("topic_score")),
                axis=1,
            )
            sort_cols = ["_strict_backfill_score"] + [c for c in ["Date"] if c in fill.columns]
            fill = fill.sort_values(sort_cols, ascending=[False] * len(sort_cols))
            need = max(0, GTI_STRICT_NEWS_TARGET_MAX - int(current_selected.sum()))
            fill_idx = fill.head(need).index
            if len(fill_idx):
                audit.loc[fill_idx, "selected"] = "Y"
                audit.loc[fill_idx, "Risk"] = audit.loc[fill_idx, "Risk"].replace({"?": "?", "": "?"})
                audit.loc[fill_idx, "priority_group"] = audit.loc[fill_idx, "samsung_impact"].apply(lambda x: "CORE" if x == "Direct" else "POLICY_WATCH" if x == "Watch" else "USABLE")
                audit.loc[fill_idx, "mail_section"] = audit.loc[fill_idx, "samsung_impact"].apply(lambda x: "News Core" if x == "Direct" else "Policy Watch" if x == "Watch" else "News Usable")
                audit.loc[fill_idx, "RejectReason"] = audit.loc[fill_idx, "RejectReason"].apply(lambda v: append_reason(v, "strict_backfill_reportable_policy"))
                log(f"Title keyword strict guard v23 backfilled reportable news: {len(fill_idx)}")

    daily = audit[audit["selected"].astype(str).str.upper().eq("Y")].copy()
    if not daily.empty:
        sort_cols = [c for c in ["samsung_impact_score", "WeightedScore", "final_score", "topic_score", "Date"] if c in daily.columns]
        daily = daily.sort_values(sort_cols, ascending=[False] * len(sort_cols)).reset_index(drop=True)
        daily["rank"] = range(1, len(daily) + 1)
    excluded2 = audit[audit["selected"].astype(str).str.upper().ne("Y")].copy()

    for frame in [daily, audit, excluded2]:
        for col in OUTPUT_COLS:
            if col not in frame.columns:
                frame[col] = ""
    return daily, audit, excluded2

# ======================================================================
# End of GTI STEP4-2 TITLE KEYWORD STRICT GUARD v23
# ======================================================================

# ======================================================================
# End of GTI STEP4-2 Strict Final Guard v22
# ======================================================================

def main() -> None:
    print("[STEP4-2] News analysis start - GUARDRAIL v4.1")
    _gti_step4_gemini_log_once()
    _gti_step4_v10_log_once()
    log(f"Gemini model candidates: {GEMINI_MODEL_CANDIDATES}")
    _gti_step4_extractor_log_once()
    df = read_input()
    daily, audit, excluded = build(df)
    daily, audit, excluded = _v16_final_weighted_top30(daily, audit, excluded)
    daily, audit, excluded = _strict_apply_final_guard(daily, audit, excluded)
    daily, audit, excluded = _v19_enrich_selected_with_gemini(daily, audit, excluded)
    daily, audit, excluded = _strict_apply_final_guard(daily, audit, excluded)
    cumulative = merge_cumulative(daily)
    legacy = to_legacy(daily)
    write_excel(daily, OUT_SUMMARY)
    write_excel(cumulative, OUT_CUMULATIVE)
    write_excel(audit, OUT_AUDIT)
    write_excel(excluded, OUT_EXCLUDED)
    write_excel(legacy, OUT_LEGACY)
    print(f"[DONE] Daily: {OUT_SUMMARY}")
    print(f"[DONE] Cumulative: {OUT_CUMULATIVE}")
    print(f"[DONE] Audit: {OUT_AUDIT}")
    print(f"[DONE] Excluded: {OUT_EXCLUDED}")
    print(f"[DONE] Legacy: {OUT_LEGACY}")
    print(f"[ROWS] selected={len(daily)} / audit={len(audit)} / excluded={len(excluded)} / cumulative={len(cumulative)}")


if __name__ == "__main__":
    main()
