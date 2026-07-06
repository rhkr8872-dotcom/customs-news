# -*- coding: utf-8 -*-
# GTI FINAL CORE v5 - Gemini regulation analysis, LAW1 only
"""
GTI STEP4-1 LAW1-ONLY REGULATION AI ANALYSIS - GUARDRAIL v4.1

Fixes
- Exclude stale regulations/notices older than GTI_STEP4_REG_MAX_AGE_DAYS (default 90).
- Exclude webinar/seminar/tender/opening ceremony/event notices.
- Exclude bad URLs such as fonts.googleapis / analytics.
- Do not misread arbitrary percentages as tariff rates.
- Keep only customs/trade/FTA/export-control/CBAM/AD-CVD/HS regulation items.
"""
from __future__ import annotations

import os
import re
import json
import ssl
import urllib.request
import urllib.error
from pathlib import Path
from datetime import datetime
from urllib.parse import quote, unquote, urlparse

import pandas as pd

BASE_DIR = Path(os.getenv("GTI_BASE_DIR", r"C:\Temp"))
INPUT_FILE = BASE_DIR / "3-1.regulation_article_summary.xlsx"  # generated only from 1-1.regulation_raw.xlsx
KEYWORD_FILE = BASE_DIR / "keyword.xlsx"
OUT_SUMMARY = BASE_DIR / "4-1.regulation_ai_summary.xlsx"
OUT_CUMULATIVE = BASE_DIR / "4-1.regulation_ai_cumulative.xlsx"
OUT_EXCLUDED = BASE_DIR / "4-1.regulation_ai_excluded.xlsx"

MAX_AGE_DAYS = int(os.getenv("GTI_STEP4_REG_MAX_AGE_DAYS", "90"))
TOP_N_MAX = int(os.getenv("GTI_STEP4_REG_TOP_N_MAX", "9999"))
MIN_SCORE = int(os.getenv("GTI_STEP4_REG_MIN_SCORE", "70"))
KEYWORD_MIN_LEN = int(os.getenv("GTI_STEP4_REG_KEYWORD_MIN_LEN", "2"))

BAD_URL_PATTERNS = ["google-analytics.com", "googletagmanager.com", "doubleclick.net", "analytics.js", "fonts.googleapis.com", "fonts.gstatic.com", "googleusercontent.com", "googleadservices.com"]
EVENT_NOISE_TERMS = [
    "webinar", "seminar", "conference", "summit", "workshop", "training", "education", "lecture", "forum", "symposium",
    "registration", "tender", "call for tender", "rfp", "expo", "opening ceremony", "ceremony", "join the upcoming",
    "live streaming",
    "웨비나", "세미나", "컨퍼런스", "서밋", "워크숍", "교육", "강의", "설명회", "포럼", "입찰", "공모", "행사", "참가신청",
]
TOPIC_RULES = [
    ("AD_CVD", ["anti-dumping", "anti dumping", "antidumping", "countervailing", "countervailing duty", "countervailing duties", "ad/cvd", "cvd", "dumping duties", "반덤핑", "덤핑방지관세", "상계관세", "무역구제"]),
    ("EXPORT_CONTROL", ["export control", "export controls", "entity list", "denied persons", "bureau of industry and security", "수출통제", "전략물자", "제재", "산업안보국", "산업보안국"]),
    ("CBAM_CARBON", ["cbam", "carbon border", "carbon border adjustment", "탄소국경"]),
    ("ORIGIN_FTA", ["fta", "cepa", "usmca", "rules of origin", "origin", "원산지", "자유무역협정", "tepa"]),
    ("HS_CLASSIFICATION", ["hs code", "classification", "tariff classification", "품목분류", "hs코드"]),
    ("TARIFF", ["section 301", "301조", "section 232", "232조", "reciprocal tariff", "tariff", "tariffs", "customs duty", "import duty", "관세", "관세율", "추가관세", "상호관세"]),
    ("CUSTOMS", ["customs", "clearance", "declaration", "통관", "세관", "관세청"]),
]
TOPIC_KR = {"EXPORT_CONTROL":"수출통제", "AD_CVD":"반덤핑/상계관세", "CBAM_CARBON":"CBAM", "ORIGIN_FTA":"FTA/원산지", "HS_CLASSIFICATION":"HS/품목분류", "TARIFF":"관세정책", "CUSTOMS":"통관/세관", "TRADE_GENERAL":"무역일반"}

STRICT_TRADE_REG_TERMS = [
    "관세", "관세율", "관세청", "통관", "세관", "보세", "수입신고", "수출신고",
    "품목분류", "hs code", "hs코드", "원산지", "fta", "자유무역협정", "cepa",
    "anti-dumping", "antidumping", "countervailing", "ad/cvd", "반덤핑", "덤핑방지관세",
    "상계관세", "무역구제", "수출통제", "전략물자", "entity list", "cbam", "carbon border",
    "customs", "tariff", "tariffs", "customs duty", "import duty", "section 301", "section 232",
]

SOFT_TRADE_REG_TERMS = [
    "import", "importation", "export", "exportation", "exporters", "trade notice", "public notice",
    "trade", "e-commerce exporters", "export obligation", "import and export", "dgft", "cbic",
    "federal register", "notice of request", "information collection", "approval", "regulation",
    "수입", "수출", "무역", "통상", "공고", "고시", "입법예고", "행정예고",
]

CONCRETE_TRADE_REG_TERMS = [
    "import", "importation", "export", "exportation", "exporters", "e-commerce exporters",
    "export obligation", "import and export", "fta", "tepa", "cepa", "safeguard",
    "anti-dumping", "antidumping", "countervailing", "ad/cvd", "tariff", "customs duty",
    "import duty", "rules of origin", "hs code", "classification",
    "수입", "수출", "원산지", "관세", "반덤핑", "상계관세", "무역구제", "세이프가드",
]

GENERIC_NOTICE_ONLY_TERMS = {"notice", "public notice", "regulation", "law", "act", "decree", "공고", "고시"}

OFFICIAL_TRADE_AGENCY_TERMS = [
    "관세청", "관세법령", "유니패스", "customs", "cbp", "ustr", "usitc", "wto", "wco",
    "taxud", "trade", "commerce", "mofcom", "dgft", "cbic", "meti", "gacc",
]

GENERAL_LAW_NOISE_TERMS = [
    "민사소송법", "형사소송법", "도로교통법", "남녀고용평등", "고용보험", "장애인고용",
    "공직선거법", "주택임대차보호법", "자동차관리법", "건설기술 진흥법", "고압가스 안전관리법",
    "전자장치 부착", "제대군인", "농어업인 삶의 질", "가맹사업거래", "국가연구개발혁신법",
]

PURE_REGULATION_TERMS = [
    "regulation", "rule", "rules", "law", "decree", "ordinance", "notice", "public notice",
    "trade notice", "federal register", "determination under", "investigation", "anti-dumping",
    "antidumping", "countervailing", "customs duty", "import duty", "export obligation",
    "법", "법률", "법령", "시행령", "시행규칙", "규칙", "고시", "공고", "훈령", "예규",
    "행정규칙", "입법예고", "행정예고", "덤핑방지관세", "상계관세", "무역구제",
    "관세율", "관세법", "보세", "통관", "수출입고시", "수입규제", "수출규제",
]

LEGAL_FORM_TITLE_TERMS = [
    "regulation", "rule", "rules", "law", "decree", "ordinance", "notice", "public notice",
    "trade notice", "federal register", "determination under", "investigation",
    "법", "법률", "법령", "시행령", "시행규칙", "규칙", "고시", "공고", "훈령", "예규",
    "행정규칙", "입법예고", "행정예고", "덤핑방지관세", "상계관세", "무역구제", "지급요령",
]

POLICY_NOTICE_NOISE_TERMS = [
    "press release", "briefing", "presidentview", "pressreleaseview", "newsid=",
    "speech", "remarks", "interview", "meeting", "delegation", "cooperation",
    "support team", "task force", "one-stop", "statistics", "provisional",
    "보도자료", "브리핑", "정상회담", "주요 성과", "성과", "면담", "대표단",
    "협력", "지원팀", "원스톱", "신설", "수출입 현황", "잠정치", "발표",
    "청장", "대통령", "경제 분야", "관세 행정 지원",
    "안내", "guidelines", "credit assistance", "support for emerging",
]

PURE_REGULATION_SOURCE_TERMS = [
    "law.go.kr", "unipass.customs.go.kr/clip", "federalregister.gov", "dgft.gov.in",
    "content.dgft.gov.in", "customs.go.jp", "mof.go.jp", "world.moleg.go.kr",
    "clhs.co.kr/law", "법령", "행정규칙", "고시", "공고", "입법예고", "행정예고",
]

UNIPASS_NOTICE_FORCE_TERMS = [
    "유니패스", "유니패스(공지사항)", "unipass", "unipass.customs.go.kr",
]

INDIRECT_CUSTOMS_TAX_LAW_TERMS = [
    "조세특례제한법", "조세특례제한법 일부개정법률안",
    "관세감면", "관세 면제", "수입부가세", "수입 부가가치세", "부가가치세 영세율",
    "개별소비세", "농어촌특별세", "세액공제", "면세",
    "tax exemption", "tax incentive", "special taxation", "customs exemption",
    "import vat", "vat exemption", "zero-rated vat",
]

POLICY_BRIEFING_NEWS_TERMS = [
    "정책브리핑", "korea.kr/briefing/pressreleaseview", "pressreleaseview.do",
    "press release", "보도자료",
]

BIS_VALID_CONTEXT = [
    "bis", "bureau of industry and security", "department of commerce", "commerce department",
    "entity list", "denied persons", "export control", "수출통제", "산업안보국", "산업보안국",
]

OUTPUT_COLS = [
    "No", "Content Type", "Mail Group", "Samsung Impact", "Affected Subsidiary", "Impact Reason", "Date", "Headline", "Summary", "AI Analysis", "Action Plan", "Country", "Agency", "Risk", "Importance Score", "Priority Group", "Issue", "Cluster", "URL", "Source", "Source File", "RejectReason", "KeywordMatches", "effective_date_hint", "hs_hint", "tariff_rate_hint"
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

def log(msg): print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}")
def clean(v): return "" if pd.isna(v) else str(v).strip()
def contains_any(text, terms):
    t = str(text or "").lower()
    return any(term.lower() in t for term in terms)

def contains_term(text, term):
    t = normalize_text(text)
    k = normalize_text(term)
    if not k:
        return False
    if re.fullmatch(r"[a-z0-9/.-]{2,5}", k):
        return re.search(rf"(?<![a-z0-9]){re.escape(k)}(?![a-z0-9])", t) is not None
    return k in t

def contains_terms(text, terms):
    return any(contains_term(text, term) for term in terms)

def normalize_text(v):
    return re.sub(r"\s+", " ", clean(v)).lower().strip()

def load_keyword_terms():
    if not KEYWORD_FILE.exists():
        return []
    try:
        df = pd.read_excel(KEYWORD_FILE)
        df = normalize_columns(df)
        active_col = pick_col(df, ["active", "use", "enabled"])
        if active_col:
            active = df[active_col].fillna("Y").astype(str).str.upper().str.strip()
            df = df[active.isin(["Y", "YES", "TRUE", "1"])]

        keyword_cols = [
            col for col in df.columns
            if "keyword" in str(col).lower() or str(col).lower() in ["kr", "en", "cn", "vi", "hi", "tr", "es", "pt"]
        ]
        terms = []
        for col in keyword_cols:
            terms.extend(df[col].dropna().astype(str).str.strip().tolist())

        broad_noise = {"수출", "수입", "무역", "통상", "세관", "customs", "trade", "import", "export", "bis", "aeo", "sta", "epa"}
        cleaned = []
        for term in terms:
            t = normalize_text(term)
            if len(t) < KEYWORD_MIN_LEN:
                continue
            if t in broad_noise:
                continue
            cleaned.append(term.strip())
        return sorted(set(cleaned), key=lambda x: x.lower())
    except Exception as exc:
        log(f"WARN keyword load failed: {KEYWORD_FILE} / {exc}")
        return []

KEYWORD_TERMS = []

def keyword_match_terms(text):
    terms = KEYWORD_TERMS or []
    t = normalize_text(text)
    return [term for term in terms if contains_term(t, term)]

def has_bis_valid_context(text):
    t = normalize_text(text)
    if not re.search(r"\bbis\b", t):
        return False
    return contains_any(t, BIS_VALID_CONTEXT)

def has_strict_trade_reg_signal(text, row=None):
    t = normalize_text(text)
    if contains_terms(t, STRICT_TRADE_REG_TERMS):
        return True
    if has_bis_valid_context(t):
        return True
    if keyword_match_terms(t):
        return True
    if row is not None:
        agency = normalize_text(row.get("Agency", row.get("agency", "")))
        source = normalize_text(row.get("Source", row.get("source", "")))
        if contains_terms(f"{agency} {source}", OFFICIAL_TRADE_AGENCY_TERMS):
            return contains_terms(t, ["notice", "regulation", "law", "act", "decree", "고시", "공고", "예고", "규칙", "법령", "관세", "통관"])
    return False

def source_trade_reg_signal(row, text):
    t = normalize_text(text)
    meta_blob = normalize_text(" ".join(clean(row.get(c, "")) for c in [
        "official_regulation_type",
        "official_regulation_reason",
        "protected_regulation_reason",
        "matched_policy_terms",
        "Agency",
        "agency",
        "Source",
        "source",
    ]))
    official_type = normalize_text(row.get("official_regulation_type", ""))
    protected_score = 0
    try:
        protected_score = int(float(clean(row.get("protected_regulation_score", 0)) or 0))
    except Exception:
        protected_score = 0

    if "official_trade_regulation" in official_type and contains_terms(meta_blob + " " + t, CONCRETE_TRADE_REG_TERMS):
        return True
    if contains_terms(meta_blob, STRICT_TRADE_REG_TERMS):
        return True
    if protected_score >= 80 and contains_terms(meta_blob + " " + t, CONCRETE_TRADE_REG_TERMS):
        return True
    if contains_terms(meta_blob, OFFICIAL_TRADE_AGENCY_TERMS) and contains_terms(t, CONCRETE_TRADE_REG_TERMS):
        return True
    return False

def soft_trade_keyword_hits(row, text):
    hits = keyword_match_terms(text)
    t = normalize_text(text)
    meta = normalize_text(" ".join(clean(row.get(c, "")) for c in [
        "matched_policy_terms",
        "official_regulation_reason",
        "protected_regulation_reason",
    ]))
    for term in CONCRETE_TRADE_REG_TERMS:
        if contains_term(t, term) or contains_term(meta, term):
            hits.append(term)
    return sorted(set(hits), key=lambda x: x.lower())

def is_general_law_noise(text):
    t = normalize_text(text)
    if not contains_terms(t, GENERAL_LAW_NOISE_TERMS):
        return False
    return not has_strict_trade_reg_signal(t)

def is_unipass_notice_candidate(row):
    blob = normalize_text(" ".join(clean(row.get(c, "")) for c in [
        "Agency", "agency", "Source", "source", "site_name", "URL", "url", "original_url",
    ]))
    return contains_terms(blob, UNIPASS_NOTICE_FORCE_TERMS)

def is_indirect_customs_tax_law(row, text):
    blob = normalize_text(" ".join([
        clean(row.get("Headline", row.get("title", ""))),
        clean(row.get("Agency", row.get("agency", ""))),
        clean(row.get("Source", row.get("source", ""))),
        clean(text),
    ]))
    if not contains_terms(blob, INDIRECT_CUSTOMS_TAX_LAW_TERMS):
        return False
    return contains_terms(blob, LEGAL_FORM_TITLE_TERMS) or contains_terms(blob, ["법률안", "일부개정법률안", "개정안"])

def is_policy_briefing_press_release(row):
    blob = normalize_text(" ".join(clean(row.get(c, "")) for c in [
        "Headline", "title", "Agency", "agency", "Source", "source", "URL", "url", "original_url",
    ]))
    return contains_terms(blob, POLICY_BRIEFING_NEWS_TERMS)

def is_pure_regulation_candidate(row, text, topic):
    t = normalize_text(text)
    headline = normalize_text(row.get("Headline", row.get("title", "")))
    url = normalize_text(row.get("URL", row.get("url", row.get("original_url", ""))))
    agency = normalize_text(row.get("Agency", row.get("agency", "")))
    source = normalize_text(row.get("Source", row.get("source", "")))
    official_type = normalize_text(row.get("official_regulation_type", ""))
    meta = normalize_text(" ".join(clean(row.get(c, "")) for c in [
        "official_regulation_reason",
        "protected_regulation_reason",
        "matched_policy_terms",
        "date_status",
    ]))
    blob = " ".join([headline, url, agency, source, official_type, meta, t])

    if is_policy_briefing_press_release(row):
        return False

    if is_unipass_notice_candidate(row):
        return True

    if is_indirect_customs_tax_law(row, text):
        return True

    if contains_terms(blob, POLICY_NOTICE_NOISE_TERMS) and not contains_terms(headline, LEGAL_FORM_TITLE_TERMS):
        return False

    if topic in {"AD_CVD", "ORIGIN_FTA", "HS_CLASSIFICATION"} and contains_terms(blob, PURE_REGULATION_TERMS):
        return True

    if "official_trade_regulation" in official_type and contains_terms(blob, PURE_REGULATION_TERMS):
        return True

    if contains_terms(url + " " + source + " " + agency, PURE_REGULATION_SOURCE_TERMS) and contains_terms(blob, PURE_REGULATION_TERMS):
        return True

    if contains_terms(headline, PURE_REGULATION_TERMS) and has_strict_trade_reg_signal(text, row):
        return True

    return False

def is_old_ad_cvd_review(topic, text, age_days):
    if age_days is None or age_days <= MAX_AGE_DAYS:
        return False
    return topic == "AD_CVD" or contains_terms(text, ["anti-dumping", "antidumping", "countervailing", "ad/cvd", "반덤핑", "상계관세"])
def normalize_columns(df):
    df = df.copy(); df.columns = [str(c).strip() for c in df.columns]
    return df.loc[:, ~pd.Index(df.columns).duplicated()]
def parse_dt(v):
    try:
        dt = pd.to_datetime(v, errors="coerce")
        if pd.isna(dt): return pd.NaT
        if getattr(dt, "tzinfo", None) is not None: dt = dt.tz_convert(None)
        return dt
    except Exception: return pd.NaT

def is_valid_url(url):
    u = safe_url(url)
    if not u.lower().startswith(("http://", "https://")): return False
    low = u.lower()
    return not any(p in low for p in BAD_URL_PATTERNS)

def safe_url(url):
    u = clean(url).replace("\r", "").replace("\n", "").strip()
    if not u:
        return ""
    return quote(unquote(u), safe=":/?#[]@!$&'()*+,;=%")

def pick_col(df, names):
    lower = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower: return lower[n.lower()]
    return None

def row_text(row):
    cols = ["Headline", "title", "Summary", "article_body", "regulation_fallback_body", "Agency", "Source", "matched_policy_terms", "official_regulation_reason"]
    return " ".join(clean(row.get(c, "")) for c in cols).lower()

def detect_topic(text):
    for topic, terms in TOPIC_RULES:
        if contains_terms(text, terms): return topic
    return "TRADE_GENERAL"

def extract_tariff_rate(text):
    # Only accept percentages close to tariff/duty/rate/관세율/세율 context. Avoid CBAM random 98/3/2/5/0 percentages.
    rates = []
    for m in re.finditer(r"(tariff|duty|rate|관세율|세율|관세)[^\n\r]{0,40}?([0-9]{1,2}(?:\.[0-9]+)?\s*%)", text, re.I):
        try:
            num = float(m.group(2).replace('%','').strip())
            if 0 < num <= 50:
                rates.append(m.group(2).replace(' ', ''))
        except Exception:
            pass
    return "; ".join(dict.fromkeys(rates)) if rates else "본문에서 확인 불가"

def action_for(topic):
    if topic == "EXPORT_CONTROL": return "수출통제팀", "BIS/Entity List/ECCN/거래상대방 스크리닝 영향 여부를 확인하십시오."
    if topic == "AD_CVD": return "통관운영/관세팀", "대상 HS·공급국·공급자·가격자료 기준 AD/CVD 적용 가능성을 점검하십시오."
    if topic == "CBAM_CARBON": return "ESG/구매/통관", "CBAM 대상 품목, 공급사 탄소자료, EU 신고 증빙 체계를 점검하십시오."
    if topic == "ORIGIN_FTA": return "FTA팀", "원산지 기준·CO 발급·수입 FTA 적용 및 증빙자료 영향을 확인하십시오."
    if topic == "HS_CLASSIFICATION": return "HS/통관팀", "품목분류 기준 변경 및 HS Master 영향 여부를 확인하십시오."
    if topic == "TARIFF": return "통관운영/FTA팀", "관세율·시행일·대상국·대상품목을 확인하고 원가 영향을 점검하십시오."
    return "통관운영", "업무 관련성 확인 후 모니터링하십시오."

def score_row(row):
    text = row_text(row)
    topic = detect_topic(text)
    headline = clean(row.get("Headline", row.get("title", "")))
    url = safe_url(row.get("URL", row.get("url", row.get("original_url", ""))))
    if not url: url = safe_url(row.get("original_url", ""))
    date_val = row.get("Date", row.get("date", ""))
    dt = parse_dt(date_val)
    now = pd.Timestamp(datetime.now())
    age_days = None if pd.isna(dt) else (now - dt).total_seconds() / 86400
    rejects = []
    keyword_hits = soft_trade_keyword_hits(row, text)
    metadata_trade_signal = source_trade_reg_signal(row, text)
    policy_briefing_news = is_policy_briefing_press_release(row)
    unipass_notice_force = is_unipass_notice_candidate(row)
    indirect_tax_law_force = is_indirect_customs_tax_law(row, text) and not policy_briefing_news
    forced_customs_trade_regulation = unipass_notice_force or indirect_tax_law_force
    strict_trade_signal = has_strict_trade_reg_signal(text, row) or metadata_trade_signal or forced_customs_trade_regulation
    old_ad_cvd_review = is_old_ad_cvd_review(topic, text, age_days)
    pure_regulation = is_pure_regulation_candidate(row, text, topic)

    if not is_valid_url(url): rejects.append("no_valid_url")
    if age_days is not None and age_days > MAX_AGE_DAYS:
        rejects.append(f"old_regulation>{MAX_AGE_DAYS}d")
        if old_ad_cvd_review:
            rejects.append("review_preserve_ad_cvd_old_date")
    if age_days is not None and age_days < -30: rejects.append("future_date_abnormal")
    event_text = (headline + " " + clean(row.get("article_body", ""))[:500] + " " + clean(row.get("regulation_fallback_body", ""))[:500]).lower()
    if contains_any(event_text, EVENT_NOISE_TERMS) and not metadata_trade_signal and not forced_customs_trade_regulation:
        rejects.append("event_training_tender_noise")
    if is_general_law_noise(text) and not metadata_trade_signal and not forced_customs_trade_regulation:
        rejects.append("general_law_not_customs_trade")
    if policy_briefing_news:
        rejects.append("policy_briefing_press_release_to_news")
    if not strict_trade_signal:
        rejects.append("not_customs_trade_keyword")
    if not pure_regulation:
        rejects.append("policy_notice_not_pure_regulation")
    if topic == "TRADE_GENERAL" and not keyword_hits and not metadata_trade_signal:
        rejects.append("weak_trade_policy_signal")

    base_map = {"EXPORT_CONTROL":100,"AD_CVD":96,"CBAM_CARBON":90,"ORIGIN_FTA":88,"HS_CLASSIFICATION":86,"TARIFF":84,"CUSTOMS":74,"TRADE_GENERAL":72 if keyword_hits else 30}
    base = base_map.get(topic, 30)
    if age_days is None and metadata_trade_signal:
        recency = 85
    else:
        recency = 100 if age_days is not None and age_days <= 30 else 85 if age_days is not None and age_days <= 60 else 70 if age_days is not None and age_days <= MAX_AGE_DAYS else 0
    score = round(base*0.75 + recency*0.25)
    if metadata_trade_signal and topic == "TRADE_GENERAL":
        score = max(score, 70)
    if forced_customs_trade_regulation:
        score = max(score, 72)
        if unipass_notice_force:
            keyword_hits.append("UNIPASS_NOTICE_FORCE_INCLUDE")
        if indirect_tax_law_force:
            keyword_hits.append("INDIRECT_CUSTOMS_TAX_LAW")
    if keyword_hits and not rejects:
        score = max(score, 72)
    if rejects:
        if "review_preserve_ad_cvd_old_date" in rejects:
            score = min(score, 55)
        else:
            score = min(score, 45 if "event_training_tender_noise" in rejects else 50)
    selected = not rejects and score >= MIN_SCORE
    owner, action = action_for(topic)
    risk = "상" if score >= 85 else "중" if score >= 70 else "하"
    issue = TOPIC_KR.get(topic, topic)
    impact = "Watch" if selected else "Reference"
    products_text = clean(row.get("affected_products", "")) or "본문에서 확인 불가"
    analysis = build_gti_ai_analysis(
        row,
        headline=headline,
        url=url,
        issue=issue,
        impact=impact,
        products_text=products_text,
        default_action=action,
        content_type="Regulation",
    )
    return {"selected": selected, "RejectReason": "; ".join(rejects), "Issue": issue, "topic": topic, "score": score, "Risk": risk, "URL": url, "Headline": headline, "Date": clean(date_val), "Agency": clean(row.get("Agency", row.get("agency", ""))), "Source": clean(row.get("Source", row.get("source", ""))), "Summary": analysis.get("Summary", ""), "AI Analysis": analysis.get("AI Analysis", ""), "Action Plan": analysis.get("Action Plan", action), "Owner": owner, "KeywordMatches": "; ".join(keyword_hits[:12]), "tariff_rate_hint": extract_tariff_rate(text), "effective_date_hint": clean(row.get("effective_date_hint", "본문에서 확인 불가")) or "본문에서 확인 불가", "hs_hint": clean(row.get("hs_hint", "본문에서 확인 불가")) or "본문에서 확인 불가", "article_extract_status": analysis.get("article_extract_status", "")}

def read_input():
    if not INPUT_FILE.exists(): raise FileNotFoundError(f"input not found: {INPUT_FILE}")
    df = normalize_columns(pd.read_excel(INPUT_FILE))
    log(f"LOAD {INPUT_FILE}: {len(df)} rows")
    # normalize common caps for scoring
    if "Headline" not in df.columns and "title" in df.columns: df["Headline"] = df["title"]
    if "URL" not in df.columns and "url" in df.columns: df["URL"] = df["url"]
    if "Date" not in df.columns and "date" in df.columns: df["Date"] = df["date"]
    if "Agency" not in df.columns and "agency" in df.columns: df["Agency"] = df["agency"]
    if "Source" not in df.columns and "source" in df.columns: df["Source"] = df["source"]
    return df

def build(df):
    rows=[]
    for _, row in df.iterrows():
        s=score_row(row)
        rows.append(s)
    audit=pd.DataFrame(rows)
    selected_all=audit[audit["selected"]].copy().sort_values(["score","Date"], ascending=[False, False]).reset_index(drop=True)
    selected=selected_all.head(TOP_N_MAX).copy().reset_index(drop=True)
    over_top=selected_all.iloc[TOP_N_MAX:].copy()
    if not over_top.empty:
        over_top["selected"] = False
        over_top["RejectReason"] = over_top["RejectReason"].fillna("").astype(str).map(lambda x: "over_top_n" if not x else f"{x}; over_top_n")
    excluded=pd.concat([audit[~audit["selected"]].copy(), over_top], ignore_index=True, sort=False).reset_index(drop=True)
    return selected, excluded, audit

def to_output(df, content_type="Regulation"):
    rows=[]
    for i,r in df.reset_index(drop=True).iterrows():
        impact = "Watch"
        rows.append({
            "No": i+1, "Content Type": content_type, "Mail Group": "Regulation" if content_type=="Regulation" else "News - 주요/참고",
            "Samsung Impact": impact, "Affected Subsidiary": "관련 법인 검토", "Impact Reason": "official_trade_regulation_watch",
            "Date": r["Date"], "Headline": r["Headline"], "Summary": r["Summary"], "AI Analysis": r["AI Analysis"], "Action Plan": r["Action Plan"],
            "Country": "", "Agency": r["Agency"], "Risk": r["Risk"], "Importance Score": int(r["score"]), "Priority Group": "CORE" if int(r["score"])>=85 else "USABLE",
            "Issue": r["Issue"], "Cluster": r["Headline"], "URL": r["URL"], "Source": r["Source"], "Source File": "3-1.regulation_article_summary.xlsx",
            "RejectReason": r.get("RejectReason", ""), "KeywordMatches": r.get("KeywordMatches", ""), "effective_date_hint": r.get("effective_date_hint", "본문에서 확인 불가"), "hs_hint": r.get("hs_hint", "본문에서 확인 불가"), "tariff_rate_hint": r.get("tariff_rate_hint", "본문에서 확인 불가")
        })
    return pd.DataFrame(rows, columns=OUTPUT_COLS)

def normalize_cum_cols(df):
    df=normalize_columns(df)
    for c in OUTPUT_COLS:
        if c not in df.columns: df[c]=""
    return df[OUTPUT_COLS]

def merge_cumulative(daily):
    if OUT_CUMULATIVE.exists():
        try:
            old=normalize_cum_cols(pd.read_excel(OUT_CUMULATIVE)); log(f"cumulative existing load: {len(old)} rows")
        except Exception: old=pd.DataFrame(columns=OUTPUT_COLS)
    else:
        old=pd.DataFrame(columns=OUTPUT_COLS); log("cumulative file missing -> new create")
    daily=normalize_cum_cols(daily)
    combined=pd.concat([old,daily], ignore_index=True, sort=False)
    key=combined["URL"].fillna("").astype(str).str.lower().str.strip()
    title=combined["Headline"].fillna("").astype(str).str.lower().str.strip()
    combined["_key"]=key.where(key.ne(""), title)
    combined=combined.drop_duplicates(subset=["_key"], keep="last").drop(columns=["_key"], errors="ignore")
    return normalize_cum_cols(combined)

def write_excel(df,path):
    path.parent.mkdir(parents=True, exist_ok=True)
    try: df.to_excel(path,index=False)
    except PermissionError:
        alt=path.with_name(path.stem+f"_{datetime.now():%Y%m%d_%H%M%S}"+path.suffix); df.to_excel(alt,index=False); log(f"SAVE fallback: {alt}")


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
    """v10 regulation-safe postprocess.

    This file is STEP4-1 regulation analysis.  A previous patch reused the
    STEP4-2 news variable NEWS_TARGET_MAX here, which raises NameError in
    regulation mode.  Regulations should keep all selected official notices
    up to TOP_N_MAX.
    """
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
    reg_target_max = int(globals().get("TOP_N_MAX", 9999))
    out = out.head(reg_target_max).reset_index(drop=True)
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
# GTI STEP4-1 Regulation Sensing Patch v11 - 2026-06-14
# ----------------------------------------------------------------------
# 목적: 정부/공식기관 신규 게시물 중 관세/통상 업무 변화 후보를
#      과도하게 탈락시키지 않고 보고서 단계까지 보존한다.
# 기본조건:
# - 게시날짜(Publish Date) 보존
# - 공식 법규/공지 URL 보존
# - AD/CVD, FTA/원산지, 수출통제, 통관/보세, 관세정책은 old date라도 review 보존
# - weak_trade_policy_signal 하나만으로 공식기관 공지를 제외하지 않음
# ======================================================================

if "Publish Date" not in OUTPUT_COLS:
    try:
        OUTPUT_COLS.insert(OUTPUT_COLS.index("Date") + 1, "Publish Date")
    except Exception:
        OUTPUT_COLS.append("Publish Date")


def _v11_text(row) -> str:
    return " ".join(clean(row.get(c, "")) for c in [
        "Headline", "Summary", "AI Analysis", "Action Plan", "Agency", "Source",
        "URL", "KeywordMatches", "Issue", "RejectReason"
    ]).lower()


def _v11_official_trade_source(row) -> bool:
    blob = _v11_text(row)
    return any(k in blob for k in [
        "dgft", "customs", "cbic", "cbp", "ustr", "usitc", "wco", "wto", "taxud",
        "federalregister.gov", "law.go.kr", "unipass", "mof.go.jp", "customs.go.jp",
        "관세", "세관", "통관", "유니패스", "국가법령", "법령", "고시", "공고",
    ])


def _v11_actionable_reg_topic(row) -> bool:
    blob = _v11_text(row)
    return any(k in blob for k in [
        "anti-dumping", "antidumping", "countervailing", "ad/cvd", "dumping", "반덤핑", "상계관세", "덤핑방지",
        "fta", "cepa", "tepa", "origin", "rules of origin", "원산지",
        "export control", "entity list", "forced labor", "uflpa", "수출통제", "전략물자",
        "customs", "clearance", "declaration", "bonded", "통관", "보세", "수입신고", "수출신고",
        "tariff", "customs duty", "import duty", "quota", "관세", "관세율", "쿼터",
        "hs code", "classification", "품목분류",
        "export obligation", "advance authorization", "epcg", "e-commerce exporters",
    ])


def _v11_clear_non_customs_noise(row) -> bool:
    blob = _v11_text(row)
    if "securities exchange act" in blob or "regulation nms" in blob:
        return True
    if any(k in blob for k in ["webinar", "seminar", "conference", "training", "tender", "recruit"]):
        return True
    return False


def _v11_reference_like(row) -> bool:
    blob = _v11_text(row)
    return any(k in blob for k in [
        "rate of exchange", "exchange rate", "과세환율",
        "modalities for export of wheat", "export of wheat", "wheat reg",
    ])


def _v11_recoverable_regulation(row) -> bool:
    reason = clean(row.get("RejectReason", ""))
    if not reason:
        return False
    if _v11_clear_non_customs_noise(row):
        return False
    if _v11_actionable_reg_topic(row):
        return True
    if _v11_official_trade_source(row) and any(r in reason for r in [
        "weak_trade_policy_signal",
        "policy_notice_not_pure_regulation",
        "review_preserve_ad_cvd_old_date",
        "old_regulation",
    ]):
        return True
    return False


def _v11_adjust_recovered_row(row: pd.Series) -> pd.Series:
    row = row.copy()
    blob = _v11_text(row)
    old_reason = clean(row.get("RejectReason", ""))
    row["selected"] = True
    row["RejectReason"] = ("v11_recovered_official_regulation" if not old_reason else f"v11_recovered_official_regulation; previous={old_reason}")
    row["score"] = max(int(float(row.get("score", 0) or 0)), 62 if _v11_reference_like(row) else 72)
    row["Risk"] = "하" if _v11_reference_like(row) else ("상" if row["score"] >= 85 else "중")
    if _v11_reference_like(row):
        row["Issue"] = "Reference"
    elif any(k in blob for k in ["anti-dumping", "antidumping", "countervailing", "반덤핑", "상계관세"]):
        row["Issue"] = "반덤핑/상계관세"
    elif any(k in blob for k in ["fta", "cepa", "tepa", "origin", "원산지"]):
        row["Issue"] = "FTA/원산지"
    elif any(k in blob for k in ["export control", "entity list", "수출통제"]):
        row["Issue"] = "수출통제"
    elif any(k in blob for k in ["customs", "clearance", "bonded", "통관", "보세"]):
        row["Issue"] = "통관/세관"
    elif any(k in blob for k in ["tariff", "관세", "quota"]):
        row["Issue"] = "관세정책"
    return row


try:
    _ORIGINAL_BUILD_V11_REG = build

    def build(df: pd.DataFrame):
        selected, excluded, audit = _ORIGINAL_BUILD_V11_REG(df)
        if excluded is None or excluded.empty:
            return selected, excluded, audit

        recover = excluded[excluded.apply(_v11_recoverable_regulation, axis=1)].copy()
        if not recover.empty:
            recover = recover.apply(_v11_adjust_recovered_row, axis=1)
            selected = pd.concat([selected, recover], ignore_index=True, sort=False)
            selected["_v11_key"] = selected["URL"].fillna("").astype(str).str.lower().str.strip()
            selected["_v11_key"] = selected["_v11_key"].where(selected["_v11_key"].ne(""), selected["Headline"].fillna("").astype(str).str.lower().str.strip())
            selected = selected.drop_duplicates(subset=["_v11_key"], keep="first").drop(columns=["_v11_key"], errors="ignore")
            selected = selected.sort_values(["score", "Date"], ascending=[False, False]).reset_index(drop=True)

            recover_keys = set(recover["URL"].fillna("").astype(str).str.lower().str.strip())
            excluded = excluded[~excluded["URL"].fillna("").astype(str).str.lower().str.strip().isin(recover_keys)].reset_index(drop=True)
        return selected, excluded, audit
except Exception:
    pass


# ======================================================================
# GTI STEP4-1 Regulation Sensing Patch v12 - 2026-06-14
# ----------------------------------------------------------------------
# 목적 기준 보완:
# - Publish Date가 NaN/공란이면 "확인 필요"로 표시한다.
# - 법규는 목록에 보존하되, 관세/통상 실행 키워드가 없는 일반 법규는
#   Top3 오승격을 막기 위해 Reference로 분류한다.
# ======================================================================

def _v12_clean_text(v) -> str:
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return clean(v)


def _v12_publish_date(row) -> str:
    for c in ["Publish Date", "Date", "published", "published_at", "collected_at", "last_checked"]:
        v = _v12_clean_text(row.get(c, ""))
        if v and v.lower() not in {"nan", "nat", "none"}:
            return v
    return "확인 필요"


def _v12_reg_blob(row) -> str:
    return " ".join(_v12_clean_text(row.get(c, "")) for c in [
        "Headline", "Title", "Summary", "AI Analysis", "Action Plan",
        "OriginalArticle", "article_body", "body", "KeywordMatches", "Agency", "Source", "URL",
    ]).lower()


# ======================================================================
# GTI STEP4-1 Regulation Sensing Patch v13 - 2026-06-14
# ----------------------------------------------------------------------
# UNIPASS 공지 URL 보정:
# rowTitle 검색 URL은 메일/엑셀에서 클릭 실패 가능성이 높으므로 ntarId 직접열람
# URL로 변환한다.  추가 ntarId는 이 사전에 계속 확장하면 된다.
# ======================================================================

UNIPASS_NOTICE_URL_PREFIX_V13 = (
    "https://unipass.customs.go.kr/csp/myc/custsppt/cmmn/"
    "NtarBrkdMtCtr/openMYC0605014Q.do?ntarId="
)

UNIPASS_NOTICE_ID_BY_TITLE_V13 = {
    "다수 사업장 운영 사업자의 전자상거래업자 등록 신청 방법": "202606122928",
}


def _v13_normalize_title(text: str) -> str:
    text = unquote(_v12_clean_text(text))
    text = re.sub(r"\([^)]*\)|\[[^]]*\]", " ", text)
    text = re.sub(r"[^0-9A-Za-z가-힣]+", " ", text)
    return re.sub(r"\s+", " ", text).strip().lower()


def _v13_extract_row_title_from_url(url: str) -> str:
    u = _v12_clean_text(url)
    m = re.search(r"[?&]rowTitle=([^&]+)", u, flags=re.I)
    return unquote(m.group(1)) if m else ""


def _v13_unipass_direct_url(row) -> str:
    values = [
        row.get("Headline", ""),
        row.get("Title", ""),
        _v13_extract_row_title_from_url(row.get("URL", "")),
        _v13_extract_row_title_from_url(row.get("Source", "")),
    ]
    norm_values = [_v13_normalize_title(v) for v in values if _v12_clean_text(v)]
    for key, ntar_id in UNIPASS_NOTICE_ID_BY_TITLE_V13.items():
        norm_key = _v13_normalize_title(key)
        if any(norm_key and (norm_key in v or v in norm_key) for v in norm_values):
            return UNIPASS_NOTICE_URL_PREFIX_V13 + ntar_id
    return ""


def _v13_fix_unipass_url(row) -> str:
    direct = _v13_unipass_direct_url(row)
    if direct:
        return direct
    url = _v12_clean_text(row.get("URL", ""))
    low = url.lower()
    if "unipass.customs.go.kr" in low and "openmyc0605014q.do" in low and "ntarid=" in low:
        return url
    return url


def _v12_reg_issue(row) -> str:
    text = _v12_reg_blob(row)
    title = _v12_clean_text(row.get("Headline", "")).lower()
    # 제목만으로 참고성 공지임이 분명한 항목은 넓은 fallback/keyword 문구보다 먼저 강등한다.
    if any(k in title for k in ["rate of exchange", "exchange rate", "과세환율", "export of wheat", "wheat reg"]):
        return "Reference"
    if any(k in title for k in [
        "인공지능이용자보호", "인공지능", "ai user", "agricultural policy", "política agrícola",
        "공동농업정책", "농업정책",
    ]):
        return "Reference"
    generic_notice = (
        "public notice" in title or "publick notice" in title or "trade notice" in title or "trrade notice" in title
        or any(k in title for k in ["credit assistance", "emerging export opportunities", "interest subvention", "collateral support", "bank validation", "testing inspections", "labsetu"])
    )
    if generic_notice:
        concrete_title = any(k in title for k in [
            "anti-dumping", "antidumping", "countervailing", "ad/cvd", "덤핑", "상계관세",
            "export obligation", "advance authorization", "epcg",
            "fta", "cepa", "tepa", "origin", "원산지",
            "customs duty", "import duty", "tariff", "관세", "통관", "보세",
        ])
        if not concrete_title:
            return "Reference"
    if any(k in text for k in ["anti-dumping", "antidumping", "countervailing", "ad/cvd", "덤핑", "상계관세", "덤핑방지"]):
        return "반덤핑/상계관세"
    if any(k in text for k in ["export control", "entity list", "uflpa", "forced labor", "수출통제", "전략물자", "강제노동"]):
        return "수출통제"
    if any(k in text for k in ["cbam", "carbon border", "탄소국경"]):
        return "CBAM"
    if (
        any(k in text for k in ["fta", "cepa", "tepa", "rules of origin", "certificate of origin", "원산지", "협정세율"])
        or re.search(r"\borigin\b", text)
    ):
        return "FTA/원산지"
    if any(k in text for k in [
        "export obligation", "advance authorization", "epcg", "e-commerce exporter", "electronic commerce exporter",
        "customs clearance", "customs declaration", "bonded", "bonded warehouse",
        "수출의무", "전자상거래업자", "통관절차", "수입신고", "수출신고", "보세", "보세창고", "과세가격",
    ]):
        return "통관/세관"
    if any(k in text for k in ["customs duty", "import duty", "tariff rate", "tariff quota", "hs code", "classification", "관세율", "할당관세", "품목분류"]):
        return "관세정책"
    return "Reference"


def to_output(df, content_type="Regulation"):
    rows = []
    for i, r in df.reset_index(drop=True).iterrows():
        issue = _v12_reg_issue(r)
        impact = "Reference" if issue == "Reference" else "Watch"
        pub_date = _v12_publish_date(r)
        rows.append({
            "No": i + 1,
            "Content Type": content_type,
            "Mail Group": "Regulation" if content_type == "Regulation" else "News",
            "Samsung Impact": impact,
            "Affected Subsidiary": "관련 법인 검토",
            "Impact Reason": "official_trade_regulation_watch",
            "Date": pub_date,
            "Publish Date": pub_date,
            "Headline": r.get("Headline", ""),
            "Summary": r.get("Summary", ""),
            "AI Analysis": r.get("AI Analysis", ""),
            "Action Plan": r.get("Action Plan", ""),
            "Country": "",
            "Agency": r.get("Agency", ""),
            "Risk": r.get("Risk", ""),
            "Importance Score": int(float(r.get("score", 0) or 0)),
            "Priority Group": "REFERENCE" if issue == "Reference" else ("CORE" if int(float(r.get("score", 0) or 0)) >= 85 else "USABLE"),
            "Issue": issue,
            "Cluster": r.get("Headline", ""),
            "URL": _v13_fix_unipass_url(r),
            "Source": r.get("Source", ""),
            "Source File": "3-1.regulation_article_summary.xlsx",
            "RejectReason": r.get("RejectReason", ""),
            "KeywordMatches": r.get("KeywordMatches", ""),
            "effective_date_hint": r.get("effective_date_hint", "본문에서 확인 불가"),
            "hs_hint": r.get("hs_hint", "본문에서 확인 불가"),
            "tariff_rate_hint": r.get("tariff_rate_hint", "본문에서 확인 불가"),
        })
    return pd.DataFrame(rows, columns=OUTPUT_COLS)


def normalize_cum_cols(df):
    df = normalize_columns(df)
    for c in OUTPUT_COLS:
        if c not in df.columns:
            df[c] = ""
    if "Publish Date" in df.columns:
        pub = df["Publish Date"].astype(str).str.strip()
        dt = df.get("Date", "").astype(str).str.strip() if "Date" in df.columns else ""
        df["Publish Date"] = df["Publish Date"].where(pub.ne("") & ~pub.str.lower().isin(["nan", "nat", "none"]), dt)
        df["Publish Date"] = df["Publish Date"].where(df["Publish Date"].astype(str).str.strip().ne(""), "확인 필요")
    if "Date" in df.columns:
        df["Date"] = df["Date"].where(df["Date"].astype(str).str.strip().ne("") & ~df["Date"].astype(str).str.lower().isin(["nan", "nat", "none"]), df.get("Publish Date", "확인 필요"))
    return df[OUTPUT_COLS]


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
    parts = []
    for col in [
        "Headline", "Summary", "AI Analysis", "Action Plan", "Issue",
        "Samsung Impact", "Agency", "Source", "RejectReason",
        "KeywordMatches", "URL", "Publish Date", "Date",
    ]:
        if col in row.index:
            parts.append(_v15_text(row.get(col)))
    return " ".join(parts).lower()


def _v15_parse_date(value):
    text = _v15_text(value)
    if not text or text.lower() in {"nan", "nat", "none"}:
        return pd.NaT
    if "확인" in text or "?" in text:
        return pd.NaT
    return pd.to_datetime(text, errors="coerce")


def _v15_is_pn51(row):
    return "pn 51" in _v15_blob(row) and "export obligation" in _v15_blob(row)


def _v15_is_reportable_regulation(row):
    """Final gate for executive regulation report rows."""
    blob = _v15_blob(row)
    headline = _v15_text(row.get("Headline")).lower()
    issue = _v15_text(row.get("Issue"))
    impact = _v15_text(row.get("Samsung Impact"))

    if _v15_is_pn51(row):
        return True

    hard_noise = [
        "rate of exchange",
        "public notice no.",
        "public notice no ",
        "public notice ",
        "public notice eng",
        "public notice english",
        "ai이용자보호",
        "인공지능이용자보호",
        "공동농업정책",
        "securities exchange act",
        "regulation nms",
    ]
    if any(k in headline for k in hard_noise):
        strong_exception = any(k in blob for k in [
            "anti-dumping", "antidumping", "countervailing", "safeguard",
            "customs duty", "tariff quota", "rules of origin", "certificate of origin",
            "export obligation", "advance authorization", "epcg",
            "반덤핑", "상계관세", "덤핑방지", "관세율", "관세쿼터", "원산지", "수출의무",
        ])
        if not strong_exception:
            return False

    if issue == "Reference" or impact == "Reference":
        return False

    pub = _v15_parse_date(row.get("Publish Date", row.get("Date", "")))
    if pd.isna(pub):
        # No publish date is allowed only for strong trade/customs execution items.
        if not any(k in blob for k in [
            "anti-dumping", "antidumping", "countervailing", "customs", "tariff",
            "rules of origin", "certificate of origin", "export obligation",
            "advance authorization", "epcg", "fta", "cepa", "tepa",
            "반덤핑", "상계관세", "관세", "통관", "원산지", "수출의무",
        ]):
            return False
    else:
        cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=int(os.getenv("GTI_STEP4_REG_MAX_AGE_DAYS", "120")))
        if pub.normalize() < cutoff:
            if not any(k in blob for k in ["anti-dumping", "antidumping", "countervailing", "반덤핑", "상계관세", "덤핑방지"]):
                return False

    bad_summary = [
        "본문 내용이 파싱되지 않아",
        "본문 확인 불가",
        "상세 내용은 확인 불가",
        "구체적인 내용은 확인하기 어렵습니다",
        "pdf 파일이 짧아",
        "확인 불가합니다",
    ]
    if any(k in blob for k in bad_summary):
        if not any(k in blob for k in ["anti-dumping", "antidumping", "countervailing", "반덤핑", "상계관세", "pn 51", "export obligation"]):
            return False

    return True


def _v15_split_reg_daily(daily):
    if daily is None or daily.empty:
        return daily, pd.DataFrame(columns=getattr(daily, "columns", OUTPUT_COLS))
    d = daily.copy()
    keep = d.apply(_v15_is_reportable_regulation, axis=1)
    kept = d[keep].copy().reset_index(drop=True)
    removed = d[~keep].copy().reset_index(drop=True)
    if not removed.empty:
        removed["RejectReason"] = removed.get("RejectReason", "").fillna("").astype(str).apply(
            lambda v: (v + "; " if v else "") + "v15_final_not_reportable_regulation"
        )
    if not kept.empty and "No" in kept.columns:
        kept["No"] = range(1, len(kept) + 1)
    return kept, removed



# ======================================================================
# GTI STEP4-1 Samsung Customs Regulation Priority Patch v16b
# ASCII source, Korean terms decoded at runtime to avoid Windows codepage loss.
# ======================================================================

def _v16u(s: str) -> str:
    return s.encode("ascii").decode("unicode_escape")

V16_CRITICAL_RULES = [
    (_v16u(r"\uad00\uc138\uc815\ucc45"), "Direct", 120, [_v16u(r"\uad00\uc138\ubc95 \uc81c71\uc870"), _v16u(r"\ud560\ub2f9\uad00\uc138"), _v16u(r"\uad00\uc138\uc728"), _v16u(r"\uad00\uc138\ucffc\ud130"), "tariff quota", "tariff rate", "customs duty", "import duty"]),
    (_v16u(r"FTA/\uc6d0\uc0b0\uc9c0"), "Direct", 115, [_v16u(r"\uc790\uc720\ubb34\uc5ed\ud611\uc815"), "FTA", "fta", "CEPA", "cepa", _v16u(r"\ud611\uc815\uc138\uc728"), _v16u(r"\uc6d0\uc0b0\uc9c0"), _v16u(r"\uc6d0\uc0b0\uc9c0\uc99d\uba85"), "rules of origin", "certificate of origin"]),
    (_v16u(r"HS/\ud488\ubaa9\ubd84\ub958"), "Direct", 110, [_v16u(r"\ud488\ubaa9\ubd84\ub958"), "HS", "hs code", "classification"]),
    (_v16u(r"\uc218\ucd9c\ud1b5\uc81c"), "Direct", 115, [_v16u(r"\uc218\ucd9c\ud1b5\uc81c"), _v16u(r"\uc804\ub7b5\ubb3c\uc790"), _v16u(r"\uc774\uc911\uc6a9\ub3c4"), "entity list", "export control", "dual-use", "UFLPA", "forced labor"]),
    (_v16u(r"\ubc18\ub364\ud551/\uc0c1\uacc4\uad00\uc138"), "Direct", 115, [_v16u(r"\ubc18\ub364\ud551"), _v16u(r"\ub364\ud551\ubc29\uc9c0"), _v16u(r"\uc0c1\uacc4\uad00\uc138"), _v16u(r"\uc138\uc774\ud504\uac00\ub4dc"), "anti-dumping", "antidumping", "countervailing", "safeguard", "AD/CVD"]),
    (_v16u(r"\ud1b5\uad00/\uc138\uad00"), "Indirect", 95, [_v16u(r"\uc218\ucd9c\uc785\ud654\ubb3c \uac80\uc0ac\ube44\uc6a9"), _v16u(r"\uac80\uc0ac\ube44\uc6a9 \uc9c0\uc6d0"), _v16u(r"\uc218\uc785\uc2e0\uace0"), _v16u(r"\uc218\ucd9c\uc2e0\uace0"), _v16u(r"\ud1b5\uad00"), _v16u(r"\uc138\uad00\uc7a5\ud655\uc778"), _v16u(r"\ud1b5\ud569\uacf5\uace0"), _v16u(r"\ubcf4\uc138"), "customs clearance", "customs declaration"]),
    ("CBAM", "Indirect", 100, ["CBAM", "carbon border", _v16u(r"\ud0c4\uc18c\uad6d\uacbd")]),
]

V16_ADMIN_NOISE_TERMS = [
    _v16u(r"\uad00\uc138\uccad\uacfc \uadf8 \uc18c\uc18d\uae30\uad00 \uc9c1\uc81c"), _v16u(r"\uc9c1\uc81c \uc2dc\ud589\uaddc\uce59"),
    _v16u(r"\ubd80\uc815\uccad\ud0c1"), _v16u(r"\uae08\ud488\ub4f1 \uc218\uc218"), _v16u(r"\uc9c8\uc11c\uc704\ubc18\ud589\uc704\uaddc\uc81c\ubc95"),
    _v16u(r"\uc2b9\uac1d\uc608\uc57d\uc790\ub8cc"), _v16u(r"\uc9c0\ubc29\ud589\uc815\uccb4\uc81c"), _v16u(r"\uc120\ubc15\uad50\ud1b5\uad00\uc81c"),
    _v16u(r"\ubcf4\uc138\ud310\ub9e4\uc7a5"), _v16u(r"\ub300\ud1b5\ub839\ub839 \uc81c"), _v16u(r"\uac1c\ubcc4\uc18c\ube44\uc138\ubc95 \uc2dc\ud589\ub839"),
]

V16_NON_TRADE_LAW_TERMS = [_v16u(r"\uc0dd\ud65c\ud654\ud559\uc81c\ud488"), _v16u(r"\ubc29\uc704\uc0ac\uc5c5\ubc95 \uc2dc\ud589\uaddc\uce59"), _v16u(r"\uc758\ub8cc\uae30\uae30\ubc95 \uc2dc\ud589\uaddc\uce59")]
V16_OFFICIAL_SOURCE_HINTS = [_v16u(r"\uad00\uc138\uccad"), _v16u(r"\uc720\ub2c8\ud328\uc2a4"), _v16u(r"\uad6d\uac00\ubc95\ub839\uc815\ubcf4\uc13c\ud130"), "law.go.kr", "unipass.customs.go.kr", "customs.go.kr", "gwanbo.go.kr", "clhs.co.kr", "federalregister.gov", "cbp.gov", "ustr.gov", "usitc.gov", "dgft.gov.in", "cbic.gov.in", "mofcom.gov.cn", "customs.gov.cn", "gacc.gov.cn", "moit.gov.vn"]


def _v16_text(value) -> str:
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value or "").strip()


def _v16_blob(row) -> str:
    cols = ["Headline", "Title", "title", "headline", "Summary", "AI Analysis", "Action Plan", "article_body", "regulation_fallback_body", "KeywordMatches", "Agency", "agency", "Source", "source", "URL", "url", "effective_date_hint", "hs_hint", "tariff_rate_hint"]
    return " ".join(_v16_text(row.get(c, "")) for c in cols).lower()


def _v16_title(row) -> str:
    for c in ["Headline", "headline", "Title", "title"]:
        v = _v16_text(row.get(c, ""))
        if v:
            return v
    return ""


def _v16_matches(blob: str, terms) -> list[str]:
    return [t for t in terms if str(t).lower() in blob]


def _v16_priority(row) -> dict:
    blob = _v16_blob(row)
    title_l = _v16_title(row).lower()
    best = {"issue": "Reference", "impact": "Reference", "score": 0, "terms": []}
    for issue, impact, base_score, terms in V16_CRITICAL_RULES:
        found = _v16_matches(blob, terms)
        if not found:
            continue
        title_bonus = 12 if _v16_matches(title_l, terms) else 0
        official_bonus = 8 if any(x.lower() in blob for x in V16_OFFICIAL_SOURCE_HINTS) else 0
        score = base_score + min(len(found), 4) * 4 + title_bonus + official_bonus
        if score > best["score"]:
            best = {"issue": issue, "impact": impact, "score": score, "terms": found}
    return best


def _v16_is_admin_noise(row) -> bool:
    blob = _v16_blob(row)
    title = _v16_title(row).lower()
    priority = _v16_priority(row)
    if priority["score"] >= 110:
        return False
    if any(t.lower() in title for t in V16_ADMIN_NOISE_TERMS):
        return True
    if any(t.lower() in title for t in V16_NON_TRADE_LAW_TERMS) and priority["score"] < 120:
        return True
    return False


def _v16_summary(row, issue: str) -> str:
    title_body = f"{_v16_title(row)} {_v16_text(row.get('Summary', ''))} {_v16_text(row.get('article_body', ''))}"
    if _v16u(r"\ud560\ub2f9\uad00\uc138") in title_body or _v16u(r"\uad00\uc138\ubc95 \uc81c71\uc870") in title_body:
        return _v16u(r"\uad00\uc138\ubc95 \uc81c71\uc870\uc5d0 \ub530\ub978 \ud560\ub2f9\uad00\uc138 \uc801\uc6a9/\ucd94\ucc9c \uc694\uac74 \uad00\ub828 \ubc95\uaddc\uc785\ub2c8\ub2e4. \ub300\uc0c1 \ud488\ubaa9, \uc801\uc6a9 \uc138\uc728, \ucd94\ucc9c\uc11c\u00b7\uc99d\ube59 \uc694\uac74, \uc2dc\ud589\uae30\uac04\uc744 \uc218\uc785 \ud488\ubaa9 \ub9c8\uc2a4\ud130\uc640 \ub300\uc870\ud574\uc57c \ud569\ub2c8\ub2e4.")
    if any(x in title_body for x in [_v16u(r"\uc790\uc720\ubb34\uc5ed\ud611\uc815"), "FTA", _v16u(r"\ud611\uc815\uc138\uc728"), _v16u(r"\uc6d0\uc0b0\uc9c0")]):
        return _v16u(r"FTA \ud611\uc815\uc138\uc728 \ub610\ub294 \uc6d0\uc0b0\uc9c0 \uc99d\uba85\u00b7\ud310\uc815 \uc808\ucc28\uc640 \uad00\ub828\ub41c \ubc95\uaddc/\uacf5\uc9c0\uc785\ub2c8\ub2e4. \ud611\uc815\ubcc4 \uc138\uc728, \uc6d0\uc0b0\uc9c0 \uc99d\ube59, BOM/HS \uae30\uc900 \ubc18\uc601 \uc5ec\ubd80\uac00 \ud575\uc2ec\uc785\ub2c8\ub2e4.")
    if any(x in title_body for x in [_v16u(r"\uc218\ucd9c\uc785\ud654\ubb3c \uac80\uc0ac\ube44\uc6a9"), _v16u(r"\uac80\uc0ac\ube44\uc6a9 \uc9c0\uc6d0")]):
        return _v16u(r"\uc218\ucd9c\uc785\ud654\ubb3c \uac80\uc0ac\ube44\uc6a9 \uc9c0\uc6d0 \uc0ac\ubb34\ucc98\ub9ac \uad00\ub828 \uace0\uc2dc\uc785\ub2c8\ub2e4. \uac80\uc0ac \ub300\uc0c1 \uc218\uc785\ud654\ubb3c, \ube44\uc6a9 \uc9c0\uc6d0 \uac00\ub2a5 \uc5ec\ubd80, \ud1b5\uad00 \ube44\uc6a9 \ud68c\uc218 \uc808\ucc28\ub97c \ud655\uc778\ud574\uc57c \ud569\ub2c8\ub2e4.")
    return (_v16_text(row.get("Summary", "")) or f"{_v16_title(row)} official regulation requiring customs review.")[:700]


def _v16_action(row, issue: str) -> str:
    title_body = f"{_v16_title(row)} {_v16_text(row.get('Summary', ''))} {_v16_text(row.get('article_body', ''))}"
    if _v16u(r"\ud560\ub2f9\uad00\uc138") in title_body or _v16u(r"\uad00\uc138\ubc95 \uc81c71\uc870") in title_body:
        return _v16u(r"\uc989\uc2dc\uc870\uce58: \ud558\ubc18\uae30 \ud560\ub2f9\uad00\uc138 HS \ub9ac\uc2a4\ud2b8\uc640 \uc0bc\uc131\uc804\uc790/1\ucc28 \ud611\ub825\uc0ac \uc218\uc785 \uc6d0\ubd80\uc790\uc7ac \ub9c8\uc2a4\ud130\ub97c \uad50\ucc28 \ub9e4\ud551\ud558\uc2ed\uc2dc\uc624. 1\uc8fc \ub0b4: \ucd94\ucc9c\uc11c\u00b7\uc99d\ube59\u00b7\uc218\uc785\uc2e0\uace0 \uc808\ucc28\ub97c \uad00\uc138\uc0ac/\uad6c\ub9e4\uc640 \ud655\uc815\ud558\uc2ed\uc2dc\uc624. Owner: HQ Customs + \uad6c\ub9e4 + \ubb3c\ub958")
    if any(x in title_body for x in [_v16u(r"\uc790\uc720\ubb34\uc5ed\ud611\uc815"), "FTA", _v16u(r"\ud611\uc815\uc138\uc728"), _v16u(r"\uc6d0\uc0b0\uc9c0")]):
        return _v16u(r"\uc989\uc2dc\uc870\uce58: \ub300\uc0c1 \ud611\uc815\uacfc \ud488\ubaa9\ubcc4 HS/\ud611\uc815\uc138\uc728 \ubcc0\uacbd \uc5ec\ubd80\ub97c \ud655\uc778\ud558\uc2ed\uc2dc\uc624. 1\uc8fc \ub0b4: BOM \uae30\uc900 \uc6d0\uc0b0\uc9c0 \ud310\uc815, \uc6d0\uc0b0\uc9c0\uc99d\uba85\uc11c \ubc1c\uae09/\ubcf4\uad00 \uc694\uac74, \ud611\ub825\uc0ac \uc6d0\uc0b0\uc9c0\ud655\uc778\uc11c \uc601\ud5a5\uc744 \uc810\uac80\ud558\uc2ed\uc2dc\uc624. Owner: FTA/Origin + Customs IT")
    if issue == _v16u(r"\uc218\ucd9c\ud1b5\uc81c"):
        return _v16u(r"\uc989\uc2dc\uc870\uce58: \ub300\uc0c1 \ud488\ubaa9\uc758 \uc804\ub7b5\ubb3c\uc790/ECCN \ud574\ub2f9 \uc5ec\ubd80\uc640 \ucd5c\uc885 \uc0ac\uc6a9\uc790 \uc2a4\ud06c\ub9ac\ub2dd\uc744 \ud655\uc778\ud558\uc2ed\uc2dc\uc624. Owner: Export Control + \ubc95\ubb34 + \uc0ac\uc5c5\ubd80")
    return _v16u(r"\uc989\uc2dc\uc870\uce58: \uc6d0\ubb38 \uae30\uc900 \uc2dc\ud589\uc77c, \uc801\uc6a9 \ud488\ubaa9, HS, \uc138\uc728, \uc2e0\uace0\uc808\ucc28 \ubcc0\uacbd \uc5ec\ubd80\ub97c \ud655\uc778\ud558\uc2ed\uc2dc\uc624. 1\uc8fc \ub0b4: \uad00\ub828 \ubc95\uc778/\ud488\ubaa9 \ub9c8\uc2a4\ud130\uc640 \uc601\ud5a5 \uc5ec\ubd80\ub97c \ub9e4\ud551\ud558\uc2ed\uc2dc\uc624. Owner: \uad00\uc138/\ud1b5\uc0c1 \ub9ac\uc2a4\ud06c \ubd84\uc11d\ud300")


def _v16_country(row) -> str:
    blob = _v16_blob(row)
    if any(x in blob for x in [_v16u(r"\uad00\uc138\uccad"), _v16u(r"\uc720\ub2c8\ud328\uc2a4"), "law.go.kr", _v16u(r"\ub300\ud55c\ubbfc\uad6d"), _v16u(r"\ud55c\uad6d")]):
        return _v16u(r"\ud55c\uad6d")
    if any(x in blob for x in ["vietnam", "moit.gov.vn", "customs.gov.vn"]): return _v16u(r"\ubca0\ud2b8\ub0a8")
    if any(x in blob for x in ["india", "dgft", "cbic"]): return _v16u(r"\uc778\ub3c4")
    if any(x in blob for x in ["china", "mofcom", "gacc", "customs.gov.cn"]): return _v16u(r"\uc911\uad6d")
    if any(x in blob for x in ["europa", "european", " eu "]): return "EU"
    if any(x in blob for x in ["federalregister", "cbp.gov", "ustr.gov", "usitc.gov", "united states", " u.s."]): return _v16u(r"\ubbf8\uad6d")
    return _v16u(r"\uad00\ub828\uad6d")


def _v16_risk(issue: str, impact: str, score: int) -> str:
    return _v16u(r"\uc0c1") if impact == "Direct" or score >= 120 else _v16u(r"\uc911")

_v16_to_output_base = to_output

def to_output(df, content_type="Regulation"):
    if content_type != "Regulation":
        return _v16_to_output_base(df, content_type)
    rows = []
    if df is None:
        df = pd.DataFrame()
    for _, r in df.reset_index(drop=True).iterrows():
        priority = _v16_priority(r)
        issue = priority["issue"] if priority["issue"] != "Reference" else _v12_reg_issue(r)
        if issue == "Reference":
            impact = "Reference"
            score = int(float(r.get("score", 0) or 0))
        else:
            impact = priority["impact"] if priority["impact"] != "Reference" else "Watch"
            score = max(int(float(r.get("score", 0) or 0)), int(priority["score"]))
        title = _v16_title(r)
        pub_date = _v12_publish_date(r)
        rows.append({
            "No": len(rows) + 1, "Content Type": content_type, "Mail Group": "Regulation", "Samsung Impact": impact,
            "Affected Subsidiary": _v16u(r"\ud55c\uad6d/\uc8fc\uc694 \uc0dd\uc0b0\ubc95\uc778 \ubc0f \uad00\ub828 \uc218\uc785\u00b7\uc218\ucd9c \ubc95\uc778"),
            "Impact Reason": "v16_samsung_customs_law_priority:" + ",".join(priority.get("terms", [])[:5]),
            "Date": pub_date, "Publish Date": pub_date, "Headline": title, "Summary": _v16_summary(r, issue),
            "AI Analysis": _v16u(r"\uc0bc\uc131\uc804\uc790 \uad00\uc138 \ub2f4\ub2f9 \uad00\uc810\uc5d0\uc11c \uc218\uc785\ud1b5\uad00, FTA\u00b7\uc6d0\uc0b0\uc9c0, HS/\ud488\ubaa9\ubd84\ub958, \uad00\uc138\ube44\uc6a9 \ub610\ub294 \uc218\ucd9c\ud1b5\uc81c \uc808\ucc28\uc5d0 \uc601\ud5a5\uc744 \uc904 \uc218 \uc788\ub294 \uacf5\uc2dd \ubc95\uaddc\uc785\ub2c8\ub2e4. \uc6d0\ubb38 \uae30\uc900 \uc801\uc6a9 \ud488\ubaa9\u00b7HS\u00b7\uc138\uc728\u00b7\uc2dc\ud589\uc77c\u00b7\uc99d\ube59\uc694\uac74\uc744 \ud488\ubaa9 \ub9c8\uc2a4\ud130\uc640 \ub300\uc870\ud574\uc57c \ud569\ub2c8\ub2e4."),
            "Action Plan": _v16_action(r, issue), "Country": _v16_country(r), "Agency": _v16_text(r.get("Agency", r.get("agency", ""))),
            "Risk": _v16_risk(issue, impact, score), "Importance Score": score,
            "Priority Group": "CORE" if score >= 110 else ("USABLE" if impact != "Reference" else "REFERENCE"),
            "Issue": issue, "Cluster": title, "URL": _v13_fix_unipass_url(r), "Source": _v16_text(r.get("Source", r.get("source", ""))),
            "Source File": "3-1.regulation_article_summary.xlsx", "RejectReason": _v16_text(r.get("RejectReason", "")),
            "KeywordMatches": _v16_text(r.get("KeywordMatches", ",".join(priority.get("terms", [])))),
            "effective_date_hint": _v16_text(r.get("effective_date_hint", _v16u(r"\ubcf8\ubb38\uc5d0\uc11c \ud655\uc778 \ud544\uc694"))) or _v16u(r"\ubcf8\ubb38\uc5d0\uc11c \ud655\uc778 \ud544\uc694"),
            "hs_hint": _v16_text(r.get("hs_hint", _v16u(r"\ubcf8\ubb38\uc5d0\uc11c \ud655\uc778 \ud544\uc694"))) or _v16u(r"\ubcf8\ubb38\uc5d0\uc11c \ud655\uc778 \ud544\uc694"),
            "tariff_rate_hint": _v16_text(r.get("tariff_rate_hint", _v16u(r"\ubcf8\ubb38\uc5d0\uc11c \ud655\uc778 \ud544\uc694"))) or _v16u(r"\ubcf8\ubb38\uc5d0\uc11c \ud655\uc778 \ud544\uc694"),
            "article_extract_status": _v16_text(r.get("article_extract_status", "")), "ExecutiveMessage": _v16_summary(r, issue)[:500],
        })
    return pd.DataFrame(rows, columns=OUTPUT_COLS)

_v16_reportable_base = _v15_is_reportable_regulation

def _v15_is_reportable_regulation(row):
    if _v16_is_admin_noise(row):
        return False
    priority = _v16_priority(row)
    if priority["score"] >= 95:
        return True
    return _v16_reportable_base(row)

_v16_split_base = _v15_split_reg_daily

def _v15_split_reg_daily(daily):
    kept, removed = _v16_split_base(daily)
    if kept is None or kept.empty:
        return kept, removed
    kept = kept.copy()
    kept["_v16_score"] = kept.apply(lambda r: _v16_priority(r).get("score", 0), axis=1)
    kept["_v16_issue_rank"] = kept["Issue"].map({_v16u(r"\uad00\uc138\uc815\ucc45"):10, _v16u(r"FTA/\uc6d0\uc0b0\uc9c0"):9, _v16u(r"\uc218\ucd9c\ud1b5\uc81c"):8, _v16u(r"\ubc18\ub364\ud551/\uc0c1\uacc4\uad00\uc138"):8, _v16u(r"HS/\ud488\ubaa9\ubd84\ub958"):7, _v16u(r"\ud1b5\uad00/\uc138\uad00"):6, "CBAM":5}).fillna(1)
    kept = kept.sort_values(["_v16_score", "_v16_issue_rank", "Importance Score"], ascending=[False, False, False])
    limit = int(os.getenv("GTI_STEP4_REG_REPORT_MAX", "8"))
    overflow = pd.DataFrame(columns=kept.columns)
    if limit > 0 and len(kept) > limit:
        overflow = kept.iloc[limit:].copy()
        kept = kept.iloc[:limit].copy()
        overflow["RejectReason"] = overflow.get("RejectReason", "").fillna("").astype(str).apply(lambda v: (v + "; " if v else "") + "v16_over_report_limit_lower_priority")
    kept = kept.drop(columns=["_v16_score", "_v16_issue_rank"], errors="ignore").reset_index(drop=True)
    if not overflow.empty:
        overflow = overflow.drop(columns=["_v16_score", "_v16_issue_rank"], errors="ignore").reset_index(drop=True)
        removed = pd.concat([removed, overflow], ignore_index=True, sort=False) if removed is not None else overflow
    if "No" in kept.columns:
        kept["No"] = range(1, len(kept) + 1)
    return kept, removed

# ======================================================================
# End of GTI STEP4-1 Samsung Customs Regulation Priority Patch v16b
# ======================================================================


# ======================================================================
# GTI STEP4-1 v16c: classify only from original/title/source fields.
# Previous AI/Action text can contain broad FTA fallback wording and must not
# influence regulation sensing.
# ======================================================================

def _v16_source_blob(row) -> str:
    cols = [
        "Headline", "Title", "title", "headline", "article_body", "regulation_fallback_body",
        "original_url", "URL", "url", "Agency", "agency", "Source", "source",
        "effective_date_hint", "hs_hint", "tariff_rate_hint", "official_regulation_reason",
        "protected_regulation_reason", "matched_policy_terms",
    ]
    return " ".join(_v16_text(row.get(c, "")) for c in cols).lower()


def _v16_priority(row) -> dict:
    blob = _v16_source_blob(row)
    title_l = _v16_title(row).lower()
    best = {"issue": "Reference", "impact": "Reference", "score": 0, "terms": []}
    for issue, impact, base_score, terms in V16_CRITICAL_RULES:
        found = _v16_matches(blob, terms)
        if not found:
            continue
        title_bonus = 18 if _v16_matches(title_l, terms) else 0
        official_bonus = 8 if any(x.lower() in blob for x in V16_OFFICIAL_SOURCE_HINTS) else 0
        score = base_score + min(len(found), 4) * 4 + title_bonus + official_bonus
        if score > best["score"]:
            best = {"issue": issue, "impact": impact, "score": score, "terms": found}
    return best


def _v16_is_admin_noise(row) -> bool:
    title = _v16_title(row).lower()
    priority = _v16_priority(row)
    if priority["score"] >= 115:
        return False
    if any(t.lower() in title for t in V16_ADMIN_NOISE_TERMS):
        return True
    if any(t.lower() in title for t in V16_NON_TRADE_LAW_TERMS) and priority["score"] < 130:
        return True
    return False


def _v15_is_reportable_regulation(row):
    if _v16_is_admin_noise(row):
        return False
    return _v16_priority(row)["score"] >= 95


def to_output(df, content_type="Regulation"):
    if content_type != "Regulation":
        return _v16_to_output_base(df, content_type)
    rows = []
    if df is None:
        df = pd.DataFrame()
    for _, r in df.reset_index(drop=True).iterrows():
        priority = _v16_priority(r)
        issue = priority["issue"]
        impact = priority["impact"]
        score = max(int(float(r.get("score", 0) or 0)), int(priority["score"])) if issue != "Reference" else int(float(r.get("score", 0) or 0))
        title = _v16_title(r)
        pub_date = _v12_publish_date(r)
        rows.append({
            "No": len(rows) + 1, "Content Type": content_type, "Mail Group": "Regulation", "Samsung Impact": impact,
            "Affected Subsidiary": _v16u(r"\ud55c\uad6d/\uc8fc\uc694 \uc0dd\uc0b0\ubc95\uc778 \ubc0f \uad00\ub828 \uc218\uc785\u00b7\uc218\ucd9c \ubc95\uc778"),
            "Impact Reason": "v16c_original_field_priority:" + ",".join(priority.get("terms", [])[:5]),
            "Date": pub_date, "Publish Date": pub_date, "Headline": title, "Summary": _v16_summary(r, issue),
            "AI Analysis": _v16u(r"\uc0bc\uc131\uc804\uc790 \uad00\uc138 \ub2f4\ub2f9 \uad00\uc810\uc5d0\uc11c \uc218\uc785\ud1b5\uad00, FTA\u00b7\uc6d0\uc0b0\uc9c0, HS/\ud488\ubaa9\ubd84\ub958, \uad00\uc138\ube44\uc6a9 \ub610\ub294 \uc218\ucd9c\ud1b5\uc81c \uc808\ucc28\uc5d0 \uc601\ud5a5\uc744 \uc904 \uc218 \uc788\ub294 \uacf5\uc2dd \ubc95\uaddc\uc785\ub2c8\ub2e4. \uc6d0\ubb38 \uae30\uc900 \uc801\uc6a9 \ud488\ubaa9\u00b7HS\u00b7\uc138\uc728\u00b7\uc2dc\ud589\uc77c\u00b7\uc99d\ube59\uc694\uac74\uc744 \ud488\ubaa9 \ub9c8\uc2a4\ud130\uc640 \ub300\uc870\ud574\uc57c \ud569\ub2c8\ub2e4."),
            "Action Plan": _v16_action(r, issue), "Country": _v16_country(r), "Agency": _v16_text(r.get("Agency", r.get("agency", ""))),
            "Risk": _v16_risk(issue, impact, score), "Importance Score": score,
            "Priority Group": "CORE" if score >= 110 else ("USABLE" if impact != "Reference" else "REFERENCE"),
            "Issue": issue, "Cluster": title, "URL": _v13_fix_unipass_url(r), "Source": _v16_text(r.get("Source", r.get("source", ""))),
            "Source File": "3-1.regulation_article_summary.xlsx", "RejectReason": _v16_text(r.get("RejectReason", "")),
            "KeywordMatches": ",".join(priority.get("terms", [])) or _v16_text(r.get("KeywordMatches", "")),
            "effective_date_hint": _v16_text(r.get("effective_date_hint", _v16u(r"\ubcf8\ubb38\uc5d0\uc11c \ud655\uc778 \ud544\uc694"))) or _v16u(r"\ubcf8\ubb38\uc5d0\uc11c \ud655\uc778 \ud544\uc694"),
            "hs_hint": _v16_text(r.get("hs_hint", _v16u(r"\ubcf8\ubb38\uc5d0\uc11c \ud655\uc778 \ud544\uc694"))) or _v16u(r"\ubcf8\ubb38\uc5d0\uc11c \ud655\uc778 \ud544\uc694"),
            "tariff_rate_hint": _v16_text(r.get("tariff_rate_hint", _v16u(r"\ubcf8\ubb38\uc5d0\uc11c \ud655\uc778 \ud544\uc694"))) or _v16u(r"\ubcf8\ubb38\uc5d0\uc11c \ud655\uc778 \ud544\uc694"),
            "article_extract_status": _v16_text(r.get("article_extract_status", "")), "ExecutiveMessage": _v16_summary(r, issue)[:500],
        })
    return pd.DataFrame(rows, columns=OUTPUT_COLS)

# ======================================================================
# End of GTI STEP4-1 v16c
# ======================================================================


# ======================================================================
# GTI STEP4-1 v16d: reduce false positives from clhs URL and generic import
# requirement laws. Generic "?????/????" is review material unless the
# title also contains Samsung customs core terms.
# ======================================================================

def _v16_match_blob(row) -> str:
    cols = ["Headline", "Title", "title", "headline", "article_body", "regulation_fallback_body", "effective_date_hint", "hs_hint", "tariff_rate_hint", "matched_policy_terms"]
    return " ".join(_v16_text(row.get(c, "")) for c in cols).lower()


def _v16_matches(blob: str, terms) -> list[str]:
    found = []
    for t in terms:
        tt = str(t)
        if tt == "HS":
            if re.search(r"(?<![A-Za-z])hs(?![A-Za-z])", blob, flags=re.I):
                found.append(t)
            continue
        if tt.lower() in blob:
            found.append(t)
    return found


def _v16_priority(row) -> dict:
    match_blob = _v16_match_blob(row)
    official_blob = _v16_source_blob(row)
    title_l = _v16_title(row).lower()
    best = {"issue": "Reference", "impact": "Reference", "score": 0, "terms": []}
    for issue, impact, base_score, terms in V16_CRITICAL_RULES:
        found = _v16_matches(match_blob, terms)
        if not found:
            continue
        # Generic import-requirement terms alone are not enough for executive report.
        if set(found) <= {_v16u(r"\uc138\uad00\uc7a5\ud655\uc778"), _v16u(r"\ud1b5\ud569\uacf5\uace0"), _v16u(r"\ubcf4\uc138")}:
            title_core = any(x in title_l for x in [_v16u(r"\ud560\ub2f9\uad00\uc138"), _v16u(r"\uc6d0\uc0b0\uc9c0"), "fta", _v16u(r"\uad00\uc138\uc728"), _v16u(r"\uc218\ucd9c\ud1b5\uc81c"), _v16u(r"\ud488\ubaa9\ubd84\ub958")])
            if not title_core:
                continue
        title_bonus = 18 if _v16_matches(title_l, terms) else 0
        official_bonus = 8 if any(x.lower() in official_blob for x in V16_OFFICIAL_SOURCE_HINTS) else 0
        score = base_score + min(len(found), 4) * 4 + title_bonus + official_bonus
        if score > best["score"]:
            best = {"issue": issue, "impact": impact, "score": score, "terms": found}
    return best


def _v16_is_admin_noise(row) -> bool:
    title = _v16_title(row).lower()
    critical_title_terms = [_v16u(r"\ud560\ub2f9\uad00\uc138"), _v16u(r"\uad00\uc138\ubc95 \uc81c71\uc870"), _v16u(r"\uc790\uc720\ubb34\uc5ed\ud611\uc815"), "fta", _v16u(r"\ud611\uc815\uc138\uc728"), _v16u(r"\uc6d0\uc0b0\uc9c0"), _v16u(r"\uc218\ucd9c\uc785\ud654\ubb3c \uac80\uc0ac\ube44\uc6a9"), _v16u(r"\uc218\ucd9c\ud1b5\uc81c"), _v16u(r"\ud488\ubaa9\ubd84\ub958")]
    if any(t.lower() in title for t in V16_ADMIN_NOISE_TERMS) and not any(t.lower() in title for t in critical_title_terms):
        return True
    if any(t.lower() in title for t in V16_NON_TRADE_LAW_TERMS):
        return True
    priority = _v16_priority(row)
    if priority["score"] >= 115:
        return False
    return False

# ======================================================================
# End of GTI STEP4-1 v16d
# ======================================================================


# ======================================================================
# GTI STEP4-1 v16e: deduplicate the same regulation from multiple official
# mirrors (UNIPASS/law.go.kr/clhs) before writing daily summary.
# ======================================================================

def _v16_reg_dedup_key(row) -> str:
    title = _v16_title(row)
    title = re.sub(r"\([^)]*\)", " ", title)
    title = re.sub(r"\[[^]]*\]", " ", title)
    title = re.sub(r"[^\w]+", "", title, flags=re.UNICODE).replace("_", "").lower()
    title = re.sub(r"20\d{2}\d*", "", title)
    title = title.replace(_v16u(r"\uc81c"), "").replace(_v16u(r"\ud638"), "")
    for suffix in [_v16u(r"\uad00\uc138\uccad\uace0\uc2dc"), _v16u(r"\uace0\uc2dc")]:
        title = title.replace(suffix, "")
    return title[:90] or _v16_text(row.get("URL", ""))[:160]

_v16e_split_base = _v15_split_reg_daily

def _v15_split_reg_daily(daily):
    kept, removed = _v16e_split_base(daily)
    if kept is None or kept.empty:
        return kept, removed
    kept = kept.copy()
    kept["_v16_dedup_key"] = kept.apply(_v16_reg_dedup_key, axis=1)
    kept["_v16_source_rank"] = kept["URL"].astype(str).str.contains("law.go.kr", case=False, na=False).astype(int) * 2 + kept["URL"].astype(str).str.contains("unipass.customs.go.kr", case=False, na=False).astype(int)
    kept = kept.sort_values(["Importance Score", "_v16_source_rank"], ascending=[False, False])
    dup_mask = kept.duplicated("_v16_dedup_key", keep="first")
    dups = kept.loc[dup_mask].copy()
    kept = kept.loc[~dup_mask].copy()
    if not dups.empty:
        dups["RejectReason"] = dups.get("RejectReason", "").fillna("").astype(str).apply(lambda v: (v + "; " if v else "") + "v16_duplicate_same_regulation")
        removed = pd.concat([removed, dups.drop(columns=["_v16_dedup_key", "_v16_source_rank"], errors="ignore")], ignore_index=True, sort=False) if removed is not None else dups
    kept = kept.drop(columns=["_v16_dedup_key", "_v16_source_rank"], errors="ignore").reset_index(drop=True)
    if "No" in kept.columns:
        kept["No"] = range(1, len(kept) + 1)
    return kept, removed

# ======================================================================
# End of GTI STEP4-1 v16e
# ======================================================================

def main():
    print("[STEP4-1] Regulation analysis start - GUARDRAIL v4.1")
    _gti_step4_gemini_log_once()
    _gti_step4_v10_log_once()
    log(f"Gemini model candidates: {GEMINI_MODEL_CANDIDATES}")
    _gti_step4_extractor_log_once()
    global KEYWORD_TERMS
    KEYWORD_TERMS = load_keyword_terms()
    log(f"keyword guardrail loaded: {len(KEYWORD_TERMS)} terms")
    df=read_input()
    selected, excluded_raw, audit_raw=build(df)
    raw_daily=to_output(selected)
    daily, removed_daily = _v15_split_reg_daily(raw_daily)
    excluded=to_output(excluded_raw)
    if removed_daily is not None and not removed_daily.empty:
        excluded = pd.concat([excluded, removed_daily], ignore_index=True, sort=False)
    cumulative=merge_cumulative(daily)
    write_excel(daily, OUT_SUMMARY); write_excel(cumulative, OUT_CUMULATIVE); write_excel(excluded, OUT_EXCLUDED)
    print(f"[DONE] Daily: {OUT_SUMMARY}")
    print(f"[DONE] Cumulative: {OUT_CUMULATIVE}")
    print(f"[DONE] Excluded: {OUT_EXCLUDED}")
    print(f"[ROWS] daily={len(daily)}, cumulative={len(cumulative)}, excluded={len(excluded)}")
if __name__ == "__main__": main()
