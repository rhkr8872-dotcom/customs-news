# -*- coding: utf-8 -*-
"""
GTI STEP4-1 REGULATION AI ANALYSIS - GUARDRAIL v4.1

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
from gti_report_window import previous_kst_day_mask

BASE_DIR = Path(os.getenv("GTI_BASE_DIR", r"C:\Temp"))
INPUT_FILE = BASE_DIR / "3-1.regulation_article_summary.xlsx"
FALLBACK_INPUT_FILE = BASE_DIR / "3-1.regulation_summary.xlsx"
KEYWORD_FILE = BASE_DIR / "keyword.xlsx"
OUT_SUMMARY = BASE_DIR / "4-1.regulation_ai_summary.xlsx"
MAX_AGE_HOURS = int(os.getenv("GTI_STEP4_REG_MAX_AGE_HOURS", "24"))
OUT_CUMULATIVE = BASE_DIR / "4-1.regulation_ai_cumulative.xlsx"
OUT_CUMULATIVE_REMOVED = BASE_DIR / "4-1.regulation_ai_cumulative_removed.xlsx"
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
    "마약", "밀수", "특별검사팀", "현장 점검",
    "thúc đẩy hợp tác hải quan", "đối thoại hải quan", "phiên đối thoại",
    "sơ bộ tình hình xuất nhập khẩu", "bắt giữ", "giả nhãn hiệu", "lịch bảo trì hệ thống",
]
TOPIC_RULES = [
    ("AD_CVD", ["anti-dumping", "anti dumping", "antidumping", "countervailing", "countervailing duty", "countervailing duties", "ad/cvd", "cvd", "dumping duties", "반덤핑", "덤핑방지관세", "덤핑사실", "국내산업피해", "산업피해구제", "불공정무역행위", "조사개시결정", "상계관세", "무역구제"]),
    ("EXPORT_CONTROL", ["export control", "export controls", "entity list", "denied persons", "bureau of industry and security", "수출통제", "전략물자", "제재", "산업안보국", "산업보안국"]),
    ("CBAM_CARBON", ["cbam", "carbon border", "carbon border adjustment", "탄소국경"]),
    ("ORIGIN_FTA", ["fta", "cepa", "usmca", "rules of origin", "origin", "원산지", "자유무역협정", "tepa"]),
    ("HS_CLASSIFICATION", ["hs code", "classification", "tariff classification", "품목분류", "hs코드"]),
    ("CUSTOMS_EDI", ["납세신고 정정", "수입신고 정정", "신고 정정", "전자문서 변경", "전자신고 변경", "5fe", "5fk"]),
    ("FX_TRADE_PAYMENT", ["외국환거래규정", "외국환 신고", "수출입대금", "지급 및 영수", "상계 신고"]),
    ("TARIFF", ["section 301", "301조", "section 232", "232조", "reciprocal tariff", "tariff", "tariffs", "customs duty", "import duty", "관세", "관세율", "추가관세", "상호관세"]),
    ("CUSTOMS", ["customs", "clearance", "declaration", "통관", "세관", "관세청"]),
]
TOPIC_KR = {"EXPORT_CONTROL":"수출통제", "AD_CVD":"반덤핑/상계관세", "CBAM_CARBON":"CBAM", "ORIGIN_FTA":"FTA/원산지", "HS_CLASSIFICATION":"HS/품목분류", "TARIFF":"관세정책", "CUSTOMS":"통관/세관", "CUSTOMS_EDI":"수입신고/전자문서", "FX_TRADE_PAYMENT":"수출입 외국환", "TRADE_GENERAL":"무역일반"}

STRICT_TRADE_REG_TERMS = [
    "관세", "관세율", "관세청", "통관", "세관", "보세", "수입신고", "수출신고",
    "품목분류", "hs code", "hs코드", "원산지", "fta", "자유무역협정", "cepa",
    "anti-dumping", "antidumping", "countervailing", "ad/cvd", "반덤핑", "덤핑방지관세",
    "덤핑사실", "국내산업피해", "산업피해구제", "불공정무역행위", "조사개시결정",
    "상계관세", "무역구제", "수출통제", "전략물자", "entity list", "cbam", "carbon border",
    "customs", "tariff", "tariffs", "customs duty", "import duty", "section 301", "section 232",
    "외국환거래규정", "외국환 신고", "수출입대금", "지급 및 영수",
    "납세신고 정정", "수입신고 정정", "전자문서 변경", "전자신고 변경", "5fe", "5fk",
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
    "외국환거래규정", "수출입대금", "납세신고 정정", "수입신고 정정", "전자문서 변경", "5fe", "5fk",
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
    "행정규칙", "입법예고", "행정예고", "덤핑방지관세", "덤핑사실", "국내산업피해", "조사개시결정", "상계관세", "무역구제",
    "관세율", "관세법", "보세", "통관", "수출입고시", "수입규제", "수출규제",
    "외국환거래규정", "납세신고 정정", "수입신고 정정", "전자문서 변경", "5fe", "5fk",
]

LEGAL_FORM_TITLE_TERMS = [
    "regulation", "rule", "rules", "law", "decree", "ordinance", "notice", "public notice",
    "trade notice", "federal register", "determination under", "investigation",
    "법", "법률", "법령", "시행령", "시행규칙", "규칙", "고시", "공고", "훈령", "예규",
    "행정규칙", "입법예고", "행정예고", "덤핑방지관세", "상계관세", "무역구제", "지급요령",
    "외국환거래규정", "전자문서 변경", "납세신고 정정", "수입신고 정정",
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

REGULATION_HARD_NOISE_TERMS = [
    "직제 시행규칙", "직제 일부개정", "그 소속기관 직제", "소속기관 직제",
    "직제 (행정관련", "직제(행정관련", "조직개편", "기구 개편", "정원 일부개정",
    "외국인청고시", "국적상실", "철도교통관제센터", "한국수출입은행법", "선박안전법",
]

def is_official_trade_policy_candidate(row: pd.Series) -> bool:
    if clean(row.get("OfficialPolicyFlag", "")).upper() == "Y":
        return True
    headline = clean(row.get("Headline", row.get("title", ""))).lower()
    owner = " ".join([
        clean(row.get("Agency", row.get("agency", ""))),
        clean(row.get("Source", row.get("source", ""))),
    ]).lower()
    agency_ok = any(x in owner for x in ["dgft", "cbic", "customs", "ustr", "usitc", "mofcom", "gacc", "taxud"])
    action_ok = any(x in headline for x in [
        "amendment in the export policy", "amendment in export policy",
        "amendment in the import policy", "amendment in import policy",
        "export policy of", "import policy of", "trade notice",
    ])
    return agency_ok and action_ok
CUMULATIVE_REMOVED_DF = pd.DataFrame()
for _quality_col in [
    "Top3 Eligible", "Body Verified", "Change Type", "Evidence", "Missing Facts",
    "RegulationMappingType", "MappingStatus", "RequiredMappingKeys", "EntityDirectFlag",
    "SamsungRelevanceScore", "CustomsTradePolicyScore", "DirectImpactScore", "WeightedScore",
    "DecisionStatus", "PriorityEligible", "Policy Stage", "BillNo", "DocumentIdentity", "EventKey",
    "OriginalDateStatus", "DateSource", "CustomsOperationalWatchFlag",
    "EvidenceStatus", "WhatChanged", "PolicyStageDisplay", "EffectiveDate",
    "AffectedProduct", "AffectedHS", "SamsungImpactPath", "RecommendedAction",
    "VerificationNeeded", "OfficialURL",
]:
    if _quality_col not in OUTPUT_COLS:
        OUTPUT_COLS.append(_quality_col)



# ======================================================================
# GTI STEP4 Gemini Original-URL Analysis Patch v5.0
# ======================================================================

GEMINI_API_KEY = (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()
GEMINI_MODEL = os.getenv("GTI_GEMINI_MODEL", "gemini-2.5-flash-lite").strip()
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
당신은 삼성전자 본사 관세·통상 및 관세컴플라이언스 책임자입니다.
아래 공식 법규/공고의 원문만 근거로 GTI Radar 의사결정 보고서를 작성하십시오.

분석 순서(반드시 준수):
1) 사실관계: 문서명, 발표기관·국가, 신규/개정/시행/조사 단계, 발표일·시행일, 대상 품목·HS, 세율·쿼터, 신고·증빙 요건을 원문 근거로 정리.
2) 삼성전자 관세업무 직접영향: 수입·수출통관, HS, 과세가격, 원산지/FTA, AD/CVD, 관세비용, 수출통제 중 실제로 바뀌는 업무와 영향 법인·제품·거래 흐름을 설명.
3) 대응: 즉시(오늘~3영업일), 1개월 내, 상시 모니터링 및 Owner를 분리.

Direct 판정 조건:
- 삼성전자 법인이 소재한 국가의 일반 통관·신고·납부·증빙·세관조사·심판청구·이의신청·행정절차 변경은 해당 국가 법인에 Direct.
- 특정품목의 관세율·HS·AD/CVD·원산지·수출통제 조치는 삼성 제품/원재료 + HS + 영향 법인 + 수출입 경로가 1:1로 확인될 때만 Direct.
- 국가명, 산업명, 삼성/반도체 단어만으로 Direct 금지. 불명확하면 Indirect 또는 Watch.

절대 금지:
- 제목 반복, 일반론, 모든 법인에 동일한 문구 사용 금지
- 원문에 없는 세율·HS·국가·시행일·대상제품을 추정하거나 지어내지 말 것
- 본문을 읽을 수 없으면 body_verified=false, Samsung Impact=Watch, Top3 Eligible=false

출력은 JSON만:
{{
  "Summary": "[사실관계] 원문 근거 3~5문장. 신규/개정 구분과 적용일 포함",
  "AI Analysis": "[삼성전자 관세업무 직접영향] 영향 경로·법인·제품·업무·비용/리스크를 근거와 함께 4~7문장",
  "Action Plan": "[즉시] ... | [1개월 내] ... | [상시] ... | [Owner] ...",
  "ExecutiveMessage": "무엇이 바뀌며 삼성전자 관세업무가 무엇을 결정해야 하는지 2문장",
  "Samsung Impact": "Direct|Indirect|Watch",
  "Top3 Eligible": false,
  "body_verified": true,
  "change_type": "신규|개정|시행|조사개시|판정|기타",
  "evidence": ["원문 근거1", "원문 근거2"],
  "missing_facts": ["원문에서 확인되지 않은 필수정보"]
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

def as_bool(value):
    if isinstance(value, bool):
        return value
    return clean(value).lower() in {"1", "true", "yes", "y"}
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
    if topic == "CUSTOMS_EDI": return "통관운영/관세시스템", "수입(납세)신고 정정 전자문서와 관세사·EDI 인터페이스 변경일을 확인하고 신고 테스트 및 SOP를 갱신하십시오."
    if topic == "FX_TRADE_PAYMENT": return "HQ관세/재무/외환", "수출입대금 지급·영수·상계·제3자 지급 신고요건과 증빙 및 ERP 지급 프로세스 영향을 확인하십시오."
    if topic == "TARIFF": return "통관운영/FTA팀", "관세율·시행일·대상국·대상품목을 확인하고 원가 영향을 점검하십시오."
    return "통관운영", "업무 관련성 확인 후 모니터링하십시오."

def guard_unverified_dates(analysis: dict, row: pd.Series) -> dict:
    """Remove dates asserted by AI when they are absent from extracted evidence."""
    result = dict(analysis or {})
    evidence_text = " ".join([
        clean(row.get("article_body", "")), clean(row.get("regulation_fallback_body", "")),
        clean(result.get("Evidence", "")),
    ])
    verified = clean(result.get("Body Verified", "N")).upper() == "Y"
    evidence_dates = set(re.findall(r"(?:20\d{2})[-년./ ]+\s*(?:0?[1-9]|1[0-2])[-월./ ]+\s*(?:0?[1-9]|[12]\d|3[01])", evidence_text))
    if verified and evidence_dates:
        return result
    for field in ["Summary", "AI Analysis", "Action Plan"]:
        value = clean(result.get(field, ""))
        if not value:
            continue
        value = re.sub(
            r"20\d{2}년\s*\d{1,2}월\s*\d{1,2}일(?:부터|자로)?\s*(?:시행|발효|적용|공포)(?:될\s*)?(?:예정입니다|예정이다|됩니다|된다|되었습니다|되었다)?[.]?",
            "구체적인 시행·발효일은 원문 확인이 필요합니다.", value,
        )
        value = value.replace("필요합니다.예정이며", "필요합니다.")
        value = value.replace("필요합니다. 예정이며", "필요합니다.")
        value = value.replace("필요합니다.,", "필요합니다.")
        value = value.replace("필요합니다..", "필요합니다.")
        result[field] = value
    return result


def enforce_unverified_evidence_contract(
    analysis: dict, row: pd.Series, *, issue: str, headline: str
) -> dict:
    """Replace all unsupported narrative when the regulation body is unverified.

    Partial regex cleanup is unsafe because a model can express dates, periods,
    rates or product scope in many forms.  For Body Verified=N, retain only
    title/metadata facts and explicitly list the facts that still require the
    official body or amendment comparison table.
    """
    result = dict(analysis or {})
    if clean(result.get("Body Verified", "N")).upper() == "Y":
        return result

    agency = clean(row.get("Agency", row.get("agency", ""))) or "공식기관"
    issue_name = clean(issue) or "관세·통상 법규"
    is_strategic = issue_name == "수출통제" or any(
        term in clean(headline).lower()
        for term in ["전략물자", "수출통제", "export control", "entity list"]
    )
    result["Summary"] = (
        f"{agency}의 '{headline}'가 게시되었습니다. 제목상 {issue_name} 관련 공식 법규·고시 후보입니다. "
        "원문 본문이 확보되지 않아 구체적인 개정내용, 대상 품목, HS/ECCN, 시행·발효일, "
        "적용기간 및 신고·허가 요건은 확인되지 않았습니다. 해당 정보는 공식 원문과 개정 전후표 확인 후 확정해야 합니다."
    )
    if is_strategic:
        result["AI Analysis"] = (
            "전략물자·수출통제 변경은 삼성전자 제품, 부품, 제조장비 및 해외 거래의 허가·스크리닝 업무에 "
            "중대한 영향을 줄 수 있으므로 긴급 원문확인 대상입니다. 다만 현재는 대상 품목, ECCN/전략물자 번호, "
            "영향 법인과 거래경로가 확인되지 않아 Direct 영향으로 확정할 수 없습니다. 원문 확인 전에는 "
            "기존 통제기준을 유지하고 신규 지정·삭제 및 허가요건 변경 여부를 우선 점검해야 합니다."
        )
        result["Action Plan"] = (
            "즉시: 공식 원문과 개정 전후표를 확보하여 통제대상 품목, 전략물자 번호/ECCN, 허가·신고요건 및 시행일을 확인합니다. "
            "| 1주 내: 확인된 변경사항을 삼성전자 Item Master, 수출통제 판정, 거래상대방·최종사용자 스크리닝과 대조합니다. "
            "| 상시: 영향 법인과 사업부에 확정된 변경사항만 배포합니다. | Owner: HQ 수출통제·관세팀"
        )
    else:
        result["AI Analysis"] = (
            f"{issue_name} 관련 공식 조치 후보로서 삼성전자 관세업무 영향 가능성은 있으나, 원문과 적용범위가 "
            "확인되지 않아 제품·HS·법인·거래경로의 직접 영향을 확정할 수 없습니다. 원문 검증과 실제 거래 매핑이 "
            "완료될 때까지 Watch로 관리해야 합니다."
        )
        result["Action Plan"] = (
            "즉시: 공식 원문과 개정 전후표를 확보하여 변경내용과 시행일을 확인합니다. "
            "| 1개월 내: 확인된 품목·HS·요건을 삼성전자 거래 및 Master Data와 대조합니다. "
            "| 상시: 원문 미확인 정보는 업무지침으로 배포하지 않습니다. | Owner: 관세·통상팀"
        )
    result["Evidence"] = ""
    result["Missing Facts"] = (
        "공식 원문 본문; 개정 전후표; 대상 품목/HS/ECCN; 시행·발효일; 적용기간; "
        "신고·허가·증빙요건; 삼성전자 영향 법인 및 거래경로"
    )
    result["Samsung Impact"] = "Watch"
    result["Top3 Eligible"] = "N"
    result["Change Type"] = "기타"
    return result

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
    official_trade_policy_force = is_official_trade_policy_candidate(row)
    forced_customs_trade_regulation = unipass_notice_force or indirect_tax_law_force or official_trade_policy_force
    strict_trade_signal = has_strict_trade_reg_signal(text, row) or metadata_trade_signal or forced_customs_trade_regulation
    old_ad_cvd_review = is_old_ad_cvd_review(topic, text, age_days)
    pure_regulation = is_pure_regulation_candidate(row, text, topic) or official_trade_policy_force

    if not is_valid_url(url): rejects.append("no_valid_url")
    if contains_any(headline.lower(), REGULATION_HARD_NOISE_TERMS):
        rejects.append("organization_or_non_customs_hard_noise")
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

    base_map = {"EXPORT_CONTROL":100,"AD_CVD":96,"CBAM_CARBON":90,"ORIGIN_FTA":88,"HS_CLASSIFICATION":86,"CUSTOMS_EDI":90,"FX_TRADE_PAYMENT":86,"TARIFF":84,"CUSTOMS":74,"TRADE_GENERAL":72 if keyword_hits else 30}
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
        if official_trade_policy_force:
            keyword_hits.append("OFFICIAL_TRADE_POLICY")
    if keyword_hits and not rejects:
        score = max(score, 72)
    if rejects:
        if "review_preserve_ad_cvd_old_date" in rejects:
            score = min(score, 55)
        else:
            score = min(score, 45 if "event_training_tender_noise" in rejects else 50)
    owner, action = action_for(topic)
    risk = "상" if score >= 85 else "중" if score >= 70 else "하"
    issue = TOPIC_KR.get(topic, topic)
    selected = not rejects and score >= MIN_SCORE
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
    analysis = guard_unverified_dates(analysis, row)
    analysis = enforce_unverified_evidence_contract(
        analysis, row, issue=issue, headline=headline
    )
    mapping_type = clean(row.get("RegulationMappingType", "POLICY_GENERAL")) or "POLICY_GENERAL"
    if mapping_type == "ITEM_1TO1":
        mapping_type = "PRODUCT_1TO1"
    elif mapping_type == "ENTITY_COUNTRY":
        mapping_type = "POLICY_GENERAL"
    selected = not rejects and (score >= MIN_SCORE or mapping_type == "PRODUCT_1TO1")
    body_ok = clean(analysis.get("Body Verified", "N")).upper() == "Y"
    hs_value = clean(row.get("hs_hint", ""))
    product_value = clean(row.get("affected_products", row.get("Product", "")))
    entity_value = clean(row.get("Affected Subsidiary", row.get("SamsungEntity", "")))
    route_value = clean(row.get("ImportExportRoute", row.get("TradeRoute", "")))
    item_mapped = all(v and "확인 불가" not in v for v in [hs_value, product_value, entity_value, route_value])

    # HS/AD-CVD/product measures always require product-level mapping even when
    # STEP3 supplied a legacy POLICY_GENERAL value.
    if topic in {"AD_CVD", "HS_CLASSIFICATION", "EXPORT_CONTROL", "CBAM_CARBON"}:
        mapping_type = "PRODUCT_1TO1"
    if hs_value and "확인 불가" not in hs_value:
        mapping_type = "PRODUCT_1TO1"

    ai_impact = clean(analysis.get("Samsung Impact", "Watch")) or "Watch"
    mapping_status = clean(row.get("MappingStatus", "POLICY_REVIEW")) or "POLICY_REVIEW"
    if mapping_type == "ENTITY_DIRECT" and body_ok:
        ai_impact = "Direct"
        mapping_status = "ENTITY_CONFIRMED"
    elif mapping_type == "PRODUCT_1TO1":
        if body_ok and item_mapped:
            ai_impact = "Direct"
            mapping_status = "MAPPED"
        else:
            ai_impact = "Watch"
            mapping_status = "MAPPING_REQUIRED" if body_ok else "VERIFICATION_PENDING"
    else:
        # Hypothetical language such as "if Samsung trades this product" is
        # not an indirect-impact mapping.  Keep general policy at Watch until
        # an actual Samsung entity/product/HS/trade route is evidenced.
        ai_impact = "Watch"
        mapping_status = "POLICY_REVIEW"

    policy_score = max(0, min(100, int(score)))
    direct_score = 100 if ai_impact == "Direct" else 65 if ai_impact == "Indirect" else 35
    samsung_relevance = 100 if mapping_status in {"ENTITY_CONFIRMED", "MAPPED"} else 50
    weighted_score = round(policy_score * 0.4 + direct_score * 0.4 + samsung_relevance * 0.2)
    # A verified, high-materiality customs regulation must reach the executive
    # verification queue even when Samsung entity/product mapping is pending.
    # This does not relax the Direct gate: Direct still requires evidence-backed
    # entity/product/route mapping above.
    high_materiality = body_ok and policy_score >= 85
    top3 = "Y" if (body_ok and ai_impact == "Direct" and weighted_score >= 80) or high_materiality else "N"
    if not body_ok:
        decision_status = "Verification Pending"
    elif ai_impact == "Direct":
        decision_status = "Action Required"
    elif high_materiality:
        decision_status = "Urgent Verification"
    else:
        decision_status = "Monitoring"

    headline_low = headline.lower()
    proposed = any(x in headline_low for x in [
        "법률안", "입법예고", "행정예고", "proposal", "proposed", "draft",
    ])
    policy_stage = "PROPOSED_OR_MONITORING" if proposed else "ENACTED_OR_EFFECTIVE"
    bill_no = clean(row.get("BillNo", ""))
    document_identity = clean(row.get("DocumentIdentity", ""))
    source_event_key = clean(row.get("EventKey", ""))
    if document_identity:
        source_event_key = document_identity.lower()
    elif bill_no:
        source_event_key = f"bill:{bill_no.split('.')[0]}"

    effective_hint = clean(row.get("effective_date_hint", ""))
    if not any(x in " ".join([clean(row.get("article_body", "")), clean(row.get("regulation_fallback_body", "")), clean(analysis.get("Evidence", ""))]).lower() for x in ["시행", "적용", "발효", "effective", "takes effect", "enters into force"]):
        effective_hint = "본문에서 확인 불가"
    return {"selected": selected, "RejectReason": "; ".join(rejects), "Issue": issue, "topic": topic, "score": score, "Risk": risk, "URL": url, "Headline": headline, "Date": clean(date_val), "Country": clean(row.get("Country", row.get("country", ""))), "Agency": clean(row.get("Agency", row.get("agency", ""))), "Source": clean(row.get("Source", row.get("source", ""))), "Summary": analysis.get("Summary", ""), "AI Analysis": analysis.get("AI Analysis", ""), "Action Plan": analysis.get("Action Plan", action), "Owner": owner, "KeywordMatches": "; ".join(keyword_hits[:12]), "tariff_rate_hint": extract_tariff_rate(text), "effective_date_hint": effective_hint or "본문에서 확인 불가", "hs_hint": hs_value or "본문에서 확인 불가", "article_extract_status": analysis.get("article_extract_status", ""), "Samsung Impact": ai_impact, "Top3 Eligible": top3, "PriorityEligible": top3, "DecisionStatus": decision_status, "Policy Stage": policy_stage, "BillNo": bill_no, "DocumentIdentity": document_identity, "EventKey": source_event_key, "Body Verified": analysis.get("Body Verified", "N"), "Change Type": analysis.get("Change Type", "기타"), "Evidence": analysis.get("Evidence", ""), "Missing Facts": analysis.get("Missing Facts", ""), "RegulationMappingType": mapping_type, "MappingStatus": mapping_status, "RequiredMappingKeys": clean(row.get("RequiredMappingKeys", "")), "EntityDirectFlag": "Y" if mapping_type == "ENTITY_DIRECT" else "N", "SamsungRelevanceScore": samsung_relevance, "CustomsTradePolicyScore": policy_score, "DirectImpactScore": direct_score, "WeightedScore": weighted_score}

def read_input():
    input_path = INPUT_FILE if INPUT_FILE.exists() else FALLBACK_INPUT_FILE
    if not input_path.exists():
        raise FileNotFoundError(
            f"regulation input not found: {INPUT_FILE} / {FALLBACK_INPUT_FILE}"
        )
    df = normalize_columns(pd.read_excel(input_path))
    log(f"LOAD {input_path}: {len(df)} rows")
    # normalize common caps for scoring
    if "Headline" not in df.columns and "title" in df.columns: df["Headline"] = df["title"]
    if "URL" not in df.columns and "url" in df.columns: df["URL"] = df["url"]
    if "Date" not in df.columns and "date" in df.columns: df["Date"] = df["date"]
    if "Agency" not in df.columns and "agency" in df.columns: df["Agency"] = df["agency"]
    if "Source" not in df.columns and "source" in df.columns: df["Source"] = df["source"]
    return df

def strict_24h(df):
    if df.empty:
        return df.copy(), df.copy()
    out = df.copy()
    keep, published, start, end = previous_kst_day_mask(out.get("Date"))
    fresh = out.loc[keep].copy()
    stale = out.loc[~keep].copy()
    log(
        f"KST PRIOR-DAY REGULATION GUARD: {len(out)} -> {len(fresh)} / "
        f"removed={len(stale)} / window={start} <= Date < {end}"
    )
    return fresh, stale

def build(df):
    rows=[]
    for _, row in df.iterrows():
        s=score_row(row)
        rows.append(s)
    audit=pd.DataFrame(rows)
    if audit.empty:
        audit = pd.DataFrame(columns=["selected", "score", "Date"])
        return audit.copy(), audit.copy(), audit
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
        impact = clean(r.get("Samsung Impact", "Watch")) or "Watch"
        headline = clean(r.get("Headline"))
        agency = clean(r.get("Agency"))
        country = clean(r.get("Country", ""))
        if "관세청" in headline:
            agency = "대한민국 관세청"
            country = "대한민국"
        strategic_priority = clean(r.get("topic")) == "EXPORT_CONTROL" or any(
            term in headline.lower() for term in ["전략물자", "수출통제", "export control", "entity list"]
        )
        rows.append({
            "No": i+1, "Content Type": content_type, "Mail Group": "Regulation" if content_type=="Regulation" else "News - 주요/참고",
            "Samsung Impact": impact,
            "Affected Subsidiary": "HQ 수출통제·관세팀 / 관련 사업부" if strategic_priority else "관련 법인 검토",
            "Impact Reason": "critical_export_control_regulation_pending_verification" if strategic_priority else "official_trade_regulation_watch",
            "Date": r["Date"], "Headline": headline, "Summary": r["Summary"], "AI Analysis": r["AI Analysis"], "Action Plan": r["Action Plan"],
            "Country": country, "Agency": agency,
            "Risk": "상" if strategic_priority else r["Risk"],
            "Importance Score": 100 if strategic_priority else int(r["score"]),
            "Priority Group": "CORE" if strategic_priority or int(r["score"])>=85 else "USABLE",
            "Issue": r["Issue"], "Cluster": r["Headline"], "URL": r["URL"], "Source": r["Source"], "Source File": "3-1.regulation_article_summary.xlsx",
            "Top3 Eligible": r.get("Top3 Eligible", "N"), "Body Verified": r.get("Body Verified", "N"), "Change Type": r.get("Change Type", "기타"), "Evidence": r.get("Evidence", ""), "Missing Facts": r.get("Missing Facts", ""),
            "PriorityEligible": r.get("PriorityEligible", r.get("Top3 Eligible", "N")), "DecisionStatus": r.get("DecisionStatus", "Monitoring"), "Policy Stage": r.get("Policy Stage", ""),
            "BillNo": r.get("BillNo", ""), "DocumentIdentity": r.get("DocumentIdentity", ""), "EventKey": r.get("EventKey", ""),
            "OriginalDateStatus": r.get("OriginalDateStatus", ""), "DateSource": r.get("date_source", r.get("DateSource", "")),
            "CustomsOperationalWatchFlag": r.get("CustomsOperationalWatchFlag", "N"),
            "RegulationMappingType": r.get("RegulationMappingType", ""), "MappingStatus": r.get("MappingStatus", ""), "RequiredMappingKeys": r.get("RequiredMappingKeys", ""), "EntityDirectFlag": r.get("EntityDirectFlag", "N"),
            "SamsungRelevanceScore": r.get("SamsungRelevanceScore", 0), "CustomsTradePolicyScore": r.get("CustomsTradePolicyScore", 0), "DirectImpactScore": r.get("DirectImpactScore", 0), "WeightedScore": r.get("WeightedScore", 0),
            "RejectReason": r.get("RejectReason", ""), "KeywordMatches": r.get("KeywordMatches", ""), "effective_date_hint": r.get("effective_date_hint", "본문에서 확인 불가"), "hs_hint": r.get("hs_hint", "본문에서 확인 불가"), "tariff_rate_hint": r.get("tariff_rate_hint", "본문에서 확인 불가"),
            "EvidenceStatus": "VERIFIED" if clean(r.get("Body Verified")) == "Y" and clean(r.get("Evidence")) else ("SUPPORTED" if clean(r.get("Body Verified")) == "Y" else "PENDING"),
            "WhatChanged": clean(r.get("Summary")),
            "PolicyStageDisplay": clean(r.get("Policy Stage")) or clean(r.get("Change Type")),
            "EffectiveDate": clean(r.get("effective_date_hint")) or "확인 필요",
            "AffectedProduct": "확인 필요",
            "AffectedHS": clean(r.get("hs_hint")) or "확인 필요",
            "SamsungImpactPath": clean(r.get("AI Analysis")),
            "RecommendedAction": clean(r.get("Action Plan")),
            "VerificationNeeded": clean(r.get("Missing Facts")) or "추가 확인 없음",
            "OfficialURL": clean(r.get("URL")) if clean(r.get("Body Verified")) == "Y" else "",
        })
    return pd.DataFrame(rows, columns=OUTPUT_COLS)

def normalize_cum_cols(df):
    df=normalize_columns(df)
    for c in OUTPUT_COLS:
        if c not in df.columns: df[c]=""
    return df[OUTPUT_COLS]

def _legacy_compound_title(title):
    markers = re.findall(
        r'(?:법률|대통령령|총리령)제\s*\d+호|'
        r'[가-힣]{2,30}(?:부령|고시|공고|훈령|예규)제?\s*\d{4}(?:[-–]\d+)?호',
        clean(title),
    )
    return len(markers) >= 2

def _clean_legacy_cumulative(df):
    """Migrate legacy STEP4 rows to the current evidence/mapping contract."""
    old = normalize_cum_cols(df)
    if old.empty:
        return old
    keep_rows = []
    for _, row in old.iterrows():
        r = row.copy()
        headline = clean(r.get("Headline"))
        url = clean(r.get("URL"))
        low = headline.lower()
        if not headline or not url:
            continue
        if low in {"feedback", "directorates", "helpdesk", "website policy"}:
            continue
        if re.fullmatch(r"법률\s*제?\s*\d+호", headline):
            continue
        if _legacy_compound_title(headline):
            continue
        if contains_terms(low, ["와인제품", "포도주", "denominação de origem", "denomination of origin", "geographical indication"]):
            continue
        if contains_terms(low, REGULATION_HARD_NOISE_TERMS):
            continue
        if contains_terms(low, [
            "전체 관세청 유관기관", "시스템 작업 안내", "오프라인 작업 안내",
            "시범운영 시행 안내", "서비스 일시중단", "점검 작업 안내",
            "공휴일법", "hari kelepasan", "holiday act", "국세기본법",
            "철도안전법", "음주운전", "범인도피", "상속재산가액",
            "방송통신기자재등 시험기관", "자원순환에 관한 법률", "수출검역요령",
            "마약", "밀수", "특별검사팀", "현장 점검",
            "thúc đẩy hợp tác hải quan", "đối thoại hải quan", "phiên đối thoại",
            "sơ bộ tình hình xuất nhập khẩu", "bắt giữ", "giả nhãn hiệu", "lịch bảo trì hệ thống",
        ]):
            continue

        text = row_text(r)
        topic = detect_topic(text)
        strict = has_strict_trade_reg_signal(text, r)
        if topic == "TRADE_GENERAL" and not strict:
            continue

        body_ok = clean(r.get("Body Verified")).upper() == "Y"
        samsung_named = contains_terms(text, [
            "삼성전자", "samsung electronics", "삼성디스플레이", "samsung display"
        ])
        product_specific = topic in {"AD_CVD", "HS_CLASSIFICATION", "EXPORT_CONTROL", "CBAM_CARBON"}
        prior_mapping_status = clean(r.get("MappingStatus"))
        item_mapped = body_ok and prior_mapping_status in {"MAPPED", "ITEM_1TO1_MAPPED"}

        if samsung_named and body_ok:
            mapping_type, mapping_status, impact = "ENTITY_DIRECT", "ENTITY_CONFIRMED", "Direct"
            required = "SamsungEntity; Transaction"
        elif product_specific:
            mapping_type = "PRODUCT_1TO1"
            mapping_status = "MAPPED" if item_mapped else ("MAPPING_REQUIRED" if body_ok else "VERIFICATION_PENDING")
            impact = "Direct" if item_mapped else "Watch"
            required = "Product; HSCode; OriginCountry; Supplier; SamsungEntity; ImportHistory"
        else:
            mapping_type = "POLICY_GENERAL"
            mapping_status = "VERIFIED_GENERAL" if body_ok else "VERIFICATION_PENDING"
            impact = "Watch"
            required = "Country; SamsungEntity"

        r["RegulationMappingType"] = mapping_type
        r["MappingStatus"] = mapping_status
        r["RequiredMappingKeys"] = required
        r["EntityDirectFlag"] = "Y" if mapping_type == "ENTITY_DIRECT" else "N"
        r["Samsung Impact"] = impact
        policy_score_hint = max(
            pd.to_numeric(pd.Series([r.get("CustomsTradePolicyScore", 0), r.get("Importance Score", 0)]), errors="coerce").fillna(0)
        )
        high_materiality = body_ok and policy_score_hint >= 85
        r["Top3 Eligible"] = "Y" if (impact == "Direct" and body_ok) or high_materiality else "N"
        r["PriorityEligible"] = r["Top3 Eligible"]
        r["DecisionStatus"] = (
            "Verification Pending" if not body_ok else
            "Action Required" if impact == "Direct" else
            "Urgent Verification" if high_materiality else "Monitoring"
        )
        r["SamsungRelevanceScore"] = 100 if impact == "Direct" else 50
        r["DirectImpactScore"] = 100 if impact == "Direct" else 35
        score_value = pd.to_numeric(
            pd.Series([r.get("CustomsTradePolicyScore", 0), r.get("Importance Score", 0)]),
            errors="coerce",
        ).fillna(0)
        policy_score = max(0, min(100, int(score_value.max())))
        importance_raw = pd.to_numeric(r.get("Importance Score", 0), errors="coerce")
        r["Importance Score"] = max(0, min(100, int(importance_raw if pd.notna(importance_raw) else 0)))
        r["CustomsTradePolicyScore"] = policy_score
        r["WeightedScore"] = round(policy_score * 0.4 + int(r["DirectImpactScore"]) * 0.4 + int(r["SamsungRelevanceScore"]) * 0.2)
        keep_rows.append(r)

    cleaned = pd.DataFrame(keep_rows, columns=OUTPUT_COLS)
    if cleaned.empty:
        return normalize_cum_cols(cleaned)
    cleaned["_date_key"] = pd.to_datetime(
        cleaned["Date"], errors="coerce", format="mixed"
    ).dt.strftime("%Y-%m-%d").fillna("")
    cleaned["_title_key"] = cleaned["Headline"].fillna("").astype(str).str.lower().str.replace(r"\s+", " ", regex=True).str.strip()
    identity = cleaned.get("DocumentIdentity", pd.Series("", index=cleaned.index)).fillna("").astype(str).str.lower().str.strip()
    event_key = cleaned.get("EventKey", pd.Series("", index=cleaned.index)).fillna("").astype(str).str.lower().str.strip()
    url_key = cleaned["URL"].fillna("").astype(str).str.lower().str.strip()
    cleaned["_document_key"] = identity.where(identity.ne(""), event_key.where(event_key.ne(""), url_key))
    fallback_key = cleaned["_title_key"] + "|" + cleaned["_date_key"]
    cleaned["_document_key"] = cleaned["_document_key"].where(cleaned["_document_key"].ne(""), fallback_key)
    cleaned = cleaned.drop_duplicates(["_document_key"], keep="last")
    return normalize_cum_cols(cleaned.drop(columns=["_title_key", "_date_key", "_document_key"], errors="ignore"))

def merge_cumulative(daily):
    global CUMULATIVE_REMOVED_DF
    if OUT_CUMULATIVE.exists():
        try:
            old_raw = pd.read_excel(OUT_CUMULATIVE)
            old = _clean_legacy_cumulative(old_raw)
            log(f"cumulative existing load: {len(old_raw)} -> cleaned={len(old)} rows")
            raw_norm = normalize_cum_cols(old_raw)
            kept_keys = set(
                old["URL"].fillna("").astype(str).str.lower().str.strip()
                + "|" + old["Headline"].fillna("").astype(str).str.lower().str.strip()
                + "|" + old["Date"].fillna("").astype(str).str[:10]
            )
            raw_keys = (
                raw_norm["URL"].fillna("").astype(str).str.lower().str.strip()
                + "|" + raw_norm["Headline"].fillna("").astype(str).str.lower().str.strip()
                + "|" + raw_norm["Date"].fillna("").astype(str).str[:10]
            )
            CUMULATIVE_REMOVED_DF = raw_norm.loc[~raw_keys.isin(kept_keys)].copy()
            if not CUMULATIVE_REMOVED_DF.empty:
                CUMULATIVE_REMOVED_DF["RejectReason"] = "LEGACY_CUMULATIVE_REVALIDATION"
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
    # 4자리 숫자만으로는 HS로 인정하지 않는다.
    # 관보 전화번호, 연도, 고시번호를 HS로 오인하는 문제를 방지한다.
    hs = []
    for m in re.finditer(
        r"(?i)(?:HS(?:\s*CODE)?|품목분류|세번)"
        r"\s*[:：-]?\s*(\d{4}(?:[.\s]?\d{2}){0,2})",
        t,
    ):
        code = re.sub(r"\s+", "", m.group(1))
        if code not in hs:
            hs.append(code)
    hs = hs[:6]
    rates = sorted(set(re.findall(r"\b\d{1,3}(?:\.\d+)?\s*%", t)))[:6]
    countries = []
    for c in ["미국", "중국", "일본", "EU", "유럽", "베트남", "인도", "모로코", "한국", "영국", "멕시코", "캐나다", "United States", "China", "Japan", "Vietnam", "India", "Morocco", "Korea", "EU"]:
        if c.lower() in t.lower():
            countries.append(c)
    return {"hs": "; ".join(hs) or "본문에서 확인 불가", "rates": "; ".join(rates) or "본문에서 확인 불가", "countries": "; ".join(dict.fromkeys(countries)) or "본문에서 확인 불가"}


def _is_navigation_or_gazette_shell(text: str) -> bool:
    """관보 검색·메뉴 화면을 법규 본문으로 인정하지 않는다."""
    t = clean(text)
    markers = [
        "관보보기", "기본검색", "고급검색", "인기관보",
        "정정관보", "관보소개", "이용문의",
    ]
    legal_markers = [
        "별표", "개정이유", "주요내용", "부칙",
        "시행한다", "변경 전", "변경 후",
    ]
    return (
        sum(m in t for m in markers) >= 3
        and not any(m in t for m in legal_markers)
    )

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

    if _is_navigation_or_gazette_shell(body):
        body = ""
        status = f"INVALID_GAZETTE_SHELL:{status}"

    cache = _ensure_gemini_cache()
    key = _analysis_cache_key(url, headline)
    cached = cache.get(key)
    if cached and not _is_bad_cached_analysis(cached, headline):
        return cached

    prompt = f"""
당신은 삼성전자 본사 관세·통상 및 관세컴플라이언스 책임자입니다.
아래 공식 법규/공고의 원문만 근거로 GTI Radar 의사결정 보고서를 작성하십시오.

분석 순서(반드시 준수):
1) 사실관계: 문서명, 발표기관·국가, 신규/개정/시행/조사 단계, 발표일·시행일, 대상 품목·HS, 세율·쿼터, 신고·증빙 요건을 원문 근거로 정리.
2) 삼성전자 관세업무 직접영향: 수입·수출통관, HS, 과세가격, 원산지/FTA, AD/CVD, 관세비용, 수출통제 중 실제로 바뀌는 업무와 영향 법인·제품·거래 흐름을 설명.
3) 대응: 즉시(오늘~3영업일), 1개월 내, 상시 모니터링 및 Owner를 분리.

Direct 판정 조건:
- 삼성전자 법인이 소재한 국가의 일반 통관·신고·납부·증빙·세관조사·심판청구·이의신청·행정절차 변경은 해당 국가 법인에 Direct.
- 특정품목의 관세율·HS·AD/CVD·원산지·수출통제 조치는 삼성 제품/원재료 + HS + 영향 법인 + 수출입 경로가 1:1로 확인될 때만 Direct.
- 특정품목 1:1 매핑이 불완전하면 Direct 금지하고 Missing Facts에 누락 키를 기록.
- 국가명, 산업명, 삼성/반도체 단어만으로 Direct 금지. 불명확하면 Indirect 또는 Watch.

절대 금지:
- 제목 반복, 일반론, 모든 법인에 동일한 문구 사용 금지
- 원문에 없는 세율·HS·국가·시행일·대상제품을 추정하거나 지어내지 말 것
- 본문을 읽을 수 없으면 body_verified=false, Samsung Impact=Watch, Top3 Eligible=false

출력은 JSON만:
{{
  "Summary": "[사실관계] 원문 근거 3~5문장. 신규/개정 구분과 적용일 포함",
  "AI Analysis": "[삼성전자 관세업무 직접영향] 영향 경로·법인·제품·업무·비용/리스크를 근거와 함께 4~7문장",
  "Action Plan": "[즉시] ... | [1개월 내] ... | [상시] ... | [Owner] ...",
  "ExecutiveMessage": "무엇이 바뀌며 삼성전자 관세업무가 무엇을 결정해야 하는지 2문장",
  "Samsung Impact": "Direct|Indirect|Watch",
  "Top3 Eligible": false,
  "body_verified": true,
  "change_type": "신규|개정|시행|조사개시|판정|기타",
  "evidence": ["원문 근거1", "원문 근거2"],
  "missing_facts": ["원문에서 확인되지 않은 필수정보"]
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
            ai_impact = clean(result.get("Samsung Impact", "Watch"))
            if ai_impact not in {"Direct", "Indirect", "Watch"}:
                ai_impact = "Watch"
            body_verified = as_bool(result.get("body_verified")) and bool(body) and "TOO_SHORT" not in str(status)
            top3_eligible = as_bool(result.get("Top3 Eligible")) and body_verified and ai_impact == "Direct"
            final = {
                "Summary": summary[:900],
                "AI Analysis": ai[:1200],
                "Action Plan": action_plan[:1200],
                "ExecutiveMessage": (executive or summary)[:700],
                "article_extract_status": f"GEMINI_OK|{status}",
                "Samsung Impact": ai_impact,
                "Top3 Eligible": "Y" if top3_eligible else "N",
                "Body Verified": "Y" if body_verified else "N",
                "Change Type": clean(result.get("change_type", "기타")),
                "Evidence": " | ".join(clean(x) for x in result.get("evidence", []) if clean(x))[:1200],
                "Missing Facts": " | ".join(clean(x) for x in result.get("missing_facts", []) if clean(x))[:900],
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
    final.update({
        "Samsung Impact": "Watch",
        "Top3 Eligible": "N",
        "Body Verified": "Y" if body and "TOO_SHORT" not in str(status) else "N",
        "Change Type": "기타",
        "Evidence": "",
        "Missing Facts": "Gemini 분석 실패 또는 원문 근거 부족",
    })
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

def main():
    print("GTI STEP4-1 REGULATION AI v8.8 KST PRIOR-DAY START")
    print(f"[MODEL] {GEMINI_MODEL}")
    _gti_step4_gemini_log_once()
    _gti_step4_extractor_log_once()
    global KEYWORD_TERMS
    KEYWORD_TERMS = load_keyword_terms()
    log(f"keyword guardrail loaded: {len(KEYWORD_TERMS)} terms")
    df=read_input()
    df, stale_input = strict_24h(df)
    selected, excluded_raw, audit_raw=build(df)
    practical_watch_pattern = re.compile(
        r"수입\s*기계(?:류)?|부분품|부속품|규격\s*확인|위탁가공|관세\s*환급|"
        r"품목분류|검사\s*예규|납세신고\s*정정|전자문서\s*변경|5FE|5FK",
        re.I,
    )
    if not selected.empty:
        selected["CustomsOperationalWatchFlag"] = selected.apply(
            lambda r: "Y" if practical_watch_pattern.search(
                " ".join(clean(r.get(c, "")) for c in ["Headline", "Summary", "Evidence"])
            ) else "N",
            axis=1,
        )
    daily=to_output(selected)
    excluded=to_output(excluded_raw)
    if not stale_input.empty:
        stale_rows = stale_input.copy()
        stale_rows["selected"] = False
        stale_rows["score"] = 0
        stale_rows["Risk"] = "하"
        stale_rows["Issue"] = ""
        stale_rows["topic"] = ""
        stale_rows["RejectReason"] = "STRICT_24H_PUBLISHED_DATE"
        stale_rows["Summary"] = stale_rows.get("Summary", "")
        stale_rows["AI Analysis"] = stale_rows.get("AI Analysis", "")
        stale_rows["Action Plan"] = stale_rows.get("Action Plan", "")
        stale_rows["Samsung Impact"] = "Reference"
        stale_rows["Top3 Eligible"] = "N"
        stale_rows["PriorityEligible"] = "N"
        stale_rows["DecisionStatus"] = "Excluded - older than 24 hours"
        stale_rows["Policy Stage"] = ""
        stale_rows["Body Verified"] = "N"
        stale_rows["Change Type"] = ""
        stale_rows["Evidence"] = ""
        stale_rows["Missing Facts"] = ""
        excluded = pd.concat([excluded, to_output(stale_rows)], ignore_index=True, sort=False)
    cumulative=merge_cumulative(daily)
    write_excel(daily, OUT_SUMMARY); write_excel(cumulative, OUT_CUMULATIVE); write_excel(excluded, OUT_EXCLUDED)
    if not CUMULATIVE_REMOVED_DF.empty:
        write_excel(CUMULATIVE_REMOVED_DF, OUT_CUMULATIVE_REMOVED)
    print(f"[DONE] Daily: {OUT_SUMMARY}")
    print(f"[DONE] Cumulative: {OUT_CUMULATIVE}")
    print(f"[DONE] Excluded: {OUT_EXCLUDED}")
    print(f"[ROWS] daily={len(daily)}, cumulative={len(cumulative)}, excluded={len(excluded)}")
if __name__ == "__main__": main()
