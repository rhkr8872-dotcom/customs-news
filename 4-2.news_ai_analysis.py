# -*- coding: utf-8 -*-
"""
GTI STEP4-2 NEWS AI v47.3 KST PRIOR-DAY QUALITY ENGINE
- Input: 3-2.news_summary.xlsx
- Strict published-date 24h guard
- No legacy v18/v20/v23/v24 override chain
- Gemini: customs/trade YES/NO + Samsung customs impact analysis
- Final: maximum 30 news items
"""

from __future__ import annotations
import os, re, json, time, html as html_lib
from difflib import SequenceMatcher
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse
import pandas as pd
from gti_action_queue_contract import apply_action_queue_contract
import requests
from gti_report_window import previous_kst_day_mask

try:
    from gti_quality_contract import apply_quality_contract, VERSION as CONTRACT_VERSION
except ImportError as exc:
    raise RuntimeError(
        "gti_quality_contract.py must be placed in the same folder as this script"
    ) from exc

BASE_DIR = Path(os.getenv("GTI_BASE_DIR", r"C:\Temp"))
INPUT_FILE = BASE_DIR / "3-2.news_summary.xlsx"
REQUIRED_COLLECTOR_FILES = [
    BASE_DIR / "2-1.naver_news_raw.xlsx",
    BASE_DIR / "2-2.google_news_raw.xlsx",
    BASE_DIR / "2-3.rss_news_raw.xlsx",
]
OUT_SUMMARY = BASE_DIR / "4-2.news_ai_summary.xlsx"
OUT_CUMULATIVE = BASE_DIR / "4-2.news_ai_cumulative.xlsx"
OUT_AUDIT = BASE_DIR / "4-2.news_ai_audit_candidates.xlsx"
OUT_EXCLUDED = BASE_DIR / "4-2.news_ai_excluded.xlsx"
OUT_LEGACY = BASE_DIR / "4.news_ai_analysis.xlsx"
MAPPING_MASTER_XLSX = BASE_DIR / "gti_samsung_customs_mapping.xlsx"
MAPPING_MASTER_CSV = BASE_DIR / "gti_samsung_customs_mapping.csv"

MAX_AGE_HOURS = int(os.getenv("GTI_STEP4_NEWS_MAX_AGE_HOURS", "24"))
INPUT_FILE_MAX_AGE_HOURS = float(os.getenv("GTI_STEP4_INPUT_FILE_MAX_AGE_HOURS", "8"))
TARGET_MAX = int(os.getenv("GTI_STEP4_NEWS_TARGET_MAX", "0"))  # 0 = quality-based, no fixed count
AI_REVIEW_MAX = int(os.getenv("GTI_STEP4_AI_REVIEW_MAX", "120"))
REPORT_TARGET = int(os.getenv("GTI_STEP4_NEWS_REPORT_TARGET", "30"))  # 품질 통과 건수의 상한, 강제 충원 목표가 아님
WATCH_MIN_RELEVANCE = int(os.getenv("GTI_STEP4_WATCH_MIN_RELEVANCE", "3"))
AI_TARIFF_QUOTA = int(os.getenv("GTI_STEP4_AI_TARIFF_QUOTA", "60"))
AI_POLICY_COVERAGE_QUOTA = int(os.getenv("GTI_STEP4_AI_POLICY_COVERAGE_QUOTA", "60"))
GEMINI_MODEL = os.getenv("GTI_GEMINI_MODEL", "gemini-2.5-flash-lite").strip()
GEMINI_TIMEOUT = int(os.getenv("GTI_GEMINI_TIMEOUT", "20"))
GEMINI_API_KEY = (
    os.getenv("GEMINI_API_KEY", "").strip()
    or os.getenv("GOOGLE_API_KEY", "").strip()
)
USE_GEMINI = bool(GEMINI_API_KEY)

ISSUE_WEIGHT = {
    "EXPORT_CONTROL": 25,
    "AD_CVD": 24,
    "TARIFF": 22,
    "ORIGIN_FTA": 20,
    "HS_CLASSIFICATION": 19,
    "CUSTOMS": 18,
    "CBAM": 17,
    "TRADE_POLICY": 12,
}

SAMSUNG_TERMS = [
    "samsung", "삼성전자", "삼성", "semiconductor", "반도체", "chip",
    "smartphone", "스마트폰", "display", "oled", "battery", "배터리",
    "steel", "철강", "aluminum", "알루미늄", "polysilicon", "폴리실리콘",
]

OUTPUT_COLS = [
    "No", "Content Type", "Mail Group", "Samsung Impact", "Affected Subsidiary",
    "Impact Reason", "Date", "Publish Date", "Headline", "Summary", "AI Analysis",
    "Action Plan", "Country", "Agency", "Risk", "Importance Score",
    "Priority Group", "Issue", "Cluster", "URL", "Source", "Source File",
    "RejectReason", "AIRelevant", "AIRelevanceScore", "AIReason",
    "Top3 Eligible", "Body Verified", "Direct Evidence", "Missing Facts",
    "Policy Event", "Official Evidence",
    "RegulationMappingType", "MappingStatus", "RequiredMappingKeys", "EntityDirectFlag",
    "MappedEntity", "MappedProduct", "MappedHS", "TradeRoute", "MappingEvidence",
    "Article Extract Status", "Article Source Type", "Article Body Evidence",
    "Policy Stage", "Quality Contract",
    "PolicyDeltaFlag", "PolicyDeltaType", "PolicyDeltaReason", "ReportLayer",
    "CustomsCaseFlag", "CustomsCaseType", "CustomsCaseReason",
    "DirectCustomsCaseFlag", "PeerPrecedentFlag",
    "OfficialSourceURL", "OfficialSourceStatus", "OfficialSourceCandidate",
    "EvidenceGateFlag", "SamsungTradeGateFlag", "ThreeGateStatus",
    "EventDuplicateCount", "AlternateHeadlines", "AlternateURLs",
    "EvidenceStatus", "WhatChanged", "PolicyStageDisplay", "EffectiveDate",
    "AffectedProduct", "AffectedHS", "SamsungImpactPath", "RecommendedAction",
    "VerificationNeeded", "OfficialURL",
]

HARD_SCOPE_EXCLUDE_TITLE_TERMS = [
    "마약", "코카인", "필로폰", "합성대마", "대마초", "다크웹", "마약수사",
    "drug bust", "drug seizure", "narcotics", "cocaine", "methamphetamine",
    "기술 탈취", "기술탈취", "기술 유출", "산업기술 유출", "영업비밀", "폭탄 증언",
    "trade secret theft", "technology theft", "industrial espionage",
]
MACRO_NOISE_TITLE_TERMS = [
    "인플레이션", "물가", "성장률", "수출 호황", "수출 증가", "일자리 전망",
    "주가", "특징주", "실적", "경제 전망", "증시", "시황",
    "inflation", "economic growth", "stock price", "earnings", "market outlook",
]
CONCRETE_TITLE_ACTION_TERMS = [
    "section 232", "section 301", "232조", "301조", "반덤핑", "상계관세",
    "세이프가드", "예비판정", "최종판정", "조사 개시", "관세 부과", "관세 인상",
    "수입금지", "수출통제", "원산지 규정", "통관절차 개정", "품목분류 결정",
    "anti-dumping", "countervailing", "safeguard", "tariff imposed", "export control",
]


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


def as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return clean(value).lower() in {"1", "true", "yes", "y"}


def safe_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


EVENT_ONLY_TERMS = [
    "경진대회", "품목분류 경진", "실력 겨루", "겨룬다", "퀴즈대회", "공모전", "시상식",
    "세미나", "웨비나", "설명회", "포럼", "컨퍼런스", "워크숍",
    "contest", "competition", "seminar", "webinar", "conference", "workshop",
]
CONCRETE_MEASURE_TERMS = [
    "시행", "발효", "부과", "인상", "인하", "폐지", "개정", "공포", "잠정조치",
    "조사 개시", "예비판정", "최종판정", "결정 고시", "명령", "고시",
    "effective", "entered into force", "imposed", "imposes", "introduced", "introducing",
    "will apply", "increased", "reduced",
    "amended", "investigation initiated", "preliminary determination",
    "final determination", "order", "notice",
]
OPINION_TITLE_TERMS = [
    "[세풍", "[사설", "[칼럼", "[기고", "오피니언", "column", "opinion", "editorial",
]

# v45.0: article-body verification alone does not make a reportable policy.
MARKET_COMPANY_NOISE_TERMS = [
    "주가", "급락", "급등", "신용등급", "실적 개선", "매출 증가", "시장점유율",
    "제품 가격", "가격 인상", "판매 증가", "시장 공략", "기업들", "날개 달고",
    "stock price", "shares fell", "shares rose", "credit rating", "earnings",
]
FOOD_SUPPLY_NOISE_TERMS = [
    "수입콩", "국산콩", "두부", "식품업계", "농식품부", "재고 한계", "원료 절벽",
]
POLICY_INSTRUMENT_TERMS = [
    "법률안", "개정안", "시행령", "시행규칙", "행정명령", "고시", "공고",
    "예비판정", "최종판정", "조사 개시", "조사개시", "관세율", "추가관세",
    "면세 폐지", "면세기준", "쿼터", "할당관세", "수입금지", "수출금지",
    "regulation", "decree", "executive order", "official notice",
    "preliminary determination", "final determination", "investigation initiated",
    "tariff rate", "additional tariff", "duty-free", "quota", "import ban",
    "safeguard", "safeguard measure", "trade remedy", "fta", "free trade agreement",
    "자유무역협정", "관세 개혁", "customs reform", "union customs code",
]


def concrete_policy_delta(text: object) -> bool:
    """Require an identifiable new/changed government customs measure."""
    t = clean(text).lower()
    if not t:
        return False
    has_instrument = any(x in t for x in POLICY_INSTRUMENT_TERMS)
    has_measure = any(x in t for x in CONCRETE_MEASURE_TERMS)
    has_authority = any(x in t for x in [
        "정부", "관세청", "세관", "상무부", "재무부", "무역위원회", "국회",
        "유럽연합", "필리핀", "commission", "customs", "department of commerce",
        "ministry", "wto", "european union", "eu ",
    ])
    return has_instrument and (has_measure or has_authority)


def customs_case_gate(row: pd.Series) -> tuple[str, str, str]:
    """Preserve Samsung-relevant customs cases even when no policy changed.

    This gate is deliberately separate from PolicyDeltaFlag.  Enforcement,
    classification and precedent are operational customs intelligence, not a
    manufactured policy amendment.
    """
    text = " ".join(clean(row.get(c)) for c in [
        "Headline", "Summary", "SummaryAI", "Direct Evidence",
        "Article Body Evidence", "Issue", "Agency", "Source",
    ]).lower()
    samsung_named = bool(re.search(
        r"삼성전자|삼성sdi|samsung electronics|samsung semiconductor|samsung sdi", text, re.I
    ))
    product_scope = bool(re.search(
        r"반도체|semiconductor|\bchip\b|메모리|스마트폰|휴대폰|display|oled|"
        r"이차전지|배터리|battery|전자제품|가전|네트워크장비|통신장비|"
        r"제조설비|반도체장비|냉각장치|센서|전자부품|설비부품", text, re.I
    ))
    peer_named = bool(re.search(
        r"lg전자|엘지전자|lg electronics|sk하이닉스|sk hynix|tsmc|asml|"
        r"인텔|intel|마이크론|micron|키옥시아|kioxia|애플|apple", text, re.I
    ))
    mapped_samsung = (
        bool(re.search(r"삼성|samsung", clean(row.get("MappedEntity")), re.I))
        and bool(clean(row.get("MappedProduct")) or clean(row.get("MappedHS")))
        and bool(clean(row.get("TradeRoute")))
    )
    # A product buzzword alone must never create a Samsung Watch case.
    # Require Samsung, an electronics peer with the actual product, or a
    # complete Samsung entity/product/route mapping in article-native data.
    samsung_scope = samsung_named or (peer_named and product_scope) or mapped_samsung
    if not samsung_scope:
        return "N", "", ""
    case_rules = [
        ("CUSTOMS_ENFORCEMENT", r"관세청|세관|customs|관세당국", r"조사\s*개시|조사\s*착수|세무조사|심사\s*통지|추징\s*통지|적발|단속|불법\s*수출|우회\s*수출|밀수|관세\s*포탈|탈세|추징|압수|enforcement|seizure|evasion|assessment notice|duty demand"),
        ("HS_CLASSIFICATION_CASE", r"품목분류|세번|hs\s*(?:code|classification)|관세율", r"결정|변경|사전심사|위원회\s*결정|분류\s*결정|classification ruling|advance ruling"),
        ("CUSTOMS_PRECEDENT", r"조세심판원|대법원|법원|관세법원|심판원|tribunal|court", r"심판|판례|불복|소송|재결|판결|결정|appeal|ruling|judgment|decision"),
        ("EXPORT_CONTROL_CASE", r"수출통제|전략물자|수출허가|재수출|환적|export control|export license|re-export|transshipment", r"적발|단속|위반|허가\s*거부|수출\s*금지|제재|enforcement|violation|license denial|ban|sanction"),
    ]
    for case_type, subject, action in case_rules:
        if re.search(subject, text, re.I) and re.search(action, text, re.I):
            # DIRECT means Samsung itself is the named target of the action.
            # A general tariff affecting a Samsung product family is Watch,
            # never a direct customs case.
            samsung_target = samsung_named and bool(re.search(
                r"삼성(?:전자|sdi)?.{0,80}(조사|추징|과세|적발|단속|판정|거부|제재|"
                r"investigation|assessment|seizure|ruling|denial|sanction)", text, re.I
            ))
            prefix = "DIRECT_" if samsung_target else ("PEER_" if peer_named else "")
            reason = (
                "삼성전자를 명시적으로 대상으로 한 관세·수출통제 조치"
                if samsung_target else
                "삼성 제품군과 동일·유사한 동종업계 관세 실무 선례"
                if peer_named else
                "삼성 제품·설비·부품과 연결되는 관세 실무 사례"
            )
            return "Y", prefix + case_type, reason
    return "N", "", ""


OFFICIAL_HOSTS = (
    ".gov", ".gov.uk", ".gov.cn", ".gov.in", ".gov.vn", ".go.kr", ".gc.ca",
    "europa.eu", "wto.org", "ustr.gov", "cbp.gov", "bis.gov", "trade.gov",
    "federalregister.gov", "usitc.gov", "customs.go.kr", "motie.go.kr",
    "mofcom.gov.cn", "dgft.gov.in", "taxation-customs.ec.europa.eu",
)
OFFICIAL_AGENCY_HOME = {
    "ustr": "https://ustr.gov/", "cbp": "https://www.cbp.gov/",
    "usitc": "https://www.usitc.gov/", "federal register": "https://www.federalregister.gov/",
    "미 상무부": "https://www.trade.gov/", "department of commerce": "https://www.trade.gov/",
    "bis": "https://www.bis.gov/", "관세청": "https://www.customs.go.kr/",
    "무역위원회": "https://www.ktc.go.kr/", "산업통상": "https://www.motie.go.kr/",
    "dgft": "https://www.dgft.gov.in/", "mofcom": "https://english.mofcom.gov.cn/",
    "eu commission": "https://policy.trade.ec.europa.eu/",
    "european commission": "https://policy.trade.ec.europa.eu/",
}


def _is_official_url(value: object) -> bool:
    url = clean(value)
    if not url.startswith(("http://", "https://")):
        return False
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return False
    return any(host == x.lstrip(".") or host.endswith(x) for x in OFFICIAL_HOSTS)


def official_source_fields(row: pd.Series) -> tuple[str, str, str]:
    """Exact official URL is evidence; an agency homepage is only a candidate."""
    for field in [
        "OfficialSourceURL", "URL", "OriginalURLCandidate", "BestLinkURL",
        "Source", "Official Evidence", "Direct Evidence",
    ]:
        value = clean(row.get(field))
        urls = re.findall(r"https?://[^\s|<>'\"]+", value) if value else []
        if value.startswith(("http://", "https://")):
            urls.insert(0, value)
        for url in dict.fromkeys(urls):
            if _is_official_url(url):
                return url.rstrip(".,);]"), "VERIFIED_EXACT", ""
    agency_text = " ".join([
        clean(row.get("Agency")), clean(row.get("Publisher")),
        clean(row.get("Official Evidence")), clean(row.get("Country")),
    ]).lower()
    candidate = next((home for key, home in OFFICIAL_AGENCY_HOME.items() if key in agency_text), "")
    return "", ("PENDING_DISCOVERY" if candidate else "NOT_IDENTIFIED"), candidate


def policy_delta_gate(row: pd.Series) -> tuple[str, str, str]:
    """Identify a new policy change; non-delta rows remain as Global Context."""
    title = clean(row.get("Headline")).lower()
    lead = " ".join([
        clean(row.get("SummaryAI")), clean(row.get("Direct Evidence")),
        clean(row.get("Official Evidence")), clean(row.get("Article Body Evidence"))[:1400],
    ]).lower()
    anchor = f"{title} {lead}"
    issue = clean(row.get("Issue")).upper()
    official_url, official_status, _ = official_source_fields(row)
    commentary = any(x in title for x in [
        "전망", "분석", "연구", "효과", "영향", "증가", "감소", "기고", "칼럼",
        "왜 ", "시사점", "outlook", "analysis", "study", "impact", "opinion",
    ])
    event_only = any(x in title for x in [
        "대화 개최", "회의 개최", "포럼", "세미나", "협력 강화", "의견 교환",
        "mou 체결", "양해각서", "dialogue", "forum", "seminar", "meeting",
    ])
    operative_terms = [
        "시행", "발효", "부과", "철회", "폐지", "면제", "인상", "인하", "개정",
        "수입 제한", "수입제한",
        "조사 개시", "예비판정", "최종판정", " 판정", "지정", "삭제", "수입금지",
        "entered into force", "effective from", "imposed", "imposes", "introduced",
        "introducing", "will apply", "lifted", "removed", "in force",
        "amended", "investigation initiated", "preliminary determination",
        "final determination", "designated", "added to", "import ban", "import restriction", "will remove",
    ]
    proposal_terms = [
        "법률안", "개정안", "입법예고", "행정예고", "공식 제안", "제안했다",
        "하겠다", "예정", "계획", "proposed rule", "proposal", "notice and comment",
        "plans to impose", "plans to lift", "will impose", "기본 합의", "실질적인 합의",
        "합의에 도달", "협상 마무리", "agreement reached", "substantial agreement",
    ]
    title_operative = any(x in title for x in operative_terms)
    title_proposal = any(x in title for x in proposal_terms)
    # Body-only historical references must not manufacture a current event.
    # An exact official page may supply the operative phrase when a terse
    # government title omits it; media articles must carry the change in title.
    operative = title_operative or (official_status == "VERIFIED_EXACT" and any(x in anchor for x in operative_terms))
    proposal = title_proposal
    instrument = concrete_policy_delta(anchor) or issue_specific_policy_signal(issue, anchor)
    if event_only and not (title_operative or title_proposal):
        return "N", "EVENT_CONTEXT", "회의·대화·협력행사이며 변경된 규정·세율·허가요건이 확인되지 않음"
    if commentary and not (title_operative or title_proposal):
        return "N", "BACKGROUND_ANALYSIS", "정책 영향·통계·전망 분석이며 당일 신규 정부조치가 확인되지 않음"
    if operative and instrument:
        reason = "구체 정책수단과 시행·부과·판정 등 현재 조치 확인"
        if official_status != "VERIFIED_EXACT":
            reason += "; 공식 원문 링크 추가 확인 필요"
        return "Y", "CONFIRMED_DELTA", reason
    if proposal and instrument:
        return "Y", "PROPOSED_DELTA", "정부의 공식 입법·행정 제안 단계로 확인"
    if official_url and instrument:
        return "Y", "OFFICIAL_DELTA", "공식기관 원문과 구체 정책수단 확인"
    return "N", "NO_CURRENT_DELTA", "관세·통상 관련성은 있으나 현재 발생한 신규 정책변화가 확인되지 않음"


def event_only_noise(title: object, body: object = "") -> bool:
    title_text = clean(title).lower()
    full_text = f"{title_text} {clean(body).lower()}"
    return (
        any(term in title_text for term in EVENT_ONLY_TERMS)
        and not any(term in full_text for term in CONCRETE_MEASURE_TERMS)
    )


def opinion_article(title: object) -> bool:
    t = clean(title).lower()
    return any(term in t for term in OPINION_TITLE_TERMS) or any(term in t for term in [
        "why does everyone hate", "paul krugman", "역사적 배경", "1980년대", "1990년대",
    ])


def hard_scope_excluded(title: object) -> bool:
    t = clean(title).lower()
    if any(term in t for term in HARD_SCOPE_EXCLUDE_TITLE_TERMS):
        return True
    return (
        any(term in t for term in MACRO_NOISE_TITLE_TERMS)
        and not any(term in t for term in CONCRETE_TITLE_ACTION_TERMS)
    )


def business_scope_noise(title: object, text: object = "") -> bool:
    """관세·전략물자 업무가 아닌 세무·식품·특허·노무·증시 기사를 결정론적으로 차단."""
    t = f"{clean(title)} {clean(text)}".lower()
    title_l = clean(title).lower()
    tax_only = any(x in title_l for x in [
        "글로벌최저한세", "글로벌 최저한세", "법인세", "원천징수", "부가가치세",
        "세무 노하우", "세정경험", "세정 경험", "국세청",
    ]) and not any(x in t for x in ["관세청", "관세율", "수입관세", "customs duty", "tariff"])
    food_only = any(x in title_l for x in [
        "김치", "배추", "식약처", "식품안전", "우유 수입", "농업 생산",
    ]) and not any(x in t for x in ["삼성", "samsung"])
    patent_only = any(x in title_l for x in ["특허", "라이선스", "patent", "license"]) and not any(
        x in t for x in ["itc exclusion order", "수입금지명령", "세관 압류", "customs seizure"]
    )
    labor_only = any(x in title_l for x in ["노조", "파업", "임단협", "부분파업", "strike"])
    market_only = any(x in title_l for x in ["금리인상", "금리 인상", "7천피", "코스피", "증시", "주가", "시황"])
    promotion_only = any(x in title_l for x in [
        "상담회", "파트너십 포럼", "전략회의", "경협 지원", "교역 2배", "시장 공략",
    ]) and not any(x in t for x in [
        "협정문 개정", "원산지 규정 개정", "관세양허 변경", "서명", "비준", "발효",
        "rules of origin amended", "tariff schedule", "entered into force",
    ])
    consumer_or_agri_only = any(x in t for x in [
        "합성니코틴", "유사니코틴", "마운자로", "위고비", "농축산물", "농가경제",
        "일본 농업", "유전자 변형 감자", "고용보험", "워크셰어링",
        "benefit-processing", "service canada", "jewelers", "다이아몬드", "보석류",
    ]) and not any(x in t for x in ["삼성전자", "samsung electronics"])
    unrelated_vehicle_commentary = any(x in title_l for x in [
        "현대차", "car problem is not a customs problem", "영국의 경고장",
    ]) and not any(x in t for x in ["삼성전자", "samsung electronics", "반도체", "semiconductor"])
    return (tax_only or food_only or patent_only or labor_only or market_only
            or promotion_only or consumer_or_agri_only or unrelated_vehicle_commentary)


def issue_specific_policy_signal(issue: object, text: object) -> bool:
    """이슈명과 실제 정책조치의 주어·행위를 함께 확인한다."""
    i = clean(issue).upper()
    t = clean(text).lower()
    rules = {
        "TARIFF": (
            ["관세", "관세율", "추가관세", "section 232", "section 301", "232조", "301조", "tariff"],
            ["부과", "인상", "인하", "면제", "유예", "철회", "유지", "연장", "확대", "검토", "조사 개시", "발표", "impose", "increase", "reduce", "exempt", "extend", "review", "investigation", "announce"],
        ),
        "AD_CVD": (
            ["반덤핑", "상계관세", "anti-dumping", "antidumping", "countervailing"],
            ["조사 개시", "예비판정", "최종판정", "부과", "종료", "initiat", "preliminary", "final determination", "impose", "terminate"],
        ),
        "EXPORT_CONTROL": (
            ["전략물자", "수출통제", "수출 금지", "entity list", "엔티티 리스트", "export control", "export restriction"],
            ["지정", "추가", "삭제", "개정", "시행", "강화", "완화", "허가", "금지", "검토", "designat", "added", "removed", "amend", "effective", "tighten", "license", "ban", "review"],
        ),
        "SANCTIONS": (
            ["제재", "sanction", "sdn", "ofac"],
            ["지정", "추가", "차단", "금지", "해제", "강화", "시행", "designat", "added", "block", "prohibit", "lift", "effective"],
        ),
        "CUSTOMS": (
            ["관세청", "세관", "통관", "수입신고", "수출신고", "customs", "clearance", "declaration"],
            ["개정", "시행", "강화", "완화", "일원화", "전수 검사", "결정", "amend", "effective", "tighten", "simplif", "decision"],
        ),
        "HS_CLASSIFICATION": (
            ["품목분류", "hs code", "hs코드", "classification ruling"],
            ["결정", "변경", "개정", "고시", "판결", "ruling", "change", "amend", "notice"],
        ),
        "ORIGIN_FTA": (
            ["원산지", "fta", "cepa", "cptpp", "자유무역협정", "rules of origin"],
            ["개정", "발효", "체결", "서명", "비준", "협상 재개", "조사 개시", "단속 강화", "amend", "effective", "signed", "ratif", "negotiation", "enforcement"],
        ),
        "CBAM_CARBON": (
            ["cbam", "탄소국경조정", "탄소 국경 조정"],
            ["시행", "발효", "신고", "인증서", "개정", "유예", "effective", "reporting", "certificate", "amend", "defer"],
        ),
    }
    subject, action = rules.get(i, ([], []))
    return bool(subject and action and any(x in t for x in subject) and any(x in t for x in action))


def official_primary_evidence(value: object) -> bool:
    t = clean(value).lower()
    if not t or any(x in t for x in ["언론", "보도", "기사", "로이터", "폴리티코", "전망", "관계자"]):
        return False
    return any(x in t for x in [
        "관보", "연방관보", "federal register", "ustr", "cbp", "bis", "ofac", "미 재무부",
        "미 상무부", "미국 상무부", "무역위원회", "국제무역위원회", "eu 집행위원회", "commission regulation", "관세청", "세관", "법원",
        "행정명령", "고시", "공식문서", "official gazette", "regulation (eu)", "decision",
    ])


def confirmed_direct_stage(text: object) -> bool:
    t = clean(text).lower()
    tentative = any(x in t for x in [
        "검토", "추진", "가능성", "예상", "방안", "계획", "협상 중", "논의", "보도",
        "consider", "review", "proposal", "proposed", "may", "could", "plan",
    ])
    operative = any(x in t for x in [
        "시행", "발효", "부과", "최종판정", "조사 개시", "명령", "고시", "지정", "수입금지",
        "effective", "entered into force", "imposed", "final determination", "investigation initiated",
        "executive order", "designated", "import ban",
    ])
    return operative and not tentative


def _event_title(v: object) -> str:
    t = clean(v).lower()
    t = re.sub(r"\s+-\s+[^-]{2,50}$", "", t)
    t = re.sub(r"[^0-9a-z가-힣%]+", " ", t)
    stop = {
        "속보", "종합", "단독", "오늘", "뉴스", "관련", "대한", "위한", "통해", "검토",
        "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with", "says",
    }
    return " ".join(x for x in t.split() if x not in stop)


def _event_anchor(row: pd.Series) -> str:
    raw_context = f"{clean(row.get('Headline'))} {clean(row.get('Summary'))} {clean(row.get('Country'))}".lower()
    t = _event_title(row.get("Headline"))
    context = f"{raw_context} {t} {_event_title(row.get('Summary'))}"
    issue = clean(row.get("Issue")).upper() or "OTHER"
    # High-frequency cross-publisher events: use policy-event identity rather
    # than the occasionally inconsistent upstream Issue label.
    if all([
        any(x in context for x in ["러시아", "russia"]),
        any(x in context for x in ["이란", "iran", "원유", "oil", "natural gas"]),
        any(x in context for x in ["100%", "100％", "secondary tariff", "제재법", "sanctions act"]),
    ]):
        return "EVENT|US_RUSSIA_IRAN_SECONDARY_TARIFF_100"
    if all([
        any(x in context for x in ["대미투자", "미국 투자", "u.s. investment", "us investment"]),
        any(x in context for x in ["301조", "section 301", "232조", "section 232", "관세"]),
        any(x in context for x in ["중간재", "자본재", "설비", "equipment", "capital goods"]),
    ]):
        return "EVENT|KR_US_INVESTMENT_301_232_COST"
    if all([
        any(x in context for x in ["트럼프", "미국", "u.s."]),
        any(x in context for x in ["시진핑", "중국", "china"]),
        any(x in context for x in ["정상회담", "회담", "summit", "관세 담판", "tariff negotiation"]),
    ]):
        return "EVENT|US_CHINA_SUMMIT_TARIFF_NEGOTIATION"
    if all([
        any(x in context for x in ["반도체", "semiconductor", "chip"]),
        any(x in context for x in ["100%", "100％", "관세", "tariff"]),
        any(x in context for x in ["미국 투자", "현지 생산", "local production", "invest"]),
    ]):
        return "EVENT|US_SEMICON_TARIFF_INVESTMENT_PRESSURE"
    if all([
        any(x in context for x in ["베트남", "vietnam"]),
        any(x in context for x in ["수출통제", "전략물자", "export control"]),
        any(x in context for x in ["hs코드", "hs code", "기술사양", "재수출", "환적", "re-export", "transshipment"]),
    ]):
        return "EVENT|VN_STRATEGIC_EXPORT_CONTROL"
    if any(x in context for x in ["멕시코", "mexico"]) and any(x in context for x in ["중국", "중국산", "china", "chinese"]) and any(x in context for x in ["관세", "tariff"]):
        return "EVENT|MEXICO_CHINA_ADDITIONAL_TARIFF"
    if any(x in context for x in ["h200", "엔비디아칩", "엔비디아 칩", "nvidia chip"]) and any(x in context for x in ["중국", "china", "中"]) and any(x in context for x in ["허용", "완화", "반입", "수입", "빗장 일부", "allow", "import"]):
        return "EVENT|CHINA_NVIDIA_H200_IMPORT_RELAXATION"
    if any(x in context for x in ["우회수출", "환적", "transshipment", "관세 회피"]) and any(x in context for x in ["미국", "백악관", "유럽", "eu", "u s"]):
        return "EVENT|US_EU_CHINA_CIRCUMVENTION_ENFORCEMENT"
    if (
        any(x in context for x in ["미국", "美", "미,", "미-", "미·", "미 캐나다", "미 관세", "usa", "us ", "u.s.", "united states"])
        and any(x in context for x in ["캐나다", "canada"])
        and any(x in context for x in ["관세", "tariff"])
    ):
        return "EVENT|US_CANADA_50PCT_RETALIATORY_TARIFFS"
    if any(x in context for x in ["호우", "침수", "수해", "flood"]) and any(x in context for x in ["관세", "세관", "customs", "납부기한", "관세조사", "원산지검증", "신속통관", "통관 지원", "지원책"]):
        return "EVENT|KR_FLOOD_CUSTOMS_RELIEF"
    if any(x in context for x in ["300억달러", "30 billion", "$30 billion"]) and any(x in context for x in ["미중", "미·중", "미국", "중국", "u.s.", "china"]) and any(x in context for x in ["상호 관세", "관세인하", "관세 인하", "tariff reduction"]):
        return "EVENT|US_CHINA_30B_TARIFF_NEGOTIATION"
    if any(x in context for x in ["tbfc", "무역기반 재정", "무역기반 금융", "특별수사단", "특수단"]) and any(x in context for x in ["관세청", "관세 범죄", "시장교란", "부정수급"]):
        return "EVENT|KR_TBFC_TASK_FORCE"
    if any(x in context for x in ["공시송달", "온라인 게시", "유니패스", "국가관세종합정보시스템"]) and any(x in context for x in ["관세법", "국세기본법", "조승래"]):
        return "EVENT|KR_CUSTOMS_PUBLIC_NOTICE_BILL"
    if any(x in context for x in ["수입콩", "두부", "국산콩"]) and any(x in context for x in ["부족", "공급", "재고", "할당"]):
        return "EVENT|KR_SOYBEAN_SUPPLY"
    countries = [
        "미국", "중국", "멕시코", "캐나다", "한국", "일본", "인도", "베트남", "브라질",
        "유럽", "eu", "usa", "china", "mexico", "canada", "korea", "japan", "india",
    ]
    products = [
        "반도체", "h200", "폴리실리콘", "철강", "자동차", "드론", "선재", "황산", "알루미늄",
        "semiconductor", "polysilicon", "steel", "vehicle", "drone", "wire rod", "aluminum",
    ]
    actions = [
        "추가 관세", "관세 인상", "반덤핑", "상계관세", "우회수출", "원산지", "수출통제",
        "수입제한", "재심", "예비판정", "최종판정", "section 232", "section 301",
        "tariff", "anti dumping", "countervailing", "transshipment", "export control",
    ]
    picked = []
    for group in (countries, products, actions):
        found = next((x for x in group if x in t), "")
        if found:
            picked.append(found)
    rate = re.search(r"\b\d{1,3}(?:\.\d+)?%", t)
    if rate:
        picked.append(rate.group(0))
    return issue + "|" + "|".join(picked) if len(picked) >= 2 else ""


def pre_ai_event_dedup(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compress same-event articles before Gemini calls; keep the best representative."""
    if df.empty:
        return df.copy(), df.iloc[0:0].copy()
    kept_idx, dropped_idx = [], []
    representatives = []
    for idx, row in df.iterrows():
        title = _event_title(row.get("Headline"))
        issue = clean(row.get("Issue")).upper()
        cluster = clean(row.get("Cluster"))
        anchor = _event_anchor(row)
        duplicate = False
        for rep in representatives:
            same_cluster = bool(cluster and cluster == rep["cluster"])
            same_anchor = bool(anchor and anchor == rep["anchor"])
            similarity = SequenceMatcher(None, title, rep["title"]).ratio() if issue == rep["issue"] else 0.0
            if same_cluster or same_anchor or similarity >= 0.62:
                duplicate = True
                break
        if duplicate:
            dropped_idx.append(idx)
        else:
            kept_idx.append(idx)
            representatives.append({"title": title, "issue": issue, "cluster": cluster, "anchor": anchor})
    kept = df.loc[kept_idx].copy().reset_index(drop=True)
    dropped = df.loc[dropped_idx].copy()
    if not dropped.empty:
        dropped["RejectReason"] = "PRE_AI_SAME_EVENT_DUPLICATE"
    log(f"PRE-AI EVENT DEDUP: {len(df)} -> {len(kept)} / removed={len(dropped)}")
    return kept, dropped


_MAPPING_CACHE = None


def load_mapping_master() -> pd.DataFrame:
    global _MAPPING_CACHE
    if _MAPPING_CACHE is not None:
        return _MAPPING_CACHE
    path = MAPPING_MASTER_XLSX if MAPPING_MASTER_XLSX.exists() else MAPPING_MASTER_CSV
    if not path.exists():
        log("MAPPING MASTER missing: gti_samsung_customs_mapping.xlsx/csv / Direct requires article-level entity evidence")
        _MAPPING_CACHE = pd.DataFrame()
        return _MAPPING_CACHE
    try:
        frame = pd.read_excel(path) if path.suffix.lower() == ".xlsx" else pd.read_csv(path, encoding="utf-8-sig")
        frame = unique_columns(frame)
        active = pick_col(frame, ["Active", "Use", "Enabled"])
        if active:
            frame = frame[~frame[active].fillna("Y").astype(str).str.upper().isin({"N", "NO", "0", "FALSE"})]
        _MAPPING_CACHE = frame.fillna("")
        log(f"MAPPING MASTER loaded: {path.name} / rows={len(_MAPPING_CACHE)}")
    except Exception as exc:
        raise RuntimeError(f"mapping master read failed: {path} / {type(exc).__name__}: {exc}")
    return _MAPPING_CACHE


def _term_in_text(text: str, term: str) -> bool:
    term = clean(term).lower()
    if not term:
        return False
    if re.fullmatch(r"[a-z0-9]{2,5}", term):
        return bool(re.search(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])", text))
    return term in text


def _explicit_samsung_customs_context(title: str, body: str) -> tuple[bool, str]:
    """Return only article-native Samsung evidence, excluding related-link/sidebar noise."""
    samsung_terms = ["삼성전자", "삼성 전자", "samsung electronics", "samsung semiconductor"]
    product_terms = [
        "반도체", "메모리", "스마트폰", "휴대전화", "tv", "텔레비전", "디스플레이", "가전",
        "네트워크 장비", "배터리", "semiconductor", "memory", "smartphone", "display", "appliance",
    ]
    customs_terms = [
        "관세", "통관", "반덤핑", "상계관세", "세이프가드", "원산지", "품목분류", "수출통제",
        "tariff", "customs", "anti-dumping", "countervailing", "origin", "export control",
        "section 232", "section 301",
    ]
    headline = clean(title).lower()
    candidates = [headline]
    candidates.extend(
        clean(x).lower() for x in re.split(r"(?:[.!?。]\s+|다\.\s+|\n+)", clean(body)[:12000])
        if clean(x)
    )
    for sentence in candidates:
        if (
            any(_term_in_text(sentence, x) for x in samsung_terms)
            and any(_term_in_text(sentence, x) for x in product_terms)
            and any(_term_in_text(sentence, x) for x in customs_terms)
        ):
            return True, sentence[:700]
    return False, ""


def _explicit_hs(text: str) -> str:
    """Accept HS only when explicitly labelled; never treat dates/years as HS."""
    m = re.search(
        r"\bhs(?:\s*code|\s*코드|\s*세번)?\s*[:#-]?\s*(\d{4,10}(?:\.\d{2,6})?)\b",
        clean(text), re.I,
    )
    return clean(m.group(1)) if m else ""


def map_article_to_business(title: str, body: str, country: str, issue: str, verified: bool) -> dict:
    text = f"{title} {body} {country}".lower()
    samsung_named, samsung_evidence = _explicit_samsung_customs_context(title, body)
    hs_value = _explicit_hs(f"{title} {body}")
    product_terms = [
        "반도체", "메모리", "스마트폰", "휴대전화", "tv", "텔레비전", "디스플레이", "가전",
        "네트워크 장비", "배터리", "semiconductor", "memory", "smartphone", "display", "appliance",
    ]
    product_text = f"{clean(title).lower()} {samsung_evidence}"
    product = next((x for x in product_terms if _term_in_text(product_text, x)), "")
    trade_route = ""
    if any(x in text for x in ["환적", "우회수출", "transshipment", "원산지 세탁"]):
        trade_route = "TRANSshipment/origin route"

    master = load_mapping_master()
    matched = None
    for _, mr in master.iterrows():
        def mv(names):
            c = pick_col(master, names)
            return clean(mr.get(c)) if c else ""
        m_country = mv(["Country", "국가"])
        m_entity = mv(["Entity", "Subsidiary", "법인"])
        m_product = mv(["Product", "제품"])
        m_hs = mv(["HS", "HSCode", "HS Code"])
        aliases = mv(["Aliases", "Alias", "키워드"])
        country_ok = not m_country or m_country.lower() in text
        candidate_master = any(x in f"{m_product} {m_hs} {mv(['TradeRoute', 'Trade Route', '거래경로'])}".lower() for x in ["후보", "확인 필요", "verify", "candidate"])
        entity_terms = [m_entity] + aliases.split(";")
        entity_ok = any(_term_in_text(text, x) for x in entity_terms if clean(x) and clean(x).lower() not in {"all", "global"})
        product_ok = bool(m_product) and _term_in_text(text, m_product)
        hs_terms = [x.strip() for x in re.split(r"[;,|]", m_hs) if x.strip()]
        hs_ok = bool(hs_value) and any(hs_value == re.sub(r"[^0-9.]", "", x) for x in hs_terms)
        detail_ok = entity_ok or (country_ok and product_ok and hs_ok)
        if country_ok and detail_ok and not candidate_master:
            matched = {"entity": m_entity, "product": m_product, "hs": m_hs, "route": mv(["TradeRoute", "Trade Route", "거래경로"])}
            break

    specific_issue = issue in {"AD_CVD", "HS_CLASSIFICATION", "ORIGIN_FTA", "TARIFF", "EXPORT_CONTROL", "SANCTIONS"}
    mapping_type = "ENTITY_DIRECT" if samsung_named else ("PRODUCT_1TO1" if specific_issue else "POLICY_GENERAL")
    if not verified:
        status = "VERIFICATION_PENDING"
    elif matched and matched.get("entity") and (matched.get("product") or matched.get("hs")):
        status = "ITEM_1TO1_MAPPED"
    elif samsung_named and product:
        status = "ENTITY_CONFIRMED"
    elif mapping_type == "POLICY_GENERAL":
        status = "POLICY_MONITORING"
    else:
        status = "MAPPING_REQUIRED"

    required = []
    if not (matched or samsung_named): required.append("Entity")
    if not (matched and matched.get("product")) and not product: required.append("Product")
    if not (matched and matched.get("hs")) and not hs_value: required.append("HS")
    if not (matched and matched.get("route")) and not trade_route: required.append("TradeRoute")
    return {
        "mapping_type": mapping_type,
        "mapping_status": status,
        "required_mapping_keys": ",".join(required),
        "entity_direct": status in {"ENTITY_CONFIRMED", "ITEM_1TO1_MAPPED"},
        "mapped_entity": (matched or {}).get("entity", "Samsung Electronics" if samsung_named else ""),
        "mapped_product": (matched or {}).get("product", product),
        "mapped_hs": (matched or {}).get("hs", hs_value),
        "trade_route": (matched or {}).get("route", trade_route),
        "mapping_evidence": "MASTER_MATCH" if matched else ("ARTICLE_ENTITY_PRODUCT" if samsung_named and product else ""),
    }


def unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out.loc[:, ~out.columns.duplicated(keep="first")].copy()


def pick_col(df: pd.DataFrame, names: list[str]):
    lookup = {str(c).strip().lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lookup:
            return lookup[n.lower()]
    return None


def normalize_url(v) -> str:
    u = clean(v)
    return u if u.startswith(("http://", "https://")) else ""


def domain(u: str) -> str:
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ""


def load_input() -> pd.DataFrame:
    unreadable = []
    newer = []
    for source_path in REQUIRED_COLLECTOR_FILES:
        if not source_path.exists():
            unreadable.append(f"MISSING:{source_path.name}")
            continue
        try:
            pd.read_excel(source_path, nrows=1)
        except Exception as exc:
            unreadable.append(f"UNREADABLE:{source_path.name}:{type(exc).__name__}")
            continue
        if INPUT_FILE.exists() and source_path.stat().st_mtime > INPUT_FILE.stat().st_mtime + 1:
            newer.append(source_path.name)
    if unreadable or newer:
        detail = "; ".join(unreadable + (["NEWER_THAN_STEP3:" + ",".join(newer)] if newer else []))
        raise RuntimeError(
            "STEP4-2 BLOCKED: STEP3-2 did not produce a trustworthy current-run input. "
            + detail
            + ". Run all three collectors and STEP3-2 successfully. Existing STEP4 outputs are preserved."
        )
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"input not found: {INPUT_FILE}")
    file_age_hours = max(0.0, (datetime.now().timestamp() - INPUT_FILE.stat().st_mtime) / 3600)
    if file_age_hours > INPUT_FILE_MAX_AGE_HOURS:
        raise RuntimeError(
            f"STALE STEP3-2 INPUT: {INPUT_FILE.name} age={file_age_hours:.1f}h "
            f"> {INPUT_FILE_MAX_AGE_HOURS:g}h. STEP3-2 did not complete in the current run. "
            "Run 2-1, 2-2, 2-3 and 3-2 successfully before STEP4-2. "
            "Existing STEP4 outputs were preserved."
        )
    raw = unique_columns(pd.read_excel(INPUT_FILE))

    aliases = {
        "Date": ["Date", "Publish Date", "date", "published", "pubDate"],
        "Headline": ["Headline", "Title", "title"],
        "Summary": ["Summary", "summary", "description"],
        "URL": ["BestURL", "URL", "OriginalURL", "original_url", "url", "link"],
        "Source": ["Source", "source"],
        "Publisher": ["Publisher", "publisher", "Agency", "agency"],
        "Issue": ["IssueKey", "Issue", "issue_type", "topic"],
        "Gate": ["CandidateGate", "Gate", "priority_group", "Tier", "Priority Group"],
        "Score": ["FinalScore", "final_score", "RuleScore", "score"],
        "Cluster": ["EventKey", "Cluster", "cluster_key"],
    }
    out = pd.DataFrame(index=raw.index)
    for target, names in aliases.items():
        c = pick_col(raw, names)
        out[target] = raw[c] if c else ""

    # audit fields if available
    for c in [
        "InputKeyword", "InputFile", "GateReason", "URLStatus", "Country", "Agency",
        "OriginalURLVerified", "URLQuality", "TitleKeywordFlag", "CustomsGateFlag",
        "RegulationRelated", "SourceScoreReason", "ClusterSize", "ClusterSources",
    ]:
        src = pick_col(raw, [c])
        out[c] = raw[src] if src else ""

    out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
    out["Headline"] = out["Headline"].fillna("").astype(str).map(clean)
    out["Summary"] = out["Summary"].fillna("").astype(str).map(clean)
    out["URL"] = out["URL"].fillna("").astype(str).map(normalize_url)
    out["Source"] = out["Source"].fillna("").astype(str).map(clean)
    out["Publisher"] = out["Publisher"].fillna("").astype(str).map(clean)
    out["Issue"] = out["Issue"].fillna("").astype(str).map(lambda x: clean(x).upper())
    out["Gate"] = out["Gate"].fillna("").astype(str).map(lambda x: clean(x).upper())
    out["Score"] = pd.to_numeric(out["Score"], errors="coerce").fillna(0)
    out["Cluster"] = out["Cluster"].fillna("").astype(str).map(clean)
    out = out[out["Headline"].ne("")].copy()
    log(f"LOAD {INPUT_FILE}: {len(out)} rows")
    return out.reset_index(drop=True)


def strict_24h(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    keep, dt, start, end = previous_kst_day_mask(df["Date"])
    stale = df.loc[~keep].copy()
    stale["RejectReason"] = "STRICT_24H_PUBLISHED_DATE"
    fresh = df.loc[keep].copy().reset_index(drop=True)
    log(
        f"KST PRIOR-DAY NEWS GUARD: {len(df)} -> {len(fresh)} / "
        f"removed={len(stale)} / window={start} <= Date < {end}"
    )
    return fresh, stale


def pre_score(row: pd.Series) -> int:
    base = int(float(row.get("Score", 0) or 0))
    issue = clean(row.get("Issue")).upper()
    gate = clean(row.get("Gate")).upper()
    text = f"{clean(row.get('Headline'))} {clean(row.get('Summary'))}".lower()
    s = base + ISSUE_WEIGHT.get(issue, 5)
    if gate == "CORE":
        s += 10
    elif gate == "USABLE":
        s += 5
    if any(x in text for x in SAMSUNG_TERMS):
        s += 10
    d = domain(row.get("URL", ""))
    if any(x in d for x in [".gov", ".go.kr", "europa.eu", "wto.org", "ustr.gov", "cbp.gov", "usitc.gov"]):
        s += 6
    if clean(row.get("OriginalURLVerified")).upper() == "Y":
        s += 7
    if clean(row.get("URLQuality")).upper() == "ORIGINAL_VERIFIED":
        s += 4
    if concrete_policy_delta(text):
        s += 12
    return min(150, s)


def policy_coverage_score(row: pd.Series) -> int:
    """Rank globally material policy instruments before Samsung-keyword bias."""
    text = " ".join([
        clean(row.get("Headline")), clean(row.get("Summary")), clean(row.get("Issue")),
        clean(row.get("Country")), clean(row.get("Agency")),
    ]).lower()
    issue = clean(row.get("Issue")).upper()
    score = 0
    if customs_case_gate(row)[0] == "Y":
        # Operational cases must reach the AI review pool even though they do
        # not contain amendment/effective-date vocabulary.
        score += 70
    if issue in {"AD_CVD", "EXPORT_CONTROL", "ORIGIN_FTA", "HS_CLASSIFICATION", "CBAM_CARBON"}:
        score += 35
    elif issue in {"TARIFF", "CUSTOMS"}:
        score += 25
    if concrete_policy_delta(text):
        score += 30
    if clean(row.get("OriginalURLVerified")).upper() == "Y":
        score += 18
    if domain(row.get("URL", "")).endswith((".gov", ".gov.uk", ".go.kr")) or any(
        x in domain(row.get("URL", "")) for x in ["europa.eu", "wto.org", "ustr.gov", "cbp.gov", "usitc.gov"]
    ):
        score += 18
    if any(x in text for x in [
        "fta", "free trade agreement", "자유무역협정", "public procurement", "공공조달",
        "반덤핑", "anti-dumping", "상계관세", "countervailing", "수출통제", "export control",
        "관세법", "customs law", "조사 개시", "예비판정", "최종판정",
    ]):
        score += 20
    # Sunset/review votes that keep an AD/CVD order in force are a concrete
    # policy delta even when a media headline omits the word "order".
    if re.search(r"\b(?:itc|usitc)\b.*\b(?:votes?|determines?)\b.*\b(?:dut(?:y|ies)|orders?)\b", text):
        score += 35
    if business_scope_noise(row.get("Headline"), row.get("Summary")) or opinion_article(row.get("Headline")):
        score -= 50
    return score


def build_diverse_review_pool(fresh: pd.DataFrame) -> pd.DataFrame:
    """Reserve AI capacity by policy family so global measures are not crowded out."""
    work = fresh.copy()
    work["PolicyCoverageScore"] = work.apply(policy_coverage_score, axis=1)
    selected_indices: list[int] = []

    family_quota = max(4, AI_POLICY_COVERAGE_QUOTA // 7)
    family_order = [
        "AD_CVD", "EXPORT_CONTROL", "ORIGIN_FTA", "CUSTOMS",
        "HS_CLASSIFICATION", "CBAM_CARBON", "TARIFF",
    ]
    for family in family_order:
        candidates = work[work["Issue"].astype(str).str.upper().eq(family)].sort_values(
            ["PolicyCoverageScore", "PreScore", "Date"], ascending=[False, False, False], kind="stable"
        )
        for idx in candidates.head(family_quota).index:
            if idx not in selected_indices:
                selected_indices.append(idx)

    coverage = work.sort_values(
        ["PolicyCoverageScore", "PreScore", "Date"], ascending=[False, False, False], kind="stable"
    )
    for idx in coverage.index:
        if len(selected_indices) >= min(AI_POLICY_COVERAGE_QUOTA, AI_REVIEW_MAX):
            break
        if idx not in selected_indices and work.at[idx, "PolicyCoverageScore"] >= 35:
            selected_indices.append(idx)

    remaining_capacity = max(0, AI_REVIEW_MAX - len(selected_indices))
    remaining = work.loc[~work.index.isin(selected_indices)]
    tariff = remaining[remaining["Issue"].astype(str).str.upper().eq("TARIFF")].head(
        min(AI_TARIFF_QUOTA, remaining_capacity)
    )
    selected_indices.extend(idx for idx in tariff.index if idx not in selected_indices)
    remaining_capacity = max(0, AI_REVIEW_MAX - len(selected_indices))
    for idx in work.loc[~work.index.isin(selected_indices)].sort_values(
        ["PreScore", "Date"], ascending=[False, False], kind="stable"
    ).head(remaining_capacity).index:
        selected_indices.append(idx)

    return work.loc[selected_indices].sort_values(
        ["PolicyCoverageScore", "PreScore", "Date"], ascending=[False, False, False], kind="stable"
    )


def gemini_json(prompt: str) -> dict:
    if not USE_GEMINI:
        return {}
    endpoint = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_MODEL}:generateContent"
    )
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.15,
            "maxOutputTokens": 4096,
            "responseMimeType": "application/json",
        },
    }
    try:
        r = requests.post(
            endpoint,
            headers={"x-goog-api-key": GEMINI_API_KEY},
            json=payload,
            timeout=GEMINI_TIMEOUT,
        )
        if r.status_code >= 400:
            return {"_error": f"HTTP_{r.status_code}: {clean(r.text)[:500]}"}
        data = r.json()
        txt = data["candidates"][0]["content"]["parts"][0]["text"]
        return json.loads(txt)
    except Exception as exc:
        message = clean(exc)
        if GEMINI_API_KEY:
            message = message.replace(GEMINI_API_KEY, "***REDACTED***")
        message = re.sub(r"([?&]key=)[^&\s]+", r"\1***REDACTED***", message, flags=re.I)
        return {"_error": f"{type(exc).__name__}:{message[:180]}"}


def fetch_article_text(url: str) -> tuple[str, str]:
    """Best-effort original body fetch. Short/blocked pages never qualify for Top3."""
    if not url.startswith(("http://", "https://")):
        return "", "NO_URL"
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 Chrome/124 Safari/537.36"}, timeout=12, allow_redirects=True)
        if r.status_code >= 400:
            return "", f"HTTP_{r.status_code}"
        text = r.text
        text = re.sub(r"(?is)<(script|style|nav|footer|header).*?>.*?</\1>", " ", text)
        text = re.sub(r"(?s)<[^>]+>", " ", text)
        text = clean(html_lib.unescape(text))
        if len(text) < 350:
            return text, "BODY_TOO_SHORT"
        return text[:12000], "BODY_OK"
    except Exception as exc:
        return "", f"FETCH_{type(exc).__name__}"


def fallback_relevant(row: pd.Series) -> tuple[bool, int, str]:
    issue = clean(row.get("Issue")).upper()
    gate = clean(row.get("Gate")).upper()
    score = pre_score(row)
    relevant = gate == "CORE" or issue in {
        "TARIFF", "CUSTOMS", "ORIGIN_FTA", "EXPORT_CONTROL", "AD_CVD",
        "HS_CLASSIFICATION", "CBAM"
    }
    return relevant, min(100, max(50 if relevant else 0, score - 35)), "RULE_FALLBACK"


def concrete_customs_signal(text: str) -> bool:
    t = clean(text).lower()
    subject = any(x in t for x in [
        "관세", "관세율", "추가관세", "반덤핑", "상계관세", "세이프가드", "원산지 규정",
        "수입신고", "수출신고", "품목분류", "전략물자", "수출통제", "제재 대상",
        "tariff rate", "additional tariff", "anti-dumping", "countervailing duty",
        "rules of origin", "customs declaration", "hs code", "export control", "sanctions",
        "section 232", "section 301", "cbam", "tariff", "customs", "duty rate",
        "import restriction", "import ban", "quota"
    ])
    action = any(x in t for x in [
        "시행", "발효", "개정", "공포", "고시", "공고", "조사 개시", "예비판정", "최종판정",
        "법원", "판결", "행정명령", "적용", "유예", "철회", "면제", "환급",
        "유지", "연장", "확대", "강화", "완화", "종료", "착수", "발표", "합의", "결렬",
        "effective", "entered into force", "amend", "notice", "investigation", "determination",
        "court", "ruling", "executive order", "exemption", "refund", "maintain", "continued",
        "extend", "expanded", "tighten", "strengthen", "relax", "terminate", "launched", "announced"
    ])
    return subject and action


def analyze_row(row: pd.Series) -> dict:
    title = clean(row.get("Headline"))
    source_summary = clean(row.get("Summary"))
    issue = clean(row.get("Issue"))
    url = clean(row.get("URL"))
    body, body_status = fetch_article_text(url)
    evidence_text = body if len(body) >= 350 else source_summary
    body_verified = body_status == "BODY_OK" and len(body) >= 350
    article_source_type = "ORIGINAL_BODY" if body_verified else "UPSTREAM_SUMMARY"
    article_body_evidence = clean(evidence_text)[:1800]

    if hard_scope_excluded(title) or business_scope_noise(title, source_summary):
        return {
            "relevant": False, "relevance_score": 0, "reason": "HARD_SCOPE_EXCLUDED",
            "analysis_ok": True, "samsung_impact": "None", "top3_eligible": False,
            "body_verified": body_verified, "direct_evidence": [],
            "affected_subsidiary": "", "risk": "하", "summary_ko": source_summary or title,
            "analysis_ko": "관세·통상 보고 범위가 아닌 범죄·기술보호·거시경제 기사로 제외",
            "action_ko": "", "country": "", "agency": clean(row.get("Publisher")) or clean(row.get("Source")),
            "missing_facts": [], "policy_event": False, "official_evidence": "", "issue": "OTHER",
            "article_extract_status": body_status, "article_source_type": article_source_type,
            "article_body_evidence": article_body_evidence, "policy_stage": "OUT_OF_SCOPE",
            "quality_contract": "REJECT_SCOPE_NOISE",
        }

    prompt = f"""
당신은 삼성전자 본사 관세·통상 및 관세컴플라이언스 책임자입니다.
아래 기사 원문을 근거로 먼저 보고 대상 여부를 판정한 뒤 의사결정용 분석을 작성하십시오.

보고 품질 원칙:
- B형 수작업 보고서 수준으로 쓰되 원문에 없는 수치·HS·법인·제품·일정은 절대 만들지 않는다.
- 정책의 '발표/검토/조사개시/예비판정/최종판정/시행' 단계를 구분한다.
- 변경 전→변경 후, 대상국, 대상품목/HS, 세율, 시행일, 법적 근거 중 원문에서 확인된 것만 명시한다.
- 삼성 영향은 반드시 [정책조치 → 대상 품목/HS → 생산·판매 법인 → 수출입 경로 → 관세업무 변화] 순서로 연결한다.
- 연결고리가 하나라도 확인되지 않으면 Direct라고 쓰지 말고 Missing Facts에 남긴다.
- 대응은 단순 '모니터링'이 아니라 확인할 데이터, 산출물, 기한, 담당 Owner를 포함한다.

1단계 관련성 판정:
- YES: 관세율·Section 232/301·AD/CVD·세이프가드·통관·HS·과세가격·FTA/원산지·수출통제·제재·CBAM·수입규제의 구체적 조치가 핵심인 기사.
- NO: 단순 산업동향, 정치 발언, 행사/세미나, 기업실적, 주가, 일반 공급망, 관세 단어가 부수적으로만 등장하는 기사.
- 주요 글로벌 관세정책은 삼성 직접영향이 없어도 YES/Watch 가능.
- relevance_score는 반드시 0~10 정수: 0~2 제외, 3~5 검증된 정책 Watch, 6~7 삼성 간접영향, 8~10 삼성 직접영향 후보.
- relevant=true이면 relevance_score는 최소 3이어야 하며, relevant=false이면 0~2여야 한다.

2단계 분석 순서(반드시 준수):
- [사실관계] 발표 주체·국가, 조치 단계, 발표/시행일, 대상 품목·HS, 세율/쿼터, 원산지·신고·증빙 요건.
- [삼성전자 관세업무 직접영향] 영향 법인·제품·거래흐름과 수입/수출통관, HS, 과세가격, FTA/원산지, 관세비용, 조사대응 중 바뀌는 업무.
- [대응] 즉시(오늘~3영업일), 1개월 내, 상시 모니터링, Owner.

Direct/Top3 조건:
- Direct는 원문 검증, 구체적 공식조치, 삼성 법인·공장·거래 직접 언급 또는 완료된 제품·HS·거래경로 1:1 매핑을 모두 요구한다.
- 삼성전자 명칭이 없으면 원칙적으로 Indirect/Watch. 환적·원산지 사건도 삼성 거래 1:1 매핑 전에는 Direct 금지.
- AD/CVD·품목분류·원산지 등 특정 품목 사건은 법인·제품·HS·거래경로 매핑 완료 전 Top3 금지.
- 생산국·제품명·관세 단어가 각각 등장하는 것만으로 연결관계를 추정하지 말 것.
- 국가명, 삼성/반도체 단어, 일반 공급망 언급만으로 Direct 금지.
- 원문 본문을 확보하지 못했으면 body_verified=false, Direct 금지, Top3 Eligible=false.
- 사실이 불명확하면 추정하지 말고 missing_facts에 기록.

JSON만 출력:
{{
 "relevant": true,
 "relevance_score": 7,
 "reason": "YES/NO의 원문 근거",
 "samsung_impact": "Direct|Indirect|Watch|None",
 "top3_eligible": false,
 "body_verified": {str(body_verified).lower()},
 "direct_evidence": ["Direct 판정 원문 근거"],
 "affected_subsidiary": "영향 법인/지역 또는 관련 법인 검토",
 "risk": "상|중|하",
 "summary_ko": "[확인 사실] 조치단계·발표/시행일·대상국·대상품목/HS·세율·법적근거를 원문 범위에서 3~5문장. [미확인] 핵심 공백을 1문장",
 "analysis_ko": "[연결 경로] 정책조치 → 품목/HS → 법인 → 거래경로 → 바뀌는 관세업무. [비용/리스크] 계산식 또는 필요한 입력데이터. 연결 미확인 시 직접영향 미확정이라고 명시",
 "action_ko": "[즉시/3영업일] 확인 데이터와 산출물 | [1개월 내] 시스템·SOP·계약 반영 산출물 | [상시/Trigger] 후속 판정·시행 등 재보고 조건 | [Owner] HQ Customs + 해당 사업부/법인",
 "country": "발표국/영향국",
 "agency": "발표기관/매체",
 "issue": "TARIFF|AD_CVD|EXPORT_CONTROL|SANCTIONS|CUSTOMS|HS_CLASSIFICATION|ORIGIN_FTA|CBAM_CARBON|OTHER",
 "policy_event": true,
 "official_evidence": "정부·세관·법원·공식문서와 구체적 조치 근거",
 "missing_facts": ["확인되지 않은 HS·세율·시행일 등"]
}}

Issue: {issue}
Headline: {title}
URL: {url}
Body status: {body_status}
원문/본문:
{evidence_text[:12000]}
""".strip()

    result = gemini_json(prompt)
    if not result or result.get("_error"):
        return {
            "relevant": False,
            "relevance_score": 0,
            "reason": "AI_ERROR_FAIL_CLOSED" + (f"; {result.get('_error')}" if result else ""),
            "analysis_ok": False,
            "samsung_impact": "None",
            "top3_eligible": False,
            "body_verified": body_verified,
            "direct_evidence": [],
            "affected_subsidiary": "관련 법인 검토",
            "risk": "하",
            "summary_ko": source_summary or title,
            "analysis_ko": "",
            "action_ko": "",
            "country": "",
            "agency": clean(row.get("Publisher")) or clean(row.get("Source")),
            "missing_facts": ["Gemini 분석 실패 또는 원문 근거 부족"],
            "policy_event": False,
            "official_evidence": "",
            "article_extract_status": body_status,
            "article_source_type": article_source_type,
            "article_body_evidence": article_body_evidence,
            "policy_stage": "AI_ERROR",
            "quality_contract": "REJECT_AI_ERROR",
        }

    impact = clean(result.get("samsung_impact"))
    if impact not in {"Direct", "Indirect", "Watch", "None"}:
        impact = "Watch" if as_bool(result.get("relevant")) else "None"
    verified = as_bool(result.get("body_verified")) and body_verified
    evidence = [clean(x) for x in result.get("direct_evidence", []) if clean(x)]
    issue_out = clean(result.get("issue")).upper()
    allowed_issues = {
        "TARIFF", "AD_CVD", "EXPORT_CONTROL", "SANCTIONS",
        "CUSTOMS", "HS_CLASSIFICATION", "ORIGIN_FTA",
        "CBAM_CARBON",
    }
    if issue_out not in allowed_issues:
        issue_out = "OTHER"

    # Gemini가 확인한 정책사건을 원문 한글 표현 사전 하나만으로 다시 0으로
    # 만들지 않는다. 원문 + AI 확인사실 + 공식근거 전체에서 구체 조치를
    # 재검증한다. Direct/Top3의 본문·매핑 증빙 기준은 별도로 그대로 유지한다.
    official_evidence = clean(result.get("official_evidence"))
    measure_text = " ".join([
        evidence_text,
        clean(result.get("summary_ko")),
        official_evidence,
        title,
    ])
    # 정책게이트는 AI가 생성한 요약문이 아니라 원문/상류요약과 공식근거만 사용한다.
    # AI 문장에 생긴 관세 단어가 스스로 통과근거가 되는 순환판정을 차단한다.
    source_measure_text = " ".join([evidence_text, official_evidence, title])
    policy_event = (
        as_bool(result.get("policy_event"))
        and issue_out in allowed_issues
        and concrete_customs_signal(source_measure_text)
        and issue_specific_policy_signal(issue_out, source_measure_text)
        and not business_scope_noise(title, source_measure_text)
    )

    # 발표국을 AI가 생략해도 STEP3에서 정규화한 국가를 보존한다.
    country = clean(result.get("country")) or clean(row.get("Country"))

    if event_only_noise(title, evidence_text):
        policy_event = False
        impact = "None"
        result["relevance_score"] = 0
        result["reason"] = "EVENT_ONLY_NO_CONCRETE_CUSTOMS_CHANGE"

    # HQ customs decision rule (fail closed): do not promote an article merely
    # because product/country/customs words occur somewhere in the body.
    # Direct requires either explicit Samsung evidence, or the narrowly defined
    # Korea-semiconductor transshipment/origin route requested by the business.
    route_text = f"{title} {evidence_text} {country}".lower()
    product_terms = [
        "반도체", "semiconductor", "memory chip", "메모리칩", "스마트폰",
        "smartphone", "display", "디스플레이", "television", "tv", "가전",
        "appliance", "network equipment", "배터리", "battery",
    ]
    route_customs_terms = [
        "관세", "tariff", "환적", "transshipment", "원산지", "origin",
        "section 232", "section 301", "반덤핑", "anti-dumping", "통관",
        "customs", "수출통제", "export control", "제재", "sanction", "fta",
    ]
    samsung_named, samsung_direct_sentence = _explicit_samsung_customs_context(title, evidence_text)
    mapping = map_article_to_business(title, evidence_text, country, issue_out, verified)
    product_specific_issue = issue_out in {"AD_CVD", "HS_CLASSIFICATION", "ORIGIN_FTA"}
    mapping_ok = (
        mapping["mapping_status"] == "ITEM_1TO1_MAPPED"
        or (
            mapping["mapping_status"] == "ENTITY_CONFIRMED"
            and samsung_named
            and not product_specific_issue
        )
    )
    direct_evidence_mentions_samsung = any(
        re.search(r"삼성전자|samsung electronics|samsung", item, re.I) for item in evidence
    )
    direct_stage_ok = confirmed_direct_stage(source_measure_text)
    primary_evidence_ok = official_primary_evidence(official_evidence)
    explicit_samsung_direct = (
        verified and policy_event and mapping_ok and samsung_named
        and any(term in route_text for term in product_terms)
        and any(term in route_text for term in route_customs_terms)
        and primary_evidence_ok and direct_evidence_mentions_samsung
        and direct_stage_ok
    )
    korea_semicon_transshipment_direct = (
        verified
        and policy_event
        and any(term in route_text for term in ["환적", "transshipment", "원산지 세탁", "origin laundering", "관세 회피"])
        and any(term in route_text for term in ["한국", "korea", "경기", "반도체벨트", "semiconductor belt"])
        and any(term in route_text for term in ["반도체", "semiconductor", "854239", "8542.39"])
        and any(term in route_text for term in ["중국", "china", "중국산"])
        and mapping["mapping_status"] == "ITEM_1TO1_MAPPED"
        and primary_evidence_ok
        and direct_stage_ok
    )
    route_direct = explicit_samsung_direct or korea_semicon_transshipment_direct

    # AI prose that explicitly denies direct impact takes precedence over a
    # broad categorical label. It may still be retained as Indirect/Watch.
    analysis_lower = clean(result.get("analysis_ko")).lower()
    denies_direct = any(term in analysis_lower for term in [
        "직접적인 영향은 없", "직접 영향은 없", "직접적인 관련성은 없",
        "직접 관련성은 없", "직접적인 관세 업무 영향은 없", "미치는 영향은 없",
        "no direct impact", "not directly related",
    ])
    if denies_direct and not korea_semicon_transshipment_direct:
        if impact == "Direct":
            impact = "Watch"
        route_direct = False
    if route_direct:
        impact = "Direct"
        try:
            result["relevance_score"] = max(8, int(float(result.get("relevance_score", 0) or 0)))
        except Exception:
            result["relevance_score"] = 8
        if not evidence:
            impact = "Indirect"
            route_direct = False

    # Enforce the same evidence gate on Gemini's original Direct label. A
    # concrete global measure without a proven Samsung route is Indirect; an
    # article explicitly denying impact is Watch.
    if impact == "Direct" and not route_direct:
        impact = "Watch" if denies_direct else "Indirect"

    # 사설·칼럼은 공식 조치의 보조 해설로만 사용하며 Direct/Top3로 올리지 않는다.
    if opinion_article(title):
        impact = "Watch" if as_bool(result.get("relevant")) else "None"
        route_direct = False
        result["top3_eligible"] = False

    third_party_case = (
        not samsung_named
        and any(term in route_text for term in ["한국타이어", "hankook tire", "타이어", "tire"])
        and any(term in route_text for term in ["불복", "소송", "심판청구", "appeal", "lawsuit", "court challenge"])
    )
    if third_party_case and impact in {"Direct", "Indirect"}:
        impact = "Watch"
        route_direct = False

    try:
        relevance_score = int(float(result.get("relevance_score", 0) or 0))
    except Exception:
        relevance_score = 0
    relevant = (as_bool(result.get("relevant")) or route_direct) and verified and policy_event and bool(country)
    # Gemini의 relevant=Y/score=0 모순은 그대로 저장하지 않는다. 이슈별 구체
    # 정책신호를 재검증한 경우에만 최소 보고점수로 보정하고, 그렇지 않으면 제외한다.
    if relevant and relevance_score < WATCH_MIN_RELEVANCE:
        if issue_specific_policy_signal(issue_out, source_measure_text) and not business_scope_noise(title, source_measure_text):
            relevance_score = WATCH_MIN_RELEVANCE
            result["reason"] = (clean(result.get("reason")) + "; SCORE_CONSISTENCY_RECOVERED").strip("; ")
        else:
            relevant = False
            result["reason"] = "RELEVANCE_SCORE_CONTRADICTION"
    result["relevance_score"] = relevance_score
    top3 = (
        (as_bool(result.get("top3_eligible")) or route_direct)
        and verified and impact == "Direct" and len(evidence) >= 1
        and mapping_ok and primary_evidence_ok and direct_stage_ok
    )
    if not relevant:
        impact = "None"
        top3 = False
    if not verified and impact == "Direct":
        impact = "Watch"
    result.update({
        "samsung_impact": impact,
        "top3_eligible": top3,
        "body_verified": verified,
        "direct_evidence": evidence,
        "missing_facts": [clean(x) for x in result.get("missing_facts", []) if clean(x)],
        "relevant": relevant,
        "analysis_ok": True,
        "policy_event": policy_event,
        "country": country,
        "issue": issue_out,
        "mapping_type": mapping["mapping_type"],
        "mapping_status": mapping["mapping_status"],
        "required_mapping_keys": mapping["required_mapping_keys"],
        "entity_direct": mapping["entity_direct"],
        "mapped_entity": mapping["mapped_entity"],
        "mapped_product": mapping["mapped_product"],
        "mapped_hs": mapping["mapped_hs"],
        "trade_route": mapping["trade_route"],
        "mapping_evidence": mapping["mapping_evidence"],
        "article_extract_status": body_status,
        "article_source_type": article_source_type,
        "article_body_evidence": article_body_evidence,
        "policy_stage": "OPERATIVE" if direct_stage_ok else "PROPOSED_OR_MONITORING",
        "quality_contract": "STRICT_PASS" if relevant else "REJECT_QUALITY_CONTRACT",
    })
    return result


def build() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = load_input()
    fresh, stale = strict_24h(df)
    if fresh.empty:
        source_dates = pd.to_datetime(df.get("Date"), errors="coerce")
        newest = source_dates.max()
        newest_text = "unknown" if pd.isna(newest) else str(newest)
        raise RuntimeError(
            f"NO FRESH NEWS AFTER 24H GUARD: input={len(df)} / fresh=0 / "
            f"newest_publish_date={newest_text}. "
            "The current 3-2.news_summary.xlsx contains no reportable article from the last 24 hours. "
            "Run 2-1, 2-2, 2-3 and 3-2 again. Existing STEP4 outputs were preserved."
        )
    fresh["PreScore"] = fresh.apply(pre_score, axis=1)
    fresh = fresh.sort_values(["PreScore", "Date"], ascending=[False, False], kind="stable").reset_index(drop=True)

    # AI 비용을 쓰기 전에 동일 사건을 대표기사 하나로 압축한다.
    fresh, pre_ai_duplicates = pre_ai_event_dedup(fresh)

    # 글로벌 공식정책과 7개 정책축을 먼저 확보하고 남은 용량을 점수순으로 채운다.
    review = build_diverse_review_pool(fresh)
    tail = fresh.loc[~fresh.index.isin(review.index)].copy()
    review_issue = review.get("Issue", pd.Series(index=review.index, dtype=str)).fillna("").astype(str).str.upper()
    coverage_n = int(pd.to_numeric(review.get("PolicyCoverageScore", 0), errors="coerce").fillna(0).ge(35).sum())
    log(f"AI REVIEW POLICY COVERAGE: total={len(review)} / coverage={coverage_n} / tariff={int(review_issue.eq('TARIFF').sum())} / other={int((~review_issue.eq('TARIFF')).sum())}")
    if not tail.empty:
        tail["RejectReason"] = "OUTSIDE_AI_REVIEW_POOL"

    audited = []
    for i, (_, row) in enumerate(review.iterrows(), 1):
        a = analyze_row(row)
        if i == 1 and "AI_ERROR_FAIL_CLOSED" in clean(a.get("reason")):
            raise RuntimeError(
                "Gemini first request failed: " + clean(a.get("reason")))
        r = row.to_dict()
        r["AIRelevant"] = "Y" if as_bool(a.get("relevant")) else "N"
        try:
            r["AIRelevanceScore"] = int(float(a.get("relevance_score", 0) or 0))
        except Exception:
            r["AIRelevanceScore"] = 0
        r["AIReason"] = clean(a.get("reason"))
        r["Samsung Impact"] = clean(a.get("samsung_impact")) or ("Watch" if r["AIRelevant"] == "Y" else "None")
        r["Top3 Eligible"] = "Y" if as_bool(a.get("top3_eligible")) else "N"
        r["Body Verified"] = "Y" if as_bool(a.get("body_verified")) else "N"
        r["Direct Evidence"] = " | ".join(clean(x) for x in a.get("direct_evidence", []) if clean(x))
        r["Missing Facts"] = " | ".join(clean(x) for x in a.get("missing_facts", []) if clean(x))
        r["Policy Event"] = "Y" if as_bool(a.get("policy_event")) else "N"
        r["Official Evidence"] = clean(a.get("official_evidence"))
        r["Analysis OK"] = "Y" if as_bool(a.get("analysis_ok")) else "N"
        r["Affected Subsidiary"] = clean(a.get("affected_subsidiary")) or "관련 법인 검토"
        r["Risk"] = clean(a.get("risk")) or "중"
        r["SummaryAI"] = clean(a.get("summary_ko")) or clean(row.get("Summary"))
        r["AnalysisAI"] = clean(a.get("analysis_ko"))
        r["ActionAI"] = clean(a.get("action_ko"))
        r["Country"] = clean(a.get("country"))
        r["Issue"] = clean(a.get("issue")) or "OTHER"
        r["Agency"] = clean(a.get("agency")) or clean(row.get("Publisher"))
        r["RegulationMappingType"] = clean(a.get("mapping_type"))
        r["MappingStatus"] = clean(a.get("mapping_status")) or "VERIFICATION_PENDING"
        r["RequiredMappingKeys"] = clean(a.get("required_mapping_keys"))
        r["EntityDirectFlag"] = "Y" if as_bool(a.get("entity_direct")) else "N"
        r["MappedEntity"] = clean(a.get("mapped_entity"))
        r["MappedProduct"] = clean(a.get("mapped_product"))
        r["MappedHS"] = clean(a.get("mapped_hs"))
        r["TradeRoute"] = clean(a.get("trade_route"))
        r["MappingEvidence"] = clean(a.get("mapping_evidence"))
        r["Article Extract Status"] = clean(a.get("article_extract_status"))
        r["Article Source Type"] = clean(a.get("article_source_type"))
        r["Article Body Evidence"] = clean(a.get("article_body_evidence"))
        r["Policy Stage"] = clean(a.get("policy_stage"))
        r["Quality Contract"] = clean(a.get("quality_contract"))
        audited.append(r)
        if i % 10 == 0 or i == len(review):
            log(f"AI REVIEW {i}/{len(review)}")

    audit = pd.DataFrame(audited)
    if audit.empty:
        audit = fresh.iloc[0:0].copy()
        audit["AIRelevant"] = ""
        audit["AIRelevanceScore"] = 0
        audit["_EventOnly"] = False

    if not audit.empty:
        audit["SelectionScore"] = (
            pd.to_numeric(audit["PreScore"], errors="coerce").fillna(0) * 0.45
            + pd.to_numeric(audit["AIRelevanceScore"], errors="coerce").fillna(0) * 0.55
        ).round(1)
        audit["_EventOnly"] = audit.apply(
            lambda r: event_only_noise(r.get("Headline"), r.get("SummaryAI")), axis=1
        )

        # 선택 결과가 갑자기 0건이 될 때 어느 증빙 게이트가 병목인지
        # 실행 로그만으로 즉시 확인할 수 있게 한다.
        log(
            "SELECTION GATE DIAGNOSTICS: "
            f"ai_relevant={int(audit['AIRelevant'].eq('Y').sum())} / "
            f"body_verified={int(audit['Body Verified'].eq('Y').sum())} / "
            f"policy_event={int(audit['Policy Event'].eq('Y').sum())} / "
            f"non_event_noise={int((~audit['_EventOnly'].fillna(False)).sum())} / "
            f"valid_issue={int(audit['Issue'].ne('OTHER').sum())}"
        )

    ai_failures = int((audit.get("Analysis OK", pd.Series(index=audit.index, dtype=str)) != "Y").sum())
    if len(audit) and ai_failures / len(audit) >= 0.20:
        raise RuntimeError(
            f"Gemini analysis failed for {ai_failures}/{len(audit)} rows. "
            "Fail-closed: existing summary/mail inputs were not overwritten. Check API key, quota and model access."
        )

    # Final selection is fail-closed. STEP4's article-native verification gates
    # and the shared deterministic contract must both pass. A row rejected as
    # non-policy or non-relevant cannot be resurrected by a looser keyword hit.
    selected, deterministic_rejected = apply_quality_contract(audit, include_reference=True)
    selected = selected[
        selected["Body Verified"].eq("Y")
        & ~selected["_EventOnly"].fillna(False)
    ].copy()
    # The shared policy contract may reject a case solely because it is not a
    # policy amendment. Rescue only body-verified, Samsung-scope customs cases;
    # this is not a generic keyword-based forced fill.
    case_mask = audit.apply(lambda r: customs_case_gate(r)[0] == "Y", axis=1)
    protected_cases = audit[
        case_mask
        & audit["Body Verified"].eq("Y")
        & ~audit["_EventOnly"].fillna(False)
    ].copy()
    protected_cases["_supplemental_watch"] = True
    if not protected_cases.empty:
        existing_keys = set(
            (selected.get("URL", pd.Series(dtype=str)).fillna("").astype(str) + "|" +
             selected.get("Headline", pd.Series(dtype=str)).fillna("").astype(str)).tolist()
        )
        protected_cases = protected_cases[
            ~(
                protected_cases.get("URL", pd.Series("", index=protected_cases.index)).fillna("").astype(str)
                + "|"
                + protected_cases.get("Headline", pd.Series("", index=protected_cases.index)).fillna("").astype(str)
            ).isin(existing_keys)
        ]
        selected = pd.concat([selected, protected_cases], axis=0, sort=False)
        log(f"CUSTOMS CASE RESCUE: {len(protected_cases)}")
    selected = selected.sort_values(
        ["ExecutiveScore", "SelectionScore", "PreScore", "Date"],
        ascending=[False, False, False, False], kind="stable"
    )
    if "_supplemental_watch" not in selected:
        selected["_supplemental_watch"] = False
    selected["_supplemental_watch"] = selected["_supplemental_watch"].fillna(False)
    if not selected.empty:
        delta_values = selected.apply(policy_delta_gate, axis=1)
        selected["PolicyDeltaFlag"] = [x[0] for x in delta_values]
        selected["PolicyDeltaType"] = [x[1] for x in delta_values]
        selected["PolicyDeltaReason"] = [x[2] for x in delta_values]
        case_values = selected.apply(customs_case_gate, axis=1)
        selected["CustomsCaseFlag"] = [x[0] for x in case_values]
        selected["CustomsCaseType"] = [x[1] for x in case_values]
        selected["CustomsCaseReason"] = [x[2] for x in case_values]
        selected["DirectCustomsCaseFlag"] = selected["CustomsCaseType"].astype(str).str.startswith("DIRECT_").map({True: "Y", False: "N"})
        selected["PeerPrecedentFlag"] = selected["CustomsCaseType"].astype(str).str.startswith("PEER_").map({True: "Y", False: "N"})
        official_values = selected.apply(official_source_fields, axis=1)
        selected["OfficialSourceURL"] = [x[0] for x in official_values]
        selected["OfficialSourceStatus"] = [x[1] for x in official_values]
        selected["OfficialSourceCandidate"] = [x[2] for x in official_values]
        selected["EvidenceGateFlag"] = selected.apply(
            lambda r: "Y" if (
                clean(r.get("OfficialSourceStatus")) == "VERIFIED_EXACT"
                or official_primary_evidence(r.get("Official Evidence"))
            ) else "N", axis=1,
        )
        selected["SamsungTradeGateFlag"] = selected.apply(
            lambda r: "Y" if (
                clean(r.get("EntityDirectFlag")).upper() == "Y"
                and bool(clean(r.get("MappedEntity")))
                and bool(clean(r.get("MappedHS")) or clean(r.get("TradeRoute")))
            ) else "N", axis=1,
        )
        # A policy delta also includes a new customs operating instrument.
        # A KCS HS cross-reference changes classification/mapping work even
        # when the underlying U.S. tariff was announced earlier. FTA guidance
        # is attached to the effective FTA event when entry into force is
        # confirmed in the article body.
        for idx, r in selected.iterrows():
            body = " ".join([
                clean(r.get("Headline")), clean(r.get("SummaryAI")),
                clean(r.get("AnalysisAI")), clean(r.get("Article Body Evidence")),
            ]).lower()
            kcs_hs_map = (
                any(x in body for x in ["품목번호 연계표", "hs code cross-reference", "한-미 품목번호"])
                and any(x in body for x in ["관세청", "korea customs service"])
                and any(x in body for x in ["미 추가관세", "미국 관세", "new us tariff", "u.s. tariff"])
            )
            mercosur_sg_fta = (
                "메르코수르" in body and "싱가포르" in body and "fta" in body
                and any(x in body for x in ["발효", "효력", "entered into force", "effective"])
            )
            if kcs_hs_map:
                selected.at[idx, "PolicyDeltaFlag"] = "Y"
                selected.at[idx, "PolicyDeltaType"] = "CUSTOMS_OPERATION_DELTA"
                selected.at[idx, "PolicyDeltaReason"] = "관세청의 미국 추가관세 대응 한·미 품목번호 연계표 신규 공개"
                selected.at[idx, "Issue"] = "HS_CLASSIFICATION"
            elif mercosur_sg_fta:
                selected.at[idx, "PolicyDeltaFlag"] = "Y"
                selected.at[idx, "PolicyDeltaType"] = "FTA_EFFECTIVE_DELTA"
                selected.at[idx, "PolicyDeltaReason"] = "메르코수르-싱가포르 FTA 발효 및 활용조건 확인 필요"
                selected.at[idx, "Issue"] = "ORIGIN_FTA"
        selected["ThreeGateStatus"] = selected.apply(
            lambda r: (
                "G1_POLICY_DELTA_FAIL" if clean(r.get("PolicyDeltaFlag")) != "Y"
                else "G2_OFFICIAL_EVIDENCE_PENDING" if clean(r.get("EvidenceGateFlag")) != "Y"
                else "PASS_ALL_GATES" if clean(r.get("SamsungTradeGateFlag")) == "Y"
                else "G3_SAMSUNG_MAPPING_PENDING"
            ), axis=1,
        )
        selected["ReportLayer"] = "POLICY_RADAR"
        # Policy Radar requires a current policy delta and named/linked official
        # evidence. Samsung mapping controls actionability, not global sensing.
        context_mask = selected["PolicyDeltaFlag"].ne("Y") | selected["EvidenceGateFlag"].ne("Y")
        selected.loc[context_mask, "ReportLayer"] = "REFERENCE"
        # Non-delta customs cases are intentionally Watch candidates, not
        # generic Global Context and never fabricated into policy changes.
        protected_mask = selected["CustomsCaseFlag"].eq("Y")
        selected.loc[protected_mask, "ReportLayer"] = "SAMSUNG_WATCH"
        selected.loc[protected_mask, "PolicyDeltaType"] = "CUSTOMS_IMPACT_CASE"
        selected.loc[protected_mask, "PolicyDeltaReason"] = "정책 개정 외 관세 실무 사례로 별도 보존"
        selected.loc[context_mask, "ExecutiveTier"] = "REFERENCE"
        selected.loc[context_mask, "Top3 Eligible"] = "N"
        selected.loc[context_mask, "Samsung Impact"] = "Watch"
        log(
            "POLICY DELTA GATE: "
            f"policy={int((~context_mask).sum())} / context={int(context_mask.sum())} / "
            f"official_exact={int(selected['OfficialSourceStatus'].eq('VERIFIED_EXACT').sum())} / "
            f"samsung_mapped={int(selected['SamsungTradeGateFlag'].eq('Y').sum())}"
        )
    # REPORT_TARGET은 최대 표시 건수다. 품질 미달 행으로 30건을 강제 충원하지 않는다.
    log(
        f"GOLD QUALITY CONTRACT {CONTRACT_VERSION}: strict_pass={len(selected)} / target_cap={REPORT_TARGET} / "
        f"shortfall={max(0, REPORT_TARGET-len(selected)) if REPORT_TARGET > 0 else 0} / forced_fill=0"
    )

    def semantic_event_key(row: pd.Series) -> str:
        title_text = clean(row.get("Headline")).lower()
        upstream_cluster = clean(row.get("Cluster")).lower()
        if upstream_cluster.startswith("event|"):
            return upstream_cluster
        text = " ".join([
            clean(row.get("Headline")), clean(row.get("SummaryAI")),
            clean(row.get("Country")), clean(row.get("Issue")),
        ]).lower()
        if (
            any(x in text for x in ["미국", "트럼프", "백악관", "washington", "u.s."])
            and any(x in title_text for x in ["삼성전자", "삼전", "samsung", "sk하이닉스", "하이닉스", "반도체", "semiconductor", "chip"])
            and any(x in text for x in ["관세", "tariff", "투자 압박", "투자 이행", "미국 투자", "유치 압박", "양자택일", "편 서지 마", "pressure"])
        ):
            return "US_SEMICON_TARIFF_INVESTMENT_PRESSURE"
        if (
            any(x in text for x in ["미국", "트럼프", "u.s."])
            and any(x in title_text for x in ["드론", "drone", "무인기", "uas"])
            and any(x in text for x in ["100%", "100％", "최대 100", "section 232", "232조"])
            and any(x in text for x in ["관세", "tariff"])
        ):
            return "US_DRONE_232_TARIFF"
        if (
            any(x in text for x in ["품목번호 연계표", "hs code cross-reference", "한-미 품목번호"])
            and any(x in text for x in ["관세청", "korea customs service"])
        ):
            return "KR_US_ADDITIONAL_TARIFF_HS_CROSS_REFERENCE"
        if "메르코수르" in text and "싱가포르" in text and "fta" in text:
            return "MERCOSUR_SINGAPORE_FTA_EFFECTIVE"
        rules = [
            ("CN_JP_DICHLOROSILANE_AD_2026", [["디클로로실란", "반도체 가스", "반도체 핵심소재"], ["반덤핑", "보증금", "99.2%", "99.2％"], ["중국", "china"]]),
            ("VN_IN_CERAMIC_TILE_AD_2026", [["세라믹 타일", "ceramic tile"], ["반덤핑", "anti-dumping"], ["인도", "india"]]),
            ("KR_CN_HGI_GI_AD_GAP_2026", [["hgi", "gi", "용융아연도금"], ["반덤핑", "관세"], ["중국", "china"]]),
            ("KR_STRATEGIC_EXPORT_CONTROL_AI_CHIP", [["전략물자수출입고시", "전략물자 수출입고시"], ["ai 칩", "ai용 집적회로", "반도체 장비", "수출통제"]]),
            ("KR_CHINA_BUTYL_ACRYLATE_AD", [["아크릴산 부틸", "butyl acrylate"], ["덤핑관세", "덤핑 관세", "반덤핑", "anti-dumping", "anti dumping", "duties"], ["중국", "china", "chinese"]]),
            ("US_KR_COUPANG_SECTION301_TARIFF", [["쿠팡", "coupang"], ["301조", "section 301", "추가 관세", "관세 보복", "retaliatory tariff"]]),
            ("KR_HOLIDAY_ORIGIN_MARKING_ENFORCEMENT", [["추석", "명절"], ["원산지표시", "국산 둔갑", "원산지 표시"], ["단속", "관세청"]]),
            ("G20_TRADE_IMBALANCE_CHINA", [["g20"], ["무역 불균형", "trade imbalance"], ["중국", "china"]]),
            ("EU_LOW_VALUE_DEMINIMIS_2026", [["eu", "유럽연합"], ["저가", "low-value", "de minimis", "150유로"], ["3유로", "€3", "면세 폐지", "tariff", "관세"]]),
            ("US_CANADA_50PCT_RETALIATORY_TARIFFS", [["미국", "美", "미,", "미-", "미·", "미 캐나다", "미 관세", "usa", "us ", "u.s.", "united states"], ["캐나다", "canada"], ["관세", "tariff"]]),
            ("KR_FLOOD_CUSTOMS_RELIEF", [["호우", "침수", "수해", "flood"], ["관세청", "세관", "customs"], ["납부기한", "관세조사", "원산지검증", "신속통관", "통관 지원", "지원책"]]),
            ("US_SECTION232_DRONE_COMPONENTS_TARIFF", [["드론", "drone", "무인기", "uas"], ["section 232", "무역확장법 232", "232조"], ["관세", "tariff"]]),
            ("US_CHINA_TRANSSHIPMENT_KOREA_SEMICON", [["환적", "transshipment", "원산지 세탁", "관세 회피"], ["한국", "korea", "경기", "반도체벨트"], ["중국", "china", "중국산"]]),
            ("US_CHINA_AUTO_TARIFF_BLOCK", [["중국차", "중국산 자동차", "chinese car", "chinese vehicle"], ["127.5%", "딜러", "dealer", "봉쇄", "진입 반대"]]),
            ("KR_STEEL_TRADE_REMEDY", [["철강", "steel"], ["반덤핑", "anti-dumping", "232", "쿼터"]]),
            ("BUSAN_EXPORT_ECONOMY", [["부산"], ["제조업", "실물경제", "수출 중심"]]),
            ("EU_CHINA_AUTO_SUPPLY_CHAIN", [["유럽", "europe", "eu"], ["중국차", "chinese car", "부품망", "공급망"]]),
        ]
        for name, groups in rules:
            if all(any(term in text for term in group) for group in groups):
                return name
        words = re.findall(r"[a-z0-9가-힣]+", clean(row.get("Headline")).lower())
        stop = {"속보", "종합", "단독", "오늘", "뉴스", "the", "a", "an", "and", "of", "to", "in"}
        return clean(row.get("Issue")) + "|" + " ".join(w for w in words if w not in stop)[:90]

    selected["SemanticEventKey"] = selected.apply(semantic_event_key, axis=1)
    selected = selected.drop_duplicates("SemanticEventKey", keep="first")
    cap = TARGET_MAX if TARGET_MAX > 0 else REPORT_TARGET
    if cap > 0:
        # Reserve the cap for qualified customs cases first. Otherwise a
        # non-policy case appended by the rescue path can be truncated merely
        # because 30 policy rows were already ahead of it.
        protected_cap = selected[
            selected.get("CustomsCaseFlag", pd.Series("N", index=selected.index)).eq("Y")
        ].copy()
        regular_cap = selected[
            ~selected.index.isin(protected_cap.index)
        ].head(max(0, cap - len(protected_cap)))
        selected = pd.concat([protected_cap.head(cap), regular_cap], axis=0, sort=False)
    selected_audit_indices = set(selected.index)
    selected = selected.reset_index(drop=True)

    selected_rows = []
    for i, r in selected.iterrows():
        supplemental_watch = bool(r.get("_supplemental_watch", False))
        impact = "Watch" if supplemental_watch else clean(r.get("Samsung Impact"))
        risk_map = {"High": "상", "Medium": "중", "Low": "하", "HIGH": "상", "MEDIUM": "중", "LOW": "하"}
        normalized_risk = risk_map.get(clean(r.get("Risk")), clean(r.get("Risk")) or "중")
        mail_group = "News - 핵심" if impact == "Direct" else "News - 주요/참고"
        selected_rows.append({
            "No": i + 1,
            "Content Type": "News",
            "Mail Group": mail_group,
            "Samsung Impact": impact or "Watch",
            "Affected Subsidiary": clean(r.get("Affected Subsidiary")) or "관련 법인 검토",
            "Impact Reason": clean(r.get("AIReason")),
            "Date": pd.to_datetime(r.get("Date"), errors="coerce"),
            "Publish Date": pd.to_datetime(r.get("Date"), errors="coerce"),
            "Headline": clean(r.get("Headline")),
            "Summary": clean(r.get("SummaryAI")),
            "AI Analysis": clean(r.get("AnalysisAI")),
            "Action Plan": clean(r.get("ActionAI")),
            "Country": clean(r.get("Country")),
            "Agency": clean(r.get("Agency")),
            "Risk": normalized_risk,
            "Importance Score": int(round(float(r.get("SelectionScore", 0) or 0))),
            "Priority Group": "CORE" if mail_group == "News - 핵심" else "USABLE",
            "Issue": clean(r.get("Issue")),
            "Cluster": clean(r.get("SemanticEventKey")) or clean(r.get("Cluster")),
            "URL": clean(r.get("URL")),
            "Source": clean(r.get("Source")),
            "Source File": str(INPUT_FILE),
            "RejectReason": "",
            "AIRelevant": clean(r.get("AIRelevant")) or "N",
            "AIRelevanceScore": int(float(r.get("AIRelevanceScore", 0) or 0)),
            "AIReason": clean(r.get("AIReason")),
            "Top3 Eligible": "N" if supplemental_watch else (clean(r.get("Top3 Eligible")) or "N"),
            "Body Verified": clean(r.get("Body Verified")) or "N",
            "Direct Evidence": clean(r.get("Direct Evidence")),
            "Missing Facts": clean(r.get("Missing Facts")),
            "Policy Event": clean(r.get("Policy Event")) or "N",
            "Official Evidence": clean(r.get("Official Evidence")),
            "RegulationMappingType": clean(r.get("RegulationMappingType")),
            "MappingStatus": clean(r.get("MappingStatus")),
            "RequiredMappingKeys": clean(r.get("RequiredMappingKeys")),
            "EntityDirectFlag": clean(r.get("EntityDirectFlag")),
            "MappedEntity": clean(r.get("MappedEntity")),
            "MappedProduct": clean(r.get("MappedProduct")),
            "MappedHS": clean(r.get("MappedHS")),
            "TradeRoute": clean(r.get("TradeRoute")),
            "MappingEvidence": clean(r.get("MappingEvidence")),
            "Article Extract Status": clean(r.get("Article Extract Status")),
            "Article Source Type": clean(r.get("Article Source Type")),
            "Article Body Evidence": clean(r.get("Article Body Evidence")),
            "Policy Stage": clean(r.get("Policy Stage")),
            "Quality Contract": clean(r.get("Quality Contract")) or "STRICT_PASS",
            "PolicyDeltaFlag": clean(r.get("PolicyDeltaFlag")) or "N",
            "PolicyDeltaType": clean(r.get("PolicyDeltaType")) or "NO_CURRENT_DELTA",
            "PolicyDeltaReason": clean(r.get("PolicyDeltaReason")),
            "ReportLayer": clean(r.get("ReportLayer")) or "REFERENCE",
            "CustomsCaseFlag": clean(r.get("CustomsCaseFlag")) or "N",
            "CustomsCaseType": clean(r.get("CustomsCaseType")),
            "CustomsCaseReason": clean(r.get("CustomsCaseReason")),
            "DirectCustomsCaseFlag": clean(r.get("DirectCustomsCaseFlag")) or "N",
            "PeerPrecedentFlag": clean(r.get("PeerPrecedentFlag")) or "N",
            "OfficialSourceURL": clean(r.get("OfficialSourceURL")),
            "OfficialSourceStatus": clean(r.get("OfficialSourceStatus")) or "NOT_IDENTIFIED",
            "OfficialSourceCandidate": clean(r.get("OfficialSourceCandidate")),
            "EvidenceGateFlag": clean(r.get("EvidenceGateFlag")) or "N",
            "SamsungTradeGateFlag": clean(r.get("SamsungTradeGateFlag")) or "N",
            "ThreeGateStatus": clean(r.get("ThreeGateStatus")) or "G1_POLICY_DELTA_FAIL",
            "EventDuplicateCount": int(float(r.get("DuplicateCount", 0) or 0)),
            "AlternateHeadlines": clean(r.get("ClusterHeadlines")),
            "AlternateURLs": clean(r.get("AlternateURLs")),
            "EvidenceStatus": (
                "VERIFIED" if clean(r.get("EvidenceGateFlag")) == "Y" and clean(r.get("OfficialSourceURL"))
                else "SUPPORTED" if clean(r.get("Body Verified")) == "Y"
                else "PENDING"
            ),
            "WhatChanged": clean(r.get("SummaryAI")),
            "PolicyStageDisplay": clean(r.get("Policy Stage")) or clean(r.get("PolicyDeltaType")),
            "EffectiveDate": clean(r.get("effective_date_hint")) or "확인 필요",
            "AffectedProduct": clean(r.get("MappedProduct")) or "확인 필요",
            "AffectedHS": clean(r.get("MappedHS")) or "확인 필요",
            "SamsungImpactPath": clean(r.get("AnalysisAI")),
            "RecommendedAction": clean(r.get("ActionAI")),
            "VerificationNeeded": clean(r.get("Missing Facts")) or "추가 확인 없음",
            "OfficialURL": clean(r.get("OfficialSourceURL")),
        })
    daily = pd.DataFrame(selected_rows, columns=OUTPUT_COLS)

    # v36 final contract: remove non-policy business/news items that can pass
    # solely because their body contains a tariff word, and invalidate master
    # mappings unless the article itself names Samsung in customs context.
    post_guard_removed = []
    if not daily.empty:
        def _post_text(row: pd.Series) -> str:
            return " ".join(clean(row.get(c)) for c in [
                "Headline", "Summary", "AI Analysis", "Direct Evidence",
                "Article Body Evidence",
            ]).lower()

        def _article_native_text(row: pd.Series) -> str:
            return " ".join(clean(row.get(c)) for c in [
                "Headline", "Direct Evidence", "Article Body Evidence",
            ]).lower()

        def _hard_nonpolicy(row: pd.Series) -> bool:
            title = clean(row.get("Headline")).lower()
            text = _post_text(row)
            title_noise = [
                "으뜸이", "할랄시장 잡아라", "금리 압박", "수출액 늘었다고 경쟁력",
                "인사 발령", "임원 인사", "수상자", "포상", "신용등급", "배당",
                "영업익", "주택주", "동반 약세", "잔디밭", "청소로봇",
                "tariffs can't fund", "job losses", "trade-offs facing",
            ]
            if any(term in title for term in title_noise):
                return True
            if any(term in title for term in MARKET_COMPANY_NOISE_TERMS + FOOD_SUPPLY_NOISE_TERMS):
                if not concrete_policy_delta(_article_native_text(row)):
                    return True
            article_text = _article_native_text(row)
            if (
                any(term in title for term in ["토요타", "toyota", "현대차", "hyundai", "general motors", "gm…"])
                and not re.search(r"삼성전자|samsung electronics|samsung semiconductor", article_text, re.I)
            ):
                return True
            enforcement_case = any(term in title for term in [
                "accused of evading", "탈세 혐의", "관세 포탈", "세액 추징",
            ])
            policy_change = any(term in text for term in [
                "법 개정", "고시 개정", "시행", "발효", "부과하기로", "도입", "폐지",
                "official notice", "entered into force", "effective from",
            ])
            if enforcement_case and not policy_change and customs_case_gate(row)[0] != "Y":
                return True
            individual_case = any(term in title for term in [
                "추가 납부 의무", "과거 수입 관세 오류", "벌금 부과", "소송 패소",
                "세액 추징", "additional payment", "past import tariff error",
            ])
            if individual_case and not concrete_policy_delta(article_text) and customs_case_gate(row)[0] != "Y":
                return True
            if ("fta" in title or "자유무역협정" in title) and not any(term in text for term in [
                "서명", "발효", "타결", "개정", "업그레이드", "upgrade", "협상 개시",
                "관세 철폐", "원산지 기준 변경",
            ]):
                return True
            return False

        hard_mask = daily.apply(_hard_nonpolicy, axis=1)
        if hard_mask.any():
            # Keep customs-relevant background in Global Context instead of
            # silently deleting it from the morning report.
            daily.loc[hard_mask, "ReportLayer"] = "REFERENCE"
            daily.loc[hard_mask, "PolicyDeltaFlag"] = "N"
            daily.loc[hard_mask, "PolicyDeltaType"] = "NON_POLICY_CONTEXT"
            daily.loc[hard_mask, "PolicyDeltaReason"] = "신규 정책조치가 아닌 기업·시장·개별사건 참고정보"
            daily.loc[hard_mask, "Samsung Impact"] = "Watch"
            daily.loc[hard_mask, "Top3 Eligible"] = "N"
            log(f"V47 GLOBAL CONTEXT DOWNGRADE: {int(hard_mask.sum())}")

        # Clear master-derived entity/product/HS/route when the original body
        # does not explicitly connect Samsung, the product and customs policy.
        mapping_downgraded = 0
        for idx, row in daily.iterrows():
            if clean(row.get("EntityDirectFlag")).upper() != "Y":
                continue
            text = _article_native_text(row)
            samsung_named = bool(re.search(r"삼성전자|samsung electronics|samsung semiconductor", text, re.I))
            mapped_product = clean(row.get("MappedProduct")).lower()
            product_named = bool(mapped_product and _term_in_text(text, mapped_product))
            customs_named = any(term in text for term in [
                "관세", "통관", "수출통제", "반덤핑", "원산지", "tariff", "customs",
                "export control", "anti-dumping", "section 232", "section 301",
            ])
            if not (samsung_named and product_named and customs_named):
                daily.at[idx, "EntityDirectFlag"] = "N"
                daily.at[idx, "MappedEntity"] = ""
                daily.at[idx, "MappedProduct"] = ""
                daily.at[idx, "MappedHS"] = ""
                daily.at[idx, "TradeRoute"] = ""
                daily.at[idx, "MappingEvidence"] = ""
                daily.at[idx, "MappingStatus"] = "MAPPING_REQUIRED"
                daily.at[idx, "RegulationMappingType"] = "POLICY_GENERAL"
                daily.at[idx, "Top3 Eligible"] = "N"
                daily.at[idx, "SamsungTradeGateFlag"] = "N"
                if clean(daily.at[idx, "PolicyDeltaFlag"]) == "Y" and clean(daily.at[idx, "EvidenceGateFlag"]) == "Y":
                    daily.at[idx, "ThreeGateStatus"] = "G3_SAMSUNG_MAPPING_PENDING"
                mapping_downgraded += 1
        if mapping_downgraded:
            log(f"V36 ARTICLE-NATIVE MAPPING GUARD: downgraded={mapping_downgraded}")

        def _issue_fix(row: pd.Series) -> str:
            text = _post_text(row)
            title = clean(row.get("Headline")).lower()
            if any(x in title for x in ["수출통제", "전략물자", "export control"]): return "EXPORT_CONTROL"
            if any(x in title for x in ["성실신고확인", "원산지표시", "특별단속", "통관"]): return "CUSTOMS"
            if any(x in text for x in ["cbam", "탄소국경", "carbon border"]): return "CBAM_CARBON"
            if any(x in text for x in ["반덤핑", "덤핑방지", "anti-dumping"]): return "AD_CVD"
            if any(x in text for x in ["수출통제", "전략물자", "export control"]): return "EXPORT_CONTROL"
            if any(x in title for x in ["fta", "자유무역협정", "원산지 기준"]): return "ORIGIN_FTA"
            if any(x in title for x in ["품목분류", "hs code", "tariff classification"]): return "HS_CLASSIFICATION"
            return clean(row.get("Issue")) or "TARIFF"
        daily["Issue"] = daily.apply(_issue_fix, axis=1)

        # Re-apply explicit event keys after AI enrichment. This also catches
        # law/news wording and Korean/English translations missed before AI.
        def _final_event_key(row: pd.Series) -> str:
            text = _post_text(row)
            rules = [
                ("US_RUSSIA_IRAN_SECONDARY_TARIFF_100", [["러시아", "russia"], ["이란", "iran", "원유", "oil", "natural gas"], ["100%", "100％", "secondary tariff", "제재법", "sanctions act"]]),
                ("KR_US_INVESTMENT_301_232_COST", [["대미투자", "미국 투자", "u.s. investment", "us investment"], ["301조", "section 301", "232조", "section 232", "관세"], ["중간재", "자본재", "설비", "equipment", "capital goods"]]),
                ("US_CHINA_SUMMIT_TARIFF_NEGOTIATION", [["트럼프", "미국", "u.s."], ["시진핑", "중국", "china"], ["정상회담", "회담", "summit", "관세 담판", "tariff negotiation"]]),
                ("US_SEMICON_TARIFF_INVESTMENT_PRESSURE", [["반도체", "semiconductor", "chip"], ["100%", "100％", "관세", "tariff"], ["미국 투자", "현지 생산", "local production", "invest"]]),
                ("VN_STRATEGIC_EXPORT_CONTROL", [["베트남", "vietnam"], ["수출통제", "전략물자", "export control"], ["hs코드", "hs code", "기술사양", "재수출", "환적", "re-export", "transshipment"]]),
                ("US_CHINA_30B_TARIFF_NEGOTIATION", [["300억달러", "30 billion", "$30 billion"], ["미중", "미·중", "미국", "중국", "u.s.", "china"], ["상호 관세", "관세인하", "관세 인하", "tariff reduction"]]),
                ("KR_TBFC_TASK_FORCE", [["tbfc", "무역기반 재정", "무역기반 금융", "특별수사단", "특수단"], ["관세청", "관세 범죄", "시장교란", "부정수급"]]),
                ("KR_CUSTOMS_PUBLIC_NOTICE_BILL", [["공시송달", "온라인 게시", "유니패스", "국가관세종합정보시스템"], ["관세법", "국세기본법", "조승래"]]),
                ("KR_SOYBEAN_SUPPLY", [["수입콩", "두부", "국산콩"], ["부족", "공급", "재고", "할당"]]),
                ("CN_JP_DICHLOROSILANE_AD_2026", [["디클로로실란", "반도체 가스", "반도체 핵심소재"], ["반덤핑", "보증금", "99.2%", "99.2％"], ["중국", "china"]]),
                ("VN_IN_CERAMIC_TILE_AD_2026", [["세라믹 타일", "ceramic tile"], ["반덤핑", "anti-dumping"], ["인도", "india"]]),
                ("KR_CN_HGI_GI_AD_GAP_2026", [["hgi", "gi", "용융아연도금"], ["반덤핑", "관세"], ["중국", "china"]]),
                ("KR_STRATEGIC_EXPORT_CONTROL_AI_CHIP", [["전략물자수출입고시", "전략물자 수출입고시"], ["ai 칩", "ai용 집적회로", "반도체 장비", "수출통제"]]),
                ("KR_CHINA_BUTYL_ACRYLATE_AD", [["아크릴산 부틸", "butyl acrylate"], ["덤핑관세", "덤핑 관세", "반덤핑", "anti-dumping", "anti dumping", "duties"]]),
                ("US_KR_COUPANG_SECTION301_TARIFF", [["쿠팡", "coupang"], ["301조", "section 301", "추가 관세", "관세 보복"]]),
                ("KR_HOLIDAY_ORIGIN_MARKING_ENFORCEMENT", [["추석", "명절"], ["원산지표시", "국산 둔갑", "원산지 표시"], ["단속", "관세청"]]),
                ("G20_TRADE_IMBALANCE_CHINA", [["g20"], ["무역 불균형", "trade imbalance"], ["중국", "china"]]),
                ("EU_LOW_VALUE_DEMINIMIS_2026", [["eu", "유럽연합"], ["저가", "low-value", "de minimis", "150유로"], ["3유로", "€3", "면세 폐지", "tariff", "관세"]]),
            ]
            for name, groups in rules:
                if all(any(term in text for term in group) for group in groups):
                    return name
            return clean(row.get("Cluster")) or clean(row.get("Headline")).lower()

        daily["_v36_event_key"] = daily.apply(_final_event_key, axis=1)
        before_v36_dedup = len(daily)
        daily["_official_rank"] = daily.get("OfficialSourceStatus", pd.Series("", index=daily.index)).eq("VERIFIED_EXACT").astype(int)
        daily = daily.sort_values(
            ["_v36_event_key", "_official_rank", "Importance Score"],
            ascending=[True, False, False], kind="stable",
        )
        collapsed = []
        for _, group in daily.groupby("_v36_event_key", sort=False, dropna=False):
            rep = group.iloc[0].copy()
            rep["EventDuplicateCount"] = int(pd.to_numeric(group.get("EventDuplicateCount", 0), errors="coerce").fillna(0).sum()) + len(group) - 1
            headlines = [clean(x) for x in group.get("Headline", pd.Series(dtype=str)) if clean(x)]
            urls = [clean(x) for x in group.get("URL", pd.Series(dtype=str)) if clean(x)]
            rep["AlternateHeadlines"] = " | ".join(dict.fromkeys(headlines))
            rep["AlternateURLs"] = " | ".join(dict.fromkeys(urls))
            collapsed.append(rep)
        daily = pd.DataFrame(collapsed).drop(columns=["_v36_event_key", "_official_rank"], errors="ignore")
        daily = daily.sort_values("Importance Score", ascending=False, kind="stable")
        if len(daily) != before_v36_dedup:
            log(f"V36 FINAL EVENT DEDUP: {before_v36_dedup} -> {len(daily)}")
        daily = daily.reset_index(drop=True)
        outlook_terms = ["전망", "퇴임해도", "기회", "관측", "가능성", "could", "may", "outlook"]
        for idx, row in daily.iterrows():
            title = clean(row.get("Headline")).lower()
            native = _article_native_text(row)
            if any(term in title for term in outlook_terms) and not concrete_policy_delta(native):
                daily.at[idx, "Samsung Impact"] = "Watch"
                daily.at[idx, "Top3 Eligible"] = "N"
                daily.at[idx, "Policy Stage"] = "COMMENTARY_OR_OUTLOOK"
                daily.at[idx, "MappingStatus"] = "MONITORING_ONLY"
        daily["No"] = range(1, len(daily) + 1)

    rejected_ai = audit[~audit.index.isin(selected_audit_indices)].copy()
    if not rejected_ai.empty:
        def _reject_reason(row: pd.Series) -> str:
            reasons = []
            if clean(row.get("AIRelevant")).upper() != "Y": reasons.append("AI_NOT_RELEVANT")
            if clean(row.get("Body Verified")).upper() != "Y": reasons.append("BODY_NOT_VERIFIED")
            if clean(row.get("Policy Event")).upper() != "Y": reasons.append("NO_CONCRETE_POLICY_EVENT")
            if safe_int(row.get("AIRelevanceScore")) < WATCH_MIN_RELEVANCE: reasons.append("SCORE_BELOW_MIN")
            if bool(row.get("_EventOnly", False)): reasons.append("EVENT_ONLY")
            if clean(row.get("Issue")).upper() == "OTHER": reasons.append("INVALID_ISSUE")
            return "|".join(reasons) or "NOT_SELECTED_AFTER_EVENT_DEDUP_OR_CAP"
        rejected_ai["RejectReason"] = rejected_ai.apply(_reject_reason, axis=1)
    excluded = pd.concat([stale, pre_ai_duplicates, tail, rejected_ai, *post_guard_removed], ignore_index=True, sort=False)

    return daily, audit, excluded


def merge_cumulative(daily: pd.DataFrame) -> pd.DataFrame:
    if OUT_CUMULATIVE.exists():
        try:
            old = unique_columns(pd.read_excel(OUT_CUMULATIVE))
        except Exception:
            old = pd.DataFrame()
    else:
        old = pd.DataFrame()
    combined = pd.concat([old, daily], ignore_index=True, sort=False)
    if combined.empty:
        return combined
    combined["_key"] = combined["Headline"].fillna("").astype(str).str.lower().str.strip()
    combined = combined.drop_duplicates("_key", keep="last").drop(columns="_key")
    return combined.reset_index(drop=True)


def safe_write(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_excel(path, index=False)
    except PermissionError:
        alt = path.with_name(f"{path.stem}_{datetime.now():%Y%m%d_%H%M%S}{path.suffix}")
        df.to_excel(alt, index=False)
        log(f"FILE LOCKED -> {alt}")


def main() -> int:
    log("GTI STEP4-2 NEWS AI v47.3 KST PRIOR-DAY POLICY ENGINE START")
    log(f"MODEL={GEMINI_MODEL} / Gemini={'Y' if USE_GEMINI else 'N'} / 24h / max={TARGET_MAX}")
    daily, audit, excluded = build()
    before_contract = len(daily)
    pre_contract_daily = daily.copy()
    daily, contract_rejected = apply_quality_contract(daily, include_reference=True)
    # Final shared contract still assumes every visible row is a policy event.
    # Re-attach only the explicitly qualified operational customs cases.
    if not pre_contract_daily.empty:
        protected_final = pre_contract_daily[
            pre_contract_daily.get("CustomsCaseFlag", pd.Series("N", index=pre_contract_daily.index))
            .fillna("N").astype(str).str.upper().eq("Y")
        ].copy()
        if not protected_final.empty:
            existing = set(daily.get("URL", pd.Series(dtype=str)).fillna("").astype(str) + "|" + daily.get("Headline", pd.Series(dtype=str)).fillna("").astype(str))
            keys = protected_final.get("URL", pd.Series("", index=protected_final.index)).fillna("").astype(str) + "|" + protected_final.get("Headline", pd.Series("", index=protected_final.index)).fillna("").astype(str)
            protected_final = protected_final[~keys.isin(existing)]
            daily = pd.concat([daily, protected_final], ignore_index=True, sort=False)
            if not protected_final.empty:
                log(f"CUSTOMS CASE FINAL CONTRACT RESCUE: {len(protected_final)}")
            if not contract_rejected.empty:
                rejected_keys = contract_rejected.get("URL", pd.Series("", index=contract_rejected.index)).fillna("").astype(str) + "|" + contract_rejected.get("Headline", pd.Series("", index=contract_rejected.index)).fillna("").astype(str)
                protected_keys = set(keys[keys.isin(existing) | keys.notna()].tolist())
                contract_rejected = contract_rejected[~rejected_keys.isin(protected_keys)].copy()
    if not contract_rejected.empty:
        contract_rejected = contract_rejected.copy()
        contract_rejected["RejectReason"] = "GOLD_CONTRACT:" + contract_rejected["ContractReason"].astype(str)
        excluded = pd.concat([excluded, contract_rejected], ignore_index=True, sort=False)
    daily = apply_action_queue_contract(daily)
    daily["No"] = range(1, len(daily) + 1)
    log(
        f"GOLD QUALITY CONTRACT {CONTRACT_VERSION}: before={before_contract} / "
        f"selected={len(daily)} / rejected={len(contract_rejected)} / forced_fill=0"
    )
    cumulative = merge_cumulative(daily)
    safe_write(OUT_SUMMARY, daily)
    safe_write(OUT_CUMULATIVE, cumulative)
    safe_write(OUT_AUDIT, audit)
    safe_write(OUT_EXCLUDED, excluded)
    safe_write(OUT_LEGACY, daily)
    log(f"DONE selected={len(daily)} / audit={len(audit)} / excluded={len(excluded)} / cumulative={len(cumulative)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
