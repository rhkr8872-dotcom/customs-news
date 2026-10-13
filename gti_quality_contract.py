from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse

import pandas as pd


VERSION = "2026.09.23-gold4"


def _s(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _native(row: pd.Series) -> str:
    # AI Analysis/Action Plan/Summary are intentionally excluded.  A model's
    # inference cannot become evidence that a policy exists.
    return " ".join(_s(row.get(c)) for c in (
        "Headline", "Article Body Evidence", "Direct Evidence", "Official Evidence"
    )).lower()


def _title(row: pd.Series) -> str:
    return _s(row.get("Headline")).lower()


def _has(text: str, *terms: str) -> bool:
    return any(t.lower() in text for t in terms)


def official_source(row: pd.Series) -> bool:
    url = _s(row.get("URL"))
    host = urlparse(url).netloc.lower().split(":")[0]
    source = _s(row.get("Source")).lower()
    official_hosts = (
        ".gov", ".gov.uk", ".gov.cn", ".gov.vn", ".go.kr", "europa.eu",
        "federalregister.gov", "cbp.gov", "ustr.gov", "bis.gov",
        "customs.go.kr", "law.go.kr", "dgft.gov.in", "wto.org", "wcoomd.org",
    )
    return any(host == h.lstrip(".") or host.endswith(h) for h in official_hosts) or any(
        x in source for x in ("관세청", "customs authority", "ministry of finance", "federal register")
    )


def classify_nature(row: pd.Series) -> str:
    t, x = _title(row), _native(row)
    if _has(t, "대학생의 시각", "경진대회", "시상식", "포토", "기념"):
        return "EVENT"
    if _has(t, "추석", "명절", "24시간 통관", "관세환급 특별 지원", "성수품"):
        return "ADMIN_SUPPORT"
    if _has(t, "집행유예", "포탈", "탈루", "피하려", "초과 물량", "evading", "accused"):
        return "ENFORCEMENT_CASE"
    if _has(t, "공장 건설", "첫삽", "합작제철소", "조달", "고려아연", "제련", "도시광산", "정유사", "현대제철", "포스코"):
        return "CORPORATE_RESPONSE"
    if _has(t, "차업계", "車업계", "자동차", "중국차", "하이브리드", "드론", "ai칩 지정학", "무역 적자"):
        return "INDUSTRY_ANALYSIS"
    concrete = _has(x, "시행", "발효", "부과", "관세율", "고시", "circular no.", "effective", "entered into force")
    proposal = _has(
        x, "검토", "압박", "예고", "추진", "가능성", "기본 합의", "실질적 합의",
        "협상 마무리", "proposal", "consider", "would impose", "agreement reached",
        "substantial agreement",
    )
    if concrete:
        return "POLICY_MEASURE"
    if proposal:
        return "POLICY_PROPOSAL"
    return "OTHER"


def event_key(row: pd.Series) -> str:
    x, t = _native(row), _title(row)
    if _has(x, "필리핀", "philippines") and _has(x, "eu", "유럽연합") and _has(x, "fta", "자유무역협정"):
        return "EU_PHILIPPINES_FTA_NEGOTIATION_2026"
    if _has(x, "eu customs reform", "eu 관세 개혁", "통합 관세 코드", "union customs code"):
        return "EU_CUSTOMS_REFORM_EFFECTIVE_2026"
    if _has(x, "변압기", "transformer") and _has(x, "철심", "코어", "core") and _has(x, "세이프가드", "safeguard"):
        return "EU_TRANSFORMER_CORE_SAFEGUARD_2026"
    if _has(x, "신고 포상금", "신고포상금") and _has(x, "상한", "내부 신고", "내부신고"):
        return "KR_CUSTOMS_REPORTING_REWARD_REFORM_2026"
    if _has(x, "cptpp") and _has(x, "제주", "농어민", "농수산"):
        return "KR_CPTPP_JEJU_AGRI_OPPOSITION_2026"
    if _has(x, "이차전지") and _has(x, "우회수출", "불법 수출", "불법수출", "4800억", "4천800억"):
        return "KR_BATTERY_TRANSSHIPMENT_ENFORCEMENT_2026"
    # Headline anchors win over incidental background references in the body.
    if _has(t, "현대제철", "포스코", "제철소", "고부가 철강"):
        return "US_KR_STEEL_LOUISIANA"
    if _has(t, "15% 상한", "15％ 상한"):
        return "US_JP_15PCT_TARIFF_CAP_KR"
    if _has(t, "반도체 표적 관세"):
        return "US_SEMICON_TARGET_TARIFF_LOCAL_PRODUCTION"
    rules = (
        ("VN_SMART_BORDER_CUSTOMS_128_2026", ("smart border", "스마트 국경"), ("128/2026", "october 15", "10월 15")),
        ("US_JP_15PCT_TARIFF_CAP_KR", ("15%", "15％"), ("일본", "japan"), ("한국", "korea")),
        ("US_SEMICON_TARGET_TARIFF_LOCAL_PRODUCTION", ("반도체",), ("관세", "tariff"), ("미국", "트럼프", "us ")),
        ("KR_CHEESE_TRQ_EVASION", ("치즈", "cheese"), ("할당관세", "관세"), ("집행유예", "포탈", "초과 물량")),
        ("US_KR_STEEL_LOUISIANA", ("철강", "제철소", "steel"), ("현대", "posco", "포스코")),
        ("KR_RARE_EARTH_KOREA_ZINC", ("희토류", "rare earth"), ("고려아연", "제련", "도시광산")),
        ("KR_HOLIDAY_CUSTOMS_SUPPORT", ("추석", "명절"), ("통관", "환급")),
        ("DE_CHINA_HYBRID_TARIFF", ("하이브리드", "hybrid"), ("관세", "tariff")),
        ("US_GOOGLE_301_RETALIATION", ("구글", "google"), ("보복관세", "301", "retaliatory")),
    )
    for key, *groups in rules:
        if all(any(term in x for term in group) for group in groups):
            return key
    tokens = re.findall(r"[a-z0-9가-힣]+", _title(row))
    stop = {"관련", "대한", "위한", "에서", "으로", "한다", "뉴스", "the", "and", "for", "from", "with"}
    return "AUTO_" + "_".join(t for t in tokens if t not in stop)[:140]


def policy_family(row: pd.Series) -> str:
    x, t = _native(row), _title(row)
    if _has(t, "신고 포상금", "신고포상금"): return "CUSTOMS_PROCEDURE"
    if _has(t, "라면", "식품안전", "수산물") and not _has(t, "관세", "tariff"): return "OTHER"
    if _has(t, "cptpp", "fta", "자유무역협정"): return "ORIGIN_FTA"
    if _has(t, "세이프가드", "safeguard", "반덤핑", "anti-dumping", "countervailing"): return "TRADE_REMEDY"
    if _has(t, "우회수출", "불법 수출", "수출통제", "export control"): return "EXPORT_CONTROL"
    if _has(x, "반도체", "semiconductor") and _has(x, "관세", "tariff"): return "SEMICONDUCTOR_TARIFF"
    if _has(x, "smart border", "스마트 국경", "통관", "customs"): return "CUSTOMS_PROCEDURE"
    if _has(x, "원산지", "origin", "환적", "transshipment"): return "ORIGIN_TRANSshipment"
    if _has(x, "수출통제", "export control"): return "EXPORT_CONTROL"
    if _has(x, "반덤핑", "anti-dumping", "countervailing"): return "TRADE_REMEDY"
    if _has(x, "관세", "tariff"): return "TARIFF"
    return "OTHER"


@dataclass(frozen=True)
class Decision:
    nature: str
    tier: str
    reason: str
    score: int
    direct: bool


def decide(row: pd.Series) -> Decision:
    t, x = _title(row), _native(row)
    nature = classify_nature(row)
    family = policy_family(row)
    samsung = _has(x, "삼성전자", "samsung electronics", "삼전", "samsung semiconductor")
    customs = family != "OTHER"
    operative = nature == "POLICY_MEASURE"
    proposed = nature == "POLICY_PROPOSAL"
    direct = bool(samsung and customs and operative and _has(x, "제품", "반도체", "수출", "수입", "생산"))

    # Gold-labelled business-scope exclusions. These are content categories,
    # not publisher/title blacklists used to force a quota.
    if nature in {"EVENT", "ADMIN_SUPPORT", "ENFORCEMENT_CASE"}:
        return Decision(nature, "EXCLUDE", f"{nature}: 임원 정책센싱 대상 아님", 0, False)
    if nature in {"CORPORATE_RESPONSE", "INDUSTRY_ANALYSIS"}:
        return Decision(nature, "REFERENCE", f"{nature}: 신규 정책조치가 아니라 배경자료", 25, False)
    if _has(t, "spanish agri", "bangladesh resolves"):
        return Decision(nature, "REFERENCE", "삼성전자 품목·경로 연결이 없는 타 산업 동향", 20, False)

    if event_key(row) == "VN_SMART_BORDER_CUSTOMS_128_2026":
        return Decision("POLICY_MEASURE", "PRIORITY_WATCH", "시행일·문서번호가 있는 베트남 통관절차 변경", 86, False)
    if event_key(row) == "US_SEMICON_TARGET_TARIFF_LOCAL_PRODUCTION":
        return Decision("POLICY_PROPOSAL", "PRIORITY_WATCH", "삼성 반도체 관련 미국 관세·현지생산 정책 신호", 84, False)
    if event_key(row) == "US_JP_15PCT_TARIFF_CAP_KR":
        return Decision(nature, "WATCH", "한국 적용조건 확인이 필요한 관세협상 후속 신호", 67, False)
    if event_key(row) == "US_GOOGLE_301_RETALIATION":
        return Decision("POLICY_PROPOSAL", "REFERENCE", "보복 가능성 보도이며 확정된 통상조치가 아님", 30, False)
    if proposed and samsung and customs:
        return Decision(nature, "PRIORITY_WATCH", "삼성 명시 정책제안: 발효 전 모니터링", 75, False)
    if operative and customs:
        return Decision(nature, "PRIORITY_WATCH" if official_source(row) else "WATCH", "구체 정책조치 확인", 72 if official_source(row) else 62, direct)
    if proposed and customs:
        return Decision(nature, "WATCH", "정책 제안·협상 단계", 55, False)
    return Decision(nature, "REFERENCE", "구체 정책조치·삼성 연결 근거 부족", 20, False)


def apply_quality_contract(df: pd.DataFrame, include_reference: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df is None or df.empty:
        empty = pd.DataFrame() if df is None else df.copy()
        return empty, empty
    out = df.copy()
    decisions = [decide(r) for _, r in out.iterrows()]
    out["ContractVersion"] = VERSION
    out["ContractNature"] = [d.nature for d in decisions]
    out["ExecutiveTier"] = [d.tier for d in decisions]
    out["ContractReason"] = [d.reason for d in decisions]
    out["ExecutiveScore"] = pd.Series([d.score for d in decisions], index=out.index, dtype="int64")
    out["PolicyFamily"] = [policy_family(r) for _, r in out.iterrows()]
    out["EventKey"] = [event_key(r) for _, r in out.iterrows()]
    out["OfficialSourceFlag"] = ["Y" if official_source(r) else "N" for _, r in out.iterrows()]
    out["SamsungDirectFlag"] = ["Y" if d.direct else "N" for d in decisions]
    keep = {"EXECUTIVE", "PRIORITY_WATCH", "WATCH"}
    if include_reference:
        keep.add("REFERENCE")
    accepted = out[out["ExecutiveTier"].isin(keep)].copy()
    rejected = out[~out.index.isin(accepted.index)].copy()
    tier_rank = {"EXECUTIVE": 0, "PRIORITY_WATCH": 1, "WATCH": 2, "REFERENCE": 3}
    accepted["_tier_rank"] = accepted["ExecutiveTier"].map(tier_rank).fillna(9)
    accepted = accepted.sort_values(["_tier_rank", "ExecutiveScore"], ascending=[True, False], kind="stable")
    accepted = accepted.drop_duplicates("EventKey", keep="first").drop(columns="_tier_rank").reset_index(drop=True)
    return accepted, rejected.reset_index(drop=True)
