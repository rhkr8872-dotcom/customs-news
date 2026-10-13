# -*- coding: utf-8 -*-
"""GTI STEP5 v45 evidence-gated executive report engine.

One preparation path, one quality contract, one send path.  --preview and
--no-email never mutate cumulative history.
"""
from __future__ import annotations

import argparse
import html
import os
import re
import smtplib
import ssl
from datetime import datetime, timedelta
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path

import pandas as pd

from gti_quality_contract import apply_quality_contract, VERSION as CONTRACT_VERSION
from gti_report_window import kst_now, previous_kst_day_mask


BASE = Path(os.getenv("GTI_BASE_DIR", r"C:\Temp"))
OUT_DIR = Path(os.getenv("GTI_OUTPUT_DIR", str(BASE / "12345" / "c_type_outputs")))
NEWS_FILE = BASE / "4-2.news_ai_summary.xlsx"
REG_FILE = BASE / "4-1.regulation_ai_summary.xlsx"
CUM_FILE = BASE / "gti_news_cumulative.xlsx"
RECIPIENT_FILE = BASE / "00.xlsx"
SMTP_HOST = os.getenv("GTI_SMTP_HOST", "smtp.naver.com")
SMTP_PORT = int(os.getenv("GTI_SMTP_PORT", "465"))
SMTP_USER = os.getenv("GTI_SMTP_USER", "kch8872@naver.com").strip()
SMTP_PASS = (os.getenv("GTI_SMTP_PASS") or os.getenv("GTI_MAIL_PW") or "").strip()


def s(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return re.sub(r"\s+", " ", str(v)).strip()


def publication_date(v) -> str:
    """Return the source publication date used for sensing, never the run date."""
    dt = pd.to_datetime(v, errors="coerce")
    if pd.isna(dt):
        return "원문 게시일 확인 필요"
    if dt.hour or dt.minute or dt.second:
        return dt.strftime("%Y-%m-%d %H:%M")
    return dt.strftime("%Y-%m-%d")


def row_publication_date(row: pd.Series) -> str:
    for col in ("Publish Date", "Date", "Original Publish Date"):
        value = row.get(col, "")
        if value is not None and not pd.isna(value) and s(value):
            return publication_date(value)
    return "원문 게시일 확인 필요"


def regulation_event_key(row: pd.Series) -> str:
    """Preserve bill/document identity so same-title bills never collapse."""
    existing = s(row.get("EventKey"))
    identity = s(row.get("DocumentIdentity"))
    bill_no = re.sub(r"\.0$", "", s(row.get("BillNo")))
    url = s(row.get("URL"))
    bill_match = re.search(r"/out/(\d+)/", url)
    if identity:
        return identity.lower()
    if bill_no:
        return f"bill:{bill_no.lower()}"
    if bill_match:
        return f"bill:{bill_match.group(1)}"
    if existing:
        return existing
    headline = re.sub(r"\W+", "_", s(row.get("Headline")).lower())[:130]
    return f"REG_{headline}_{publication_date(row.get('Date'))}"


def read_excel_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_excel(path)
        df.columns = [s(c) for c in df.columns]
        return df
    except Exception as exc:
        raise RuntimeError(f"INPUT UNREADABLE: {path.name}: {type(exc).__name__}: {exc}") from exc


def within_24h(df: pd.DataFrame, now: datetime) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df.copy(), df.copy()
    out = df.copy()
    col = "Publish Date" if "Publish Date" in out else "Date"
    keep, out["_published"], _, _ = previous_kst_day_mask(out.get(col), now=now)
    return out[keep].drop(columns="_published"), out[~keep].drop(columns="_published")


def has_substantive_text(value, headline="", min_chars: int = 45) -> bool:
    text = re.sub(r"\s+", " ", s(value)).strip()
    title = re.sub(r"\s+", " ", s(headline)).strip()
    if not text or len(text) < min_chars:
        return False
    if title and re.sub(r"\W+", "", text).lower() == re.sub(r"\W+", "", title).lower():
        return False
    return True


def normalize_regulation(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    if "Headline" not in out:
        for c in ("Title", "Regulation", "제목"):
            if c in out:
                out["Headline"] = out[c]
                break
    for col in ("Headline", "Summary", "AI Analysis", "Action Plan", "Country", "Agency", "URL", "Date"):
        if col not in out: out[col] = ""
    out["Content Type"] = "Regulation"
    verified = out.get("Body Verified", pd.Series("N", index=out.index)).astype(str).str.upper().eq("Y")
    score = pd.to_numeric(out.get("Importance Score", 70), errors="coerce").fillna(70)
    out["ExecutiveScore"] = score.where(verified, score.clip(upper=59))
    out["ExecutiveTier"] = "WATCH"
    out.loc[verified & score.ge(85), "ExecutiveTier"] = "PRIORITY_WATCH"
    out["Original Publish Date"] = out["Date"].map(publication_date)
    out["EventKey"] = out.apply(regulation_event_key, axis=1)
    out["ContractReason"] = "공식 원문 검증 완료: 적용범위·시행일 및 삼성 거래 매핑 확인"
    out.loc[~verified, "ContractReason"] = "공식 게시물이나 원문 본문 미확인: 확인 완료 전 경영진 우선정책 승격 금지"
    out["SamsungDirectFlag"] = "N"
    out["DirectConfirmedFlag"] = "N"
    out["OfficialSourceFlag"] = "Y"
    out["VerificationStatus"] = verified.map({True: "VERIFIED", False: "PENDING"})
    out["DecisionStatus"] = verified.map({True: "Urgent Verification", False: "Verification Pending"})
    out["PriorityEligible"] = (verified & score.ge(85)).map({True: "Y", False: "N"})
    return out


def direct_confirmed_mask(rows: pd.DataFrame) -> pd.Series:
    if rows.empty:
        return pd.Series(False, index=rows.index, dtype=bool)
    return rows.get("DirectConfirmedFlag", pd.Series("N", index=rows.index)).astype(str).str.upper().eq("Y")


def enrich_decision_status(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    out = rows.copy()
    derived_verification = out.get("Body Verified", pd.Series("N", index=out.index)).astype(str).str.upper().eq("Y").map({True: "VERIFIED", False: "PENDING"})
    if "DecisionStatus" not in out: out["DecisionStatus"] = "Monitoring"
    else: out["DecisionStatus"] = out["DecisionStatus"].where(out["DecisionStatus"].fillna("").astype(str).str.strip().ne(""), "Monitoring")
    if "VerificationStatus" not in out: out["VerificationStatus"] = derived_verification
    else: out["VerificationStatus"] = out["VerificationStatus"].where(out["VerificationStatus"].fillna("").astype(str).str.strip().ne(""), derived_verification)
    if "PriorityEligible" not in out: out["PriorityEligible"] = "N"
    else: out["PriorityEligible"] = out["PriorityEligible"].fillna("N")
    news = out.get("Content Type", pd.Series("", index=out.index)).astype(str).str.lower().eq("news")
    direct = direct_confirmed_mask(out)
    proposed = out.get("Policy Stage", pd.Series("", index=out.index)).astype(str).str.upper().eq("PROPOSED_OR_MONITORING")
    verified = out["VerificationStatus"].eq("VERIFIED")
    score = pd.to_numeric(out.get("ExecutiveScore", 0), errors="coerce").fillna(0)
    priority_signal = out.get("ExecutiveTier", pd.Series("", index=out.index)).isin(["EXECUTIVE", "PRIORITY_WATCH"])
    out.loc[news & verified, "DecisionStatus"] = "Monitoring"
    out.loc[news & verified & proposed & priority_signal, "DecisionStatus"] = "Scenario Analysis"
    out.loc[news & verified & ~proposed & priority_signal & ~direct, "DecisionStatus"] = "Urgent Verification"
    out.loc[direct, "DecisionStatus"] = "Action Required"
    out.loc[news & verified & (direct | priority_signal) & score.ge(70), "PriorityEligible"] = "Y"
    return out


def select_priority(rows: pd.DataFrame, limit: int = 3) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    eligible = rows[rows.get("PriorityEligible", pd.Series("N", index=rows.index)).astype(str).str.upper().eq("Y")].copy()
    rank = {"Action Required": 0, "Urgent Verification": 1, "Scenario Analysis": 2, "Monitoring": 3}
    eligible["_decision_rank"] = eligible.get("DecisionStatus", "Monitoring").map(rank).fillna(9)
    eligible = eligible.sort_values(["_decision_rank", "ExecutiveScore"], ascending=[True, False], kind="stable")
    return eligible.head(limit).drop(columns="_decision_rank")


def historical_keys(df: pd.DataFrame) -> set[str]:
    keys: set[str] = set()
    for _, r in df.iterrows():
        url = s(r.get("URL")).lower()
        headline = re.sub(r"\W+", "", s(r.get("Headline")).lower())
        if url: keys.add("U:" + url)
        if headline: keys.add("H:" + headline)
    return keys


def remove_history(rows: pd.DataFrame, old: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if rows.empty or old.empty:
        return rows.copy(), 0
    known = historical_keys(old)
    mask = []
    for _, r in rows.iterrows():
        url = s(r.get("URL")).lower()
        headline = re.sub(r"\W+", "", s(r.get("Headline")).lower())
        # A valid official URL is the document identity.  Headline fallback is
        # used only when URL is absent, because separate bills can share the
        # exact same title (for example multiple 관세법 일부개정법률안).
        mask.append(("U:" + url in known) if url else (bool(headline) and "H:" + headline in known))
    dup = pd.Series(mask, index=rows.index)
    return rows[~dup].copy(), int(dup.sum())


def executive_sentence(rows: pd.DataFrame) -> str:
    if rows.empty:
        return "최근 24시간 내 새로 확인된 삼성전자 관세·통상 핵심 조치는 없습니다. 기존 고위험 정책은 변동 여부를 계속 모니터링합니다."
    axes = []
    priority = select_priority(rows, 3)
    source = priority if not priority.empty else rows.head(3)
    for _, r in source.iterrows():
        fam = s(r.get("PolicyFamily"))
        if fam == "SEMICONDUCTOR_TARIFF": axes.append("미국 반도체 관세와 현지생산 조건")
        elif fam == "CUSTOMS_PROCEDURE": axes.append("베트남 통관절차 변경")
        elif fam == "ORIGIN_TRANSshipment": axes.append("원산지·우회수출 검증")
        elif fam == "TARIFF": axes.append("주요국 관세협상 후속조건")
    axes = list(dict.fromkeys(axes))
    joined = "·".join(axes) if axes else "관세·통상 정책 변화"
    return f"금일 핵심 센싱은 {joined}입니다. HQ Customs는 시행조건과 대상 품목·법인·거래경로를 확인하고, 확정 전 사안은 비용 시나리오와 증빙 준비 수준으로 관리해야 합니다."


def action_text(row: pd.Series) -> str:
    fam = s(row.get("PolicyFamily"))
    if fam == "SEMICONDUCTOR_TARIFF":
        return "대미 반도체 품목별 관세 Cost와 미국 생산 전환 손익을 시나리오별 산출하고 발표·시행 Trigger를 지정"
    if fam == "CUSTOMS_PROCEDURE":
        return "베트남 법인과 Circular 원문·적용 국경·시행일을 확인하고 통관 SOP 및 시스템 변경사항을 Gap 분석"
    if fam == "ORIGIN_TRANSshipment":
        return "베트남 생산품의 BOM·원산지·제조공정·선적경로를 연결한 Origin Traceability 증빙 점검"
    return s(row.get("Action Plan")) or "원문·적용범위·시행일을 확인하고 관련 법인 영향도를 재판정"


def esc(v) -> str:
    return html.escape(s(v))


def build_html(rows: pd.DataFrame, run_date: str) -> str:
    priority = select_priority(rows, 3)
    direct_n = int(direct_confirmed_mask(rows).sum())
    urgent_n = int(rows.get("DecisionStatus", pd.Series(dtype=str)).eq("Urgent Verification").sum())
    scenario_n = int(rows.get("DecisionStatus", pd.Series(dtype=str)).eq("Scenario Analysis").sum())
    monitoring_n = int(rows.get("DecisionStatus", pd.Series(dtype=str)).eq("Monitoring").sum())
    pending_n = int(rows.get("DecisionStatus", pd.Series(dtype=str)).eq("Verification Pending").sum())
    cards = []
    for i, (_, r) in enumerate(priority.iterrows(), 1):
        badge = esc(r.get("DecisionStatus"))
        cards.append(f"""
        <div class='card'><div class='badge'>{badge}</div>
        <h3>{i}. {esc(r.get('Headline'))}</h3>
        <p><b>임원 판단</b> {esc(r.get('ContractReason'))}</p>
        <p><b>삼성전자 관세업무</b> {esc(r.get('AI Analysis')) or '직접 비용은 미확정이며 적용범위 검증이 필요합니다.'}</p>
        <p><b>지시사항</b> {esc(action_text(r))}</p>
        <p class='meta'><b>원본 게시일자</b> {esc(row_publication_date(r))} · {esc(r.get('Country'))} · {esc(r.get('Agency') or r.get('Source'))} · <a href='{esc(r.get('URL'))}'>원문</a></p></div>""")
    if not cards:
        cards.append("<div class='empty'><b>신규 Action Queue 없음</b><br>Direct 확정 또는 원문 검증을 통과한 우선 검토대상이 없습니다.</div>")

    def table_rows(frame: pd.DataFrame) -> str:
        rendered = []
        for _, r in frame.iterrows():
            original_date = row_publication_date(r)
            rendered.append(f"<tr><td>{esc(r.get('DecisionStatus'))}</td><td>{esc(original_date)}</td><td><a href='{esc(r.get('URL'))}'>{esc(r.get('Headline'))}</a></td><td>{esc(r.get('Country'))}</td><td>{esc(r.get('ContractReason'))}</td></tr>")
        return "".join(rendered)

    verified = rows[rows.get("VerificationStatus", pd.Series("PENDING", index=rows.index)).eq("VERIFIED")]
    pending = rows[rows.get("VerificationStatus", pd.Series("PENDING", index=rows.index)).ne("VERIFIED")]
    return f"""<!doctype html><html><head><meta charset='utf-8'><style>
    body{{font-family:'SamsungOne-400','SamsungOne',Arial,'Malgun Gothic',sans-serif;color:#172033;max-width:980px;margin:auto;padding:24px;background:#f5f7fb}}
    header,.section{{background:white;border-radius:12px;padding:22px;margin-bottom:14px}} h1{{margin:0;color:#123b70;font-size:16pt}} h2{{color:#123b70;font-size:14pt}}
    .lead{{font-size:17px;line-height:1.65;border-left:5px solid #1d63b7;padding:12px 16px;background:#eef5ff}}
    .metric{{display:inline-block;margin-right:18px;font-weight:bold}} .card{{border:1px solid #dbe4ef;border-radius:10px;padding:16px;margin:12px 0}}
    .badge{{display:inline-block;background:#e8f1ff;color:#174f91;border-radius:10px;padding:4px 9px;font-size:12px}} .meta{{font-size:12px;color:#687386}}
    .pending{{border-left:5px solid #d79b00}} .verified{{border-left:5px solid #2b8a3e}}
    table{{width:100%;border-collapse:collapse}} th,td{{padding:9px;border-bottom:1px solid #e4e8ef;text-align:left;font-size:13px}} .empty{{padding:18px;background:#f7f8fa}}
    </style></head><body><header><h1>[GTI Radar] Global Trade Intelligence</h1><p>{run_date} | Samsung Electronics Customs Executive Brief</p></header>
    <section class='section'><h2>1. 오늘의 관세정책 센싱</h2><p class='lead'><b>{esc(executive_sentence(rows))}</b></p>
    <span class='metric'>보고 {len(rows)}건</span><span class='metric'>Action Required {direct_n}건</span><span class='metric'>Urgent Verification {urgent_n}건</span><span class='metric'>Scenario {scenario_n}건</span><span class='metric'>Monitoring {monitoring_n}건</span><span class='metric'>Verification Pending {pending_n}건</span></section>
    <section class='section'><h2>2. Samsung Customs Action Queue</h2>{''.join(cards)}</section>
    <section class='section verified'><h2>3. 원문 검증 완료 ({len(verified)}건)</h2><table><tr><th>상태</th><th>원본 게시일자</th><th>정책 신호</th><th>국가</th><th>선정 근거</th></tr>{table_rows(verified)}</table></section>
    <section class='section pending'><h2>4. 원문 확인 필요 ({len(pending)}건)</h2><table><tr><th>상태</th><th>원본 게시일자</th><th>정책 신호</th><th>국가</th><th>확인 사유</th></tr>{table_rows(pending)}</table></section>
    <section class='section'><small>정책 존재는 기사 원문·공식출처로만 판정하며 AI 분석문은 증거로 사용하지 않습니다. Contract {CONTRACT_VERSION}</small></section></body></html>"""


def write_xlsx(path: Path, rows: pd.DataFrame) -> None:
    top3 = select_priority(rows, 3)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        rows.to_excel(writer, sheet_name="Executive Radar", index=False)
        top3.to_excel(writer, sheet_name="Priority Watch Top3", index=False)
        if rows.empty:
            pd.DataFrame({"Message": ["금일 신규 핵심정책 없음"]}).to_excel(writer, sheet_name="Run Summary", index=False)
        else:
            pd.DataFrame({"Metric": ["Selected", "Action Required", "Urgent Verification", "Scenario Analysis", "Monitoring", "Verification Pending", "Contract"], "Value": [len(rows), int(direct_confirmed_mask(rows).sum()), int(rows["DecisionStatus"].eq("Urgent Verification").sum()), int(rows["DecisionStatus"].eq("Scenario Analysis").sum()), int(rows["DecisionStatus"].eq("Monitoring").sum()), int(rows["DecisionStatus"].eq("Verification Pending").sum()), CONTRACT_VERSION]}).to_excel(writer, sheet_name="Run Summary", index=False)
        for ws in writer.book.worksheets:
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions
            ws.row_dimensions[1].height = 24
            for cell in ws[1]:
                cell.font = __import__("openpyxl").styles.Font(bold=True, color="FFFFFF")
                cell.fill = __import__("openpyxl").styles.PatternFill("solid", fgColor="1F4E78")
            for col in ws.columns:
                letter = col[0].column_letter
                ws.column_dimensions[letter].width = min(55, max(12, max(len(s(c.value)) for c in col) + 2))


# ---------------------------------------------------------------------------
# v50 fixed-form executive report contract
# ---------------------------------------------------------------------------
ENGINE_VERSION = "v52.0 EVIDENCE-FIRST EXECUTIVE BRIEF"
HEALTH_FILE = BASE / "1.site_crawl_health.xlsx"


def yn(df: pd.DataFrame, col: str) -> pd.Series:
    return df.get(col, pd.Series("N", index=df.index)).fillna("N").astype(str).str.upper().eq("Y")


def text_blob(df: pd.DataFrame) -> pd.Series:
    cols = [c for c in ["Headline", "Summary", "AI Analysis", "Issue", "Country", "Agency", "Source"] if c in df]
    if not cols:
        return pd.Series("", index=df.index)
    out = pd.Series("", index=df.index)
    for col in cols:
        out = out + " " + df[col].fillna("").astype(str)
    return out.str.lower()


def _row_native_text(row: pd.Series) -> str:
    """Article-native text only; AI Summary/Analysis must not create relevance."""
    body = s(row.get("Article Body Evidence"))
    # Remove recommendation/navigation tails. They frequently contain an
    # unrelated Samsung headline and previously manufactured Watch relevance.
    body = re.split(
        r"많이\s*본\s*뉴스|인기\s*기사|추천\s*기사|"
        r"related\s+(?:news|articles)|most\s+read|copyright\s*©",
        body, maxsplit=1, flags=re.I,
    )[0]
    return " ".join([
        s(row.get("Headline")), body, s(row.get("Country")),
        s(row.get("Agency")), s(row.get("Source")),
    ]).lower()


def _canonical_policy_family(row: pd.Series) -> str:
    """Stable event identity across publishers and Korean/English headlines."""
    t = _row_native_text(row)
    if re.search(r"코트라|kotra", t, re.I) and re.search(r"관세\s*환급|tariff\s*refund", t, re.I) and re.search(r"5244|5천\s*200|5200|50여\s*개사|48개사|50개사", t, re.I):
        return "KR_KOTRA_US_TARIFF_REFUND_2026"
    if "cptpp" in t and re.search(r"가입|보류|제동|무산|지연|bid|membership", t, re.I):
        return "KR_CPTPP_MEMBERSHIP_DELAY_2026"
    if re.search(r"중국|china|chinese", t, re.I) and re.search(r"eu|유럽연합", t, re.I) and re.search(r"하이브리드|hybrid", t, re.I) and re.search(r"수출\s*(?:제한|축소|조절)|limit.*export|halve.*import", t, re.I):
        return "CN_EU_HYBRID_EXPORT_LIMIT_2026"
    if re.search(r"러시아|russia", t, re.I) and re.search(r"경유|diesel", t, re.I) and re.search(r"트럼프|trump|미국|u\.s\.", t, re.I):
        return "US_RUSSIA_DIESEL_DEAL_2026"
    if re.search(r"lg전자|엘지전자|lg electronics", t, re.I) and re.search(r"인도|india", t, re.I) and re.search(r"로열티|royalt", t, re.I) and re.search(r"관세|customs|duty|과세", t, re.I):
        return "IN_LG_ELECTRONICS_ROYALTY_CUSTOMS_ASSESSMENT"
    if re.search(r"삼성전자|samsung electronics", t, re.I) and re.search(r"인도|india", t, re.I) and re.search(r"oled|디스플레이", t, re.I) and re.search(r"관세|customs|duty|추징|조사", t, re.I):
        return "IN_SAMSUNG_OLED_CUSTOMS_CASE"
    if re.search(r"필리핀|philippines", t, re.I) and re.search(r"eu|유럽연합", t, re.I) and re.search(r"fta|자유무역협정", t, re.I):
        return "EU_PHILIPPINES_FTA_NEGOTIATION_2026"
    if re.search(r"eu\s*customs\s*reform|eu\s*관세\s*개혁|통합\s*관세\s*코드|union\s*customs\s*code", t, re.I):
        return "EU_CUSTOMS_REFORM_EFFECTIVE_2026"
    if re.search(r"변압기|transformer", t, re.I) and re.search(r"철심|코어|core", t, re.I) and re.search(r"세이프가드|safeguard", t, re.I):
        return "EU_TRANSFORMER_CORE_SAFEGUARD_2026"
    if re.search(r"신고\s*포상금", t) and re.search(r"상한|내부\s*신고|누진형", t):
        return "KR_CUSTOMS_REPORTING_REWARD_REFORM_2026"
    if "cptpp" in t and re.search(r"제주|농어민|농수산", t):
        return "KR_CPTPP_JEJU_AGRI_OPPOSITION_2026"
    if "이차전지" in t and re.search(r"우회\s*수출|불법\s*수출|4800억|4천800억", t):
        return "KR_BATTERY_TRANSSHIPMENT_ENFORCEMENT_2026"
    rules = [
        ("US_RUSSIA_IRAN_SECONDARY_TARIFF_100", [
            ["러시아", "russia"], ["이란", "iran", "원유", "oil", "natural gas"],
            ["100%", "100％", "secondary tariff", "제재법", "sanctions act"],
        ]),
        ("KR_US_INVESTMENT_301_232_COST", [
            ["대미투자", "미국 투자", "u.s. investment", "us investment"],
            ["301조", "section 301", "232조", "section 232", "관세"],
            ["중간재", "자본재", "설비", "equipment", "capital goods"],
        ]),
        ("US_CHINA_SUMMIT_TARIFF_NEGOTIATION", [
            ["트럼프", "미국", "u.s."], ["시진핑", "중국", "china"],
            ["정상회담", "회담", "summit", "관세 담판", "tariff negotiation"],
        ]),
        ("US_SEMICON_TARIFF_INVESTMENT_PRESSURE", [
            ["반도체", "semiconductor", "chip"], ["100%", "100％", "관세", "tariff"],
            ["미국 투자", "현지 생산", "local production", "invest"],
        ]),
        ("VN_STRATEGIC_EXPORT_CONTROL", [
            ["베트남", "vietnam"], ["수출통제", "전략물자", "export control"],
            ["hs코드", "hs code", "기술사양", "재수출", "환적", "re-export", "transshipment"],
        ]),
    ]
    for key, groups in rules:
        if all(any(term in t for term in group) for group in groups):
            return key
    existing = s(row.get("EventKey")) or s(row.get("Cluster"))
    if existing and not existing.upper().startswith("AUTO_"):
        return existing.casefold()
    title = re.sub(r"[^0-9a-z가-힣%]+", " ", s(row.get("Headline")).lower())
    return "title:" + " ".join(title.split()[:14])


def _context_noise(row: pd.Series) -> bool:
    """Remove non-policy/background noise from the visible report, not Excel audit."""
    t = _row_native_text(row)
    title = s(row.get("Headline")).lower()
    samsung_named = bool(re.search(
        r"삼성전자|삼성sdi|samsung electronics|samsung semiconductor|samsung sdi", t, re.I
    ))
    current_measure = bool(re.search(
        r"시행|발효|부과|인상|인하|철회|폐지|개정|조사\s*개시|예비판정|최종판정|"
        r"입법예고|행정예고|고시|공고|명령|지정|금지|강화|완화|"
        r"effective|entered into force|imposed|amended|investigation initiated|"
        r"preliminary determination|final determination|official notice|executive order",
        t, re.I,
    ))
    hard_noise = bool(re.search(
        r"합성니코틴|유사니코틴|마운자로|위고비|농축산물|농가경제|일본\s*농업|"
        r"쌀|사과|유전자\s*변형\s*감자|고용보험|워크셰어링|benefit-processing|"
        r"service canada|현대차|자동차\s*문제|car\s+problem|jewelers|보석|다이아몬드",
        title, re.I,
    ))
    executive_or_generic_ai = bool(re.search(
        r"임원\s*(?:조기\s*)?인사|사장단\s*인사|리더십\s*개편|사업부장\s*겸직|"
        r"ai\s*(?:세이프가드|답변\s*오류|안전|에이전트)", title, re.I,
    ))
    agriculture_enforcement = bool(re.search(
        r"농식품|농축산물|농수산물|포도|소고기|돼지|과일|두리안|원산지\s*위반", t, re.I,
    )) and not samsung_named
    old_policy_rehash = bool(re.search(
        r"202[0-5]년.{0,30}(?:시행|발효|부과|개정)|과거.{0,30}(?:정책|관세|규제)|"
        r"historical|since\s+202[0-5]|in\s+202[0-5]", t, re.I,
    )) and bool(re.search(r"과거|재해석|회고|역사|rehash|historical", title, re.I)) \
       and not bool(re.search(r"2026년?.{0,40}(?:시행|발효|부과|개정|조사\s*개시|판정)", t, re.I))
    opinion_or_history = bool(re.search(
        r"paul krugman|why does everyone hate|\[칼럼|\[기고|\[사설|오피니언|"
        r"시각과\s*전망|cptpp.{0,30}무조건\s*가야|관영매체|"
        r"보호무역주의\s*안돼|촉구|주장|"
        r"1980년대|1990년대|역사적\s*배경|historical background",
        title, re.I,
    ))
    unrelated_enforcement = bool(re.search(
        r"k-?뷰티|화장품|명품|농식품|농축산물|농수산물", t, re.I
    )) and bool(re.search(r"짝퉁|위조상품|단속|적발|원산지", t, re.I))
    employment_support = "tariff support" in t and not current_measure
    non_policy_story = bool(re.search(
        r"결혼|부고|무역흑자|무역적자|수출\s*급증|수출\s*물류|인터뷰|국감장|"
        r"현장조사\s*거부|incoming passengers|duty-not-paid cigarettes|여행객|담배",
        title, re.I,
    ))
    negotiation_only = bool(re.search(
        r"협상|negotiation|talks", title, re.I,
    )) and not bool(re.search(
        r"타결|서명|발효|공식\s*합의|concluded|signed|entered into force", title, re.I,
    ))
    implementation_check_only = bool(re.search(
        r"이행\s*(?:상황\s*)?점검|상황\s*점검|implementation\s+review", title, re.I,
    )) and not bool(re.search(r"고시|개정|시행|제정|부과|금지|official notice|amended|effective", title, re.I))
    absolute_noise = executive_or_generic_ai or agriculture_enforcement or unrelated_enforcement
    conditional_noise = (
        hard_noise or old_policy_rehash or opinion_or_history or employment_support
        or non_policy_story or negotiation_only or implementation_check_only
    ) and not samsung_named
    return absolute_noise or conditional_noise


def _samsung_watch_scope(row: pd.Series) -> bool:
    """Require a plausible Samsung customs route, not a product buzzword alone."""
    t = _row_native_text(row)
    samsung_named = bool(re.search(
        r"삼성전자|삼성sdi|samsung electronics|samsung semiconductor|samsung sdi",
        t, re.I
    ))
    customs = bool(re.search(
        r"관세|통관|품목분류|원산지|수출통제|전략물자|반덤핑|상계관세|cbam|"
        r"tariff|customs|hs\s*code|rules of origin|export control|anti-dumping|countervailing",
        t, re.I,
    ))
    electronics = bool(re.search(
        r"반도체|semiconductor|메모리|chip|스마트폰|smartphone|휴대폰|display|oled|"
        r"가전|전자제품|electronics|lg전자|엘지전자|sk하이닉스|sk hynix|tsmc|asml|"
        r"네트워크장비|배터리|battery",
        t, re.I,
    ))
    specific_product = bool(re.search(
        r"\blfp\b|\bncm\d*\b|\bess\b\s*(?:용)?\s*배터리|에너지저장장치|\bbess\b|oled|hbm|dram|nand|"
        r"냉장고|세탁기|에어컨|tv|스마트폰|통신장비|네트워크장비|"
        r"반도체\s*(?:장비|소자|칩|메모리)|hs\s*(?:84|85)\d{2,}", t, re.I
    ))
    route = bool(re.search(
        r"미국\s*투자|현지\s*생산|한국.{0,20}미국|베트남|vietnam|재수출|환적|"
        r"수입|수출|import|export|re-export|transshipment|local production",
        t, re.I,
    ))
    peer_case = bool(re.search(
        r"lg전자|엘지전자|lg electronics|sk하이닉스|sk hynix|tsmc|asml|인텔|intel", t, re.I
    )) and electronics and bool(re.search(
        r"관세청|세관|관세당국|조세심판원|법원|tribunal|court|customs", t, re.I
    )) and bool(re.search(
        r"조사\s*개시|추징|과세\s*처분|심판|판결|재결|분류\s*결정|assessment|duty demand|appeal|ruling|judgment", t, re.I
    ))
    mapped_entity = s(row.get("MappedEntity"))
    samsung_mapped = bool(re.search(
        r"samsung|삼성|sea\b|ssi\b|semiconductor inc", mapped_entity, re.I
    ))
    mapped = (
        s(row.get("EntityDirectFlag")).upper() == "Y"
        and samsung_mapped
        and bool(s(row.get("MappedHS")) or s(row.get("TradeRoute")))
    )
    ai_relevant = s(row.get("AIRelevant")).upper() == "Y"
    headline = s(row.get("Headline")).lower()
    headline_customs_product = bool(re.search(
        r"관세|통관|품목분류|수출통제|tariff|customs|export control", headline, re.I
    )) and bool(re.search(
        r"반도체|배터리|\bbess\b|\bess\b|oled|hbm|dram|nand|가전|스마트폰|"
        r"semiconductor|battery|electronics|appliance", headline, re.I
    ))
    explicit_electronics_link = electronics and (samsung_named or bool(re.search(
        r"lg전자|엘지전자|lg electronics|sk하이닉스|sk hynix|tsmc|asml|"
        r"인텔|intel|마이크론|micron|키옥시아|kioxia|애플|apple", t, re.I
    )))
    # CustomsCaseFlag alone is never sufficient. Watch needs article-native
    # Samsung/peer electronics evidence or a complete Samsung transaction map.
    article_product_gate = headline_customs_product or (
        route and (explicit_electronics_link or specific_product) and ai_relevant
    )
    return customs and ((peer_case and ai_relevant) or mapped or article_product_gate)


def _direct_samsung_customs_case(row: pd.Series) -> bool:
    """True only for an article-native, verified action aimed at Samsung.

    A policy amendment is not required.  The trigger is an actual customs or
    export-control investigation, assessment, ruling, seizure or licence
    decision whose subject is Samsung itself.
    """
    t = _row_native_text(row)
    samsung = bool(re.search(
        r"삼성전자|삼성sdi|samsung electronics|samsung semiconductor|samsung sdi",
        s(row.get("Headline")), re.I
    )) or bool(re.search(
        r"samsung|삼성", s(row.get("MappedEntity")), re.I
    ))
    authority = bool(re.search(
        r"관세청|세관|관세당국|조세심판원|법원|상무부|산업안보국|customs|tribunal|court|department of commerce|\bbis\b", t, re.I
    ))
    concrete_action = bool(re.search(
        r"조사\s*개시|조사\s*착수|세무조사|심사\s*통지|추징\s*통지|과세\s*처분|추징|압수|적발|"
        r"품목분류\s*(?:결정|변경)|사전심사\s*결정|수출허가\s*거부|수출\s*금지|제재\s*대상|"
        r"investigation initiated|customs investigation|assessment notice|duty demand|seizure|"
        r"classification ruling|advance ruling|license denial|export ban|sanctioned", t, re.I
    ))
    speculative = bool(re.search(
        r"가능성|우려|전망|예상|검토할\s*수|may|could|might|potential", t, re.I
    )) and not concrete_action
    body_verified = s(row.get("Body Verified")).upper() == "Y"
    declared = s(row.get("DirectCustomsCaseFlag")).upper() == "Y"
    return body_verified and samsung and authority and concrete_action and not speculative and (declared or concrete_action)


def _official_evidence(row: pd.Series) -> bool:
    """Official evidence must be a verified original, not a media claim."""
    source_flag = s(row.get("OfficialSourceFlag")).upper() == "Y"
    source_status = s(row.get("OfficialSourceStatus")).upper() in {
        "VERIFIED", "VERIFIED_EXACT", "OFFICIAL_VERIFIED",
    }
    official_document = s(row.get("OfficialDocumentVerified")).upper() == "Y"
    regulation_body = (
        s(row.get("Content Type")).lower() == "regulation"
        and s(row.get("Body Verified")).upper() == "Y"
    )
    return bool((source_flag and (source_status or official_document)) or regulation_body)


def _final_policy_stage(row: pd.Series) -> bool:
    stage = s(row.get("Policy Stage")).upper()
    return stage in {
        "ENACTED", "EFFECTIVE", "FINAL", "IMPLEMENTED", "OPERATIVE",
        "FINAL_RULE", "FINAL_DETERMINATION",
    }


def _transaction_evidence(row: pd.Series) -> bool:
    """Require an actual Samsung entity plus product/HS and a trade route."""
    entity = bool(s(row.get("MappedEntity")))
    product_or_hs = bool(s(row.get("MappedProduct")) or s(row.get("MappedHS")))
    route = bool(s(row.get("TradeRoute")))
    transaction_flag = s(row.get("SamsungTransactionMapped")).upper() == "Y"
    return transaction_flag and entity and product_or_hs and route


def _action_defined(row: pd.Series) -> bool:
    plan = s(row.get("Action Plan"))
    declared = s(row.get("RequiredActionDefined")).upper() == "Y"
    owner = bool(re.search(r"owner\s*[:：]|담당(?:조직)?\s*[:：]|hq customs|법인", plan, re.I))
    deadline = bool(re.search(
        r"\d+\s*(?:영업일|일|주|개월)\s*(?:내|이내)|즉시|당일|금일|상시|기한|deadline|within\s+\d+",
        plan, re.I,
    ))
    return declared and has_substantive_text(plan, min_chars=35) and owner and deadline


def _strict_action_gate(row: pd.Series) -> bool:
    return all((
        s(row.get("PolicyDeltaFlag")).upper() == "Y",
        _official_evidence(row),
        _final_policy_stage(row) or _direct_samsung_customs_case(row),
        _transaction_evidence(row),
        _action_defined(row),
    ))


def _business_policy_change(row: pd.Series) -> bool:
    """Keep real customs-policy changes; reject commentary and unrelated sectors."""
    t = _row_native_text(row)
    headline = s(row.get("Headline"))
    body = s(row.get("Article Body Evidence"))[:1800]
    decision_text = f"{headline} {body}"
    declared_delta = s(row.get("PolicyDeltaFlag")).upper() == "Y"
    ai_field_present = "AIRelevant" in row.index and bool(s(row.get("AIRelevant")))
    ai_relevant = s(row.get("AIRelevant")).upper() == "Y"
    change_signal = bool(re.search(
        r"시행|발효|공포|개정|제정|부과|인상|인하|철회|폐지|조사\s*개시|"
        r"예비판정|최종판정|입법예고|행정예고|고시|명령|수입\s*금지|수출\s*금지|"
        r"effective|entered into force|enacted|amended|imposed|revoked|"
        r"investigation initiated|preliminary determination|final determination|official notice",
        decision_text, re.I,
    ))
    commentary = bool(re.search(
        r"\[칼럼\]|\[기고\]|\[사설\]|오피니언|전망|주가|실적|수혜주|"
        r"왜 .*관세|could|might|may impose|opinion|editorial",
        headline, re.I,
    ))
    policy_domain = bool(re.search(
        r"관세(?!사)|통관|품목분류|hs\s*(?:code|코드)|원산지|fta|자유무역협정|"
        r"반덤핑|상계관세|세이프가드|무역구제|수출통제|전략물자|수입승인|수출승인|"
        r"tariff|customs|rules of origin|anti-dumping|countervailing|safeguard|"
        r"export control|import approval|trade remedy",
        decision_text, re.I,
    ))
    unrelated = bool(re.search(
        r"가상자산\s*압류|가축전염병|농축어업|농어민|타이어|유정관|풍력타워|"
        r"석유|원유|소고기|식용유|담배|전자담배|의약품|바이오|철강|강관|"
        r"항만운송사업|면세점|여행자|마약류|여권|비자",
        t, re.I,
    ))
    samsung_electronics = bool(re.search(
        r"삼성전자|samsung electronics|반도체|semiconductor|디스플레이|oled|"
        r"스마트폰|smartphone|가전|전자부품|네트워크장비|배터리|battery|"
        r"hs\s*85|hs\s*84",
        decision_text, re.I,
    ))
    # STEP4 can conservatively leave PolicyDeltaFlag=N when a media article
    # describes a concrete enactment/notice.  Keep that candidate in Context
    # (never Action) when the article-native wording itself proves a change.
    delta = declared_delta or (change_signal and not commentary)
    return (
        delta and change_signal and policy_domain and not commentary
        and (not unrelated or samsung_electronics)
        and (declared_delta or not ai_field_present or ai_relevant)
    )


def _global_context_candidate(row: pd.Series) -> bool:
    """Retain verified current customs/trade events without inventing impact.

    Global Context is intentionally broader than Core Policy.  It may contain
    a body-verified enforcement, investigation, ruling, consultation or
    implementation event even while the exact official source or Samsung
    transaction mapping remains pending.  Commentary, historical background
    and unrelated-sector noise remain excluded by _context_noise.
    """
    if _business_policy_change(row):
        return True
    t = _row_native_text(row)
    body_verified = s(row.get("Body Verified")).upper() == "Y"
    policy_domain = bool(re.search(
        r"관세(?!사)|통관|세관|품목분류|hs\s*(?:code|코드)|원산지|fta|"
        r"반덤핑|상계관세|세이프가드|무역구제|수출통제|전략물자|"
        r"tariff|customs|rules of origin|anti-dumping|countervailing|"
        r"safeguard|export control|trade remedy", t, re.I,
    ))
    current_event = bool(re.search(
        r"시행|발효|공포|개정|제정|부과|인상|인하|철회|폐지|조사\s*개시|"
        r"예비판정|최종판정|입법예고|행정예고|고시|명령|강화|완화|단속|적발|판결|재결|"
        r"effective|entered into force|enacted|amended|imposed|revoked|"
        r"investigation initiated|preliminary determination|final determination|"
        r"official notice|ruling|judgment|enforcement", t, re.I,
    ))
    headline_event = bool(re.search(
        r"시행|발효|공포|개정|제정|부과|인상|인하|철회|폐지|조사\s*개시|"
        r"예비판정|최종판정|고시|명령|단속|적발|판결|재결|"
        r"effective|entered into force|enacted|amended|imposed|revoked|investigation initiated|"
        r"preliminary determination|final determination|official notice|ruling|judgment|enforcement",
        s(row.get("Headline")), re.I,
    ))
    ai_field_present = "AIRelevant" in row.index and bool(s(row.get("AIRelevant")))
    ai_relevant = s(row.get("AIRelevant")).upper() == "Y"
    declared_delta = s(row.get("PolicyDeltaFlag")).upper() == "Y"
    return (
        body_verified and policy_domain and current_event
        and (declared_delta or headline_event)
        and (not ai_field_present or ai_relevant)
    )


def _collapse_policy_events(d: pd.DataFrame) -> pd.DataFrame:
    if d.empty:
        return d
    out = d.copy()
    out["_quality_family"] = out.apply(_canonical_policy_family, axis=1)
    out["_official_rank2"] = (
        out.get("OfficialSourceStatus", pd.Series("", index=out.index))
        .fillna("").astype(str).str.upper().eq("VERIFIED_EXACT").astype(int)
    )
    out["_body_rank2"] = yn(out, "Body Verified").astype(int)
    out = out.sort_values(
        ["_quality_family", "_official_rank2", "_body_rank2", "_score"],
        ascending=[True, False, False, False], kind="stable",
    )
    kept = []
    for _, group in out.groupby("_quality_family", sort=False, dropna=False):
        rep = group.iloc[0].copy()
        headlines = [s(x) for x in group.get("Headline", pd.Series(dtype=str)) if s(x)]
        urls = [s(x) for x in group.get("URL", pd.Series(dtype=str)) if s(x)]
        duplicate_values = group.get(
            "EventDuplicateCount", pd.Series(0, index=group.index, dtype="int64")
        )
        rep["EventDuplicateCount"] = max(
            int(pd.to_numeric(duplicate_values, errors="coerce").fillna(0).sum()),
            len(group) - 1,
        )
        rep["AlternateHeadlines"] = " | ".join(dict.fromkeys(headlines))
        rep["AlternateURLs"] = " | ".join(dict.fromkeys(urls))
        kept.append(rep)
    return pd.DataFrame(kept).drop(columns=["_official_rank2", "_body_rank2"], errors="ignore")


def classify_report_layers(rows: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Apply the immutable report hierarchy without forced filling.

    Action Queue requires all three gates. Core policy requires current delta
    plus official evidence. Samsung Watch holds business-relevant candidates
    with one or more gates pending. Remaining valid policy context is compact.
    """
    if rows.empty:
        empty = rows.copy()
        return empty, {k: empty.copy() for k in ["action", "core", "watch", "context", "excluded"]}
    d = rows.copy()
    for col, default in [
        ("Content Type", "News"), ("ReportLayer", "REFERENCE"),
        ("PolicyDeltaFlag", "N"), ("EvidenceGateFlag", "N"),
        ("SamsungTradeGateFlag", "N"), ("DirectConfirmedFlag", "N"),
        ("Body Verified", "N"), ("ContractReason", ""), ("DecisionStatus", "Monitoring"),
    ]:
        if col not in d: d[col] = default
    raw_score = d.get("ExecutiveScore", d.get("Importance Score", pd.Series(0, index=d.index)))
    if not isinstance(raw_score, pd.Series):
        raw_score = pd.Series(raw_score, index=d.index)
    d["_score"] = pd.to_numeric(raw_score, errors="coerce").fillna(0)
    blob = text_blob(d)
    native = pd.Series("", index=d.index)
    # Samsung scope must come from article-native evidence. AI summaries often
    # mention Samsung hypothetically and must never create relevance by itself.
    for col in ["Headline", "Direct Evidence", "Article Body Evidence", "Country", "Agency"]:
        if col in d: native = native + " " + d[col].fillna("").astype(str)
    native = native.str.lower()
    reg = d["Content Type"].astype(str).str.lower().eq("regulation")
    delta = yn(d, "PolicyDeltaFlag")
    evidence = yn(d, "EvidenceGateFlag")
    body = yn(d, "Body Verified")
    trade = yn(d, "SamsungTradeGateFlag")

    # A regulation listing is not automatically a material policy change.
    # The original body still has to prove a new enactment/amendment/notice.
    official_mask = d.apply(_official_evidence, axis=1)
    evidence = evidence & official_mask
    d.loc[~official_mask, "EvidenceGateFlag"] = "N"

    customs_operation = blob.str.contains(
        r"외국환거래규정|수출입대금|납세신고\s*정정|수입신고\s*정정|전자문서\s*변경|5fe|5fk|품목번호\s*연계표",
        regex=True, na=False,
    )
    d.loc[customs_operation, "PolicyDeltaFlag"] = "Y"
    delta = delta | customs_operation

    # Defensive event-family merge. The same KCS table or FTA event is often
    # reported by several outlets. Propagate the strongest gate evidence to
    # the family and keep one representative article.
    d["_family"] = d.get("EventKey", pd.Series("", index=d.index)).fillna("").astype(str)
    kcs_family = native.str.contains(r"품목번호\s*연계표|hs code cross-reference|한.?미\s*품목번호", regex=True, na=False) & native.str.contains(r"관세청|korea customs service", regex=True, na=False)
    fta_family = native.str.contains("메르코수르", regex=False, na=False) & native.str.contains("싱가포르", regex=False, na=False) & native.str.contains("fta", regex=False, na=False)
    d.loc[kcs_family, "_family"] = "KR_US_ADDITIONAL_TARIFF_HS_CROSS_REFERENCE"
    d.loc[fta_family, "_family"] = "MERCOSUR_SINGAPORE_FTA_EFFECTIVE"
    for family in ["KR_US_ADDITIONAL_TARIFF_HS_CROSS_REFERENCE", "MERCOSUR_SINGAPORE_FTA_EFFECTIVE"]:
        members = d["_family"].eq(family)
        if not members.any(): continue
        if yn(d.loc[members], "PolicyDeltaFlag").any() or family.startswith("KR_US_"):
            d.loc[members, "PolicyDeltaFlag"] = "Y"
        if yn(d.loc[members], "EvidenceGateFlag").any():
            d.loc[members, "EvidenceGateFlag"] = "Y"
    d["_evidence_rank"] = yn(d, "EvidenceGateFlag").astype(int)
    d["_delta_rank"] = yn(d, "PolicyDeltaFlag").astype(int)
    d = d.sort_values(["_evidence_rank", "_delta_rank", "_score"], ascending=[False, False, False], kind="stable")
    has_family = d["_family"].astype(str).str.strip().ne("")
    family_rows = d[has_family].drop_duplicates("_family", keep="first")
    d = pd.concat([family_rows, d[~has_family]], axis=0).sort_values("_score", ascending=False, kind="stable")
    # Cross-publisher event identity is re-evaluated after STEP4 enrichment.
    # This prevents the same policy from occupying several report rows merely
    # because publishers used different titles or country strings.
    before_event_collapse = len(d)
    d = _collapse_policy_events(d).reset_index(drop=True)
    if len(d) != before_event_collapse:
        print(f"[STEP5 EVENT DEDUP] {before_event_collapse} -> {len(d)} / removed={before_event_collapse-len(d)}")
    blob = text_blob(d)
    native = pd.Series("", index=d.index)
    for col in ["Headline", "Direct Evidence", "Article Body Evidence", "Country", "Agency"]:
        if col in d: native = native + " " + d[col].fillna("").astype(str)
    native = native.str.lower()
    reg = d["Content Type"].astype(str).str.lower().eq("regulation")
    delta = yn(d, "PolicyDeltaFlag")
    evidence = d.apply(_official_evidence, axis=1)
    trade = yn(d, "SamsungTradeGateFlag")
    strict_action = d.apply(_strict_action_gate, axis=1)
    business_change = d.apply(_business_policy_change, axis=1)

    samsung_scope = d.apply(_samsung_watch_scope, axis=1)
    declared_excluded = d["ReportLayer"].astype(str).str.upper().eq("EXCLUDED") | d.get("DecisionStatus", "").astype(str).str.lower().eq("excluded")
    substantive_customs = native.str.contains(
        r"통관지원|통관\s*지원|세관상호지원|세관\s*상호지원|관세협력|관세\s*협력|"
        r"합동단속|합동\s*단속|위조상품|위조\s*상품|국경단계|지식재산권\s*보호|"
        r"customs\s*(?:assistance|cooperation|enforcement)|counterfeit",
        regex=True, na=False,
    )
    administrative_only = native.str.contains(
        r"채용|입찰|교육생\s*모집|설명회\s*개최|행사\s*안내|공고\s*마감|"
        r"recruitment|tender\s+notice|training\s+registration",
        regex=True, na=False,
    ) & ~substantive_customs
    # General customs cooperation is context, not Samsung Watch. Rescue an
    # excluded row only when the strict Samsung scope is independently met.
    strict_samsung_scope_pre = d.apply(_samsung_watch_scope, axis=1)
    rescued_customs = declared_excluded & substantive_customs & strict_samsung_scope_pre
    d.loc[rescued_customs, "ReportLayer"] = "SAMSUNG_WATCH"
    d.loc[rescued_customs, "ContractReason"] = (
        "관세당국 간 통관지원·단속 협력 내용 확인: 삼성전자 관련 품목·법인·적용범위 추가 검증 필요"
    )
    d.loc[rescued_customs, "Missing Facts"] = (
        "공식 MOU·협정 원문 | 대상 품목·법인 | 적용 시점 | 통관지원 연락창구·단속 절차"
    )
    direct_case = d.apply(_direct_samsung_customs_case, axis=1)
    customs_case = yn(d, "CustomsCaseFlag")
    # CustomsCaseFlag is only a case-type hint. It cannot promote a row by
    # itself. Strict article-native Samsung scope is mandatory.
    protected_watch = direct_case | trade | samsung_scope | rescued_customs
    context_noise = d.apply(_context_noise, axis=1)
    excluded_mask = administrative_only | (
        (declared_excluded | context_noise) & ~protected_watch
    )
    # Direct requires official evidence, an actual mapped transaction and an
    # executable owner/deadline. A media report about a Samsung investigation
    # remains Urgent Verification until those gates are closed.
    action_mask = ~excluded_mask & strict_action
    core_mask = ~excluded_mask & ~action_mask & business_change & evidence
    watch_mask = (
        ~excluded_mask & ~action_mask & ~core_mask
        & protected_watch
    )

    action = d[action_mask].sort_values("_score", ascending=False, kind="stable").head(3).copy()
    core = d[core_mask].sort_values("_score", ascending=False, kind="stable").head(5).copy()
    watch = d[watch_mask].sort_values("_score", ascending=False, kind="stable").head(10).copy()
    watch = sort_by_country_and_publish_date(watch)
    used = set(action.index) | set(core.index) | set(watch.index)
    context_eligible = d.apply(_global_context_candidate, axis=1)
    context_candidates = d[
        ~excluded_mask & ~d.index.isin(used) & context_eligible
    ].copy()
    context = context_candidates.sort_values("_score", ascending=False, kind="stable").head(20).copy()
    used |= set(context.index)
    excluded = d[excluded_mask | ~d.index.isin(used)].copy()

    for frame, layer, status, reason in [
        (action, "ACTION_QUEUE", "Action Required", "삼성전자를 직접 대상으로 한 관세·품목분류·수출통제 조치 확인"),
        (core, "CORE_POLICY", "Core Policy", "신규 정책 변화와 공식 근거 확인"),
        (watch, "SAMSUNG_WATCH", "Urgent Verification", "삼성 제품·법인·거래 관련성이 높으나 공식근거 또는 거래 매핑 추가 확인 필요"),
        (context, "GLOBAL_CONTEXT", "Global Context", "삼성 직접 영향은 미확인이나 신규 글로벌 통상정책 흐름으로 관리"),
        (excluded, "EXCLUDED", "Excluded", "리포트 표시 요건 미충족; Excel 감사자료에 보관"),
    ]:
        frame["ReportSection"] = layer
        frame["ReportLayer"] = layer
        frame["DecisionStatus"] = status
        frame["ContractReason"] = reason
    if not core.empty:
        proposed = core.get("Policy Stage", pd.Series("", index=core.index)).fillna("").astype(str).str.upper().isin(
            {"PROPOSAL", "PROPOSED", "PROPOSED_OR_MONITORING", "DRAFT", "CONSULTATION", "FORECAST", "ANNOUNCED"}
        )
        core.loc[proposed, "DecisionStatus"] = "Scenario Analysis"
        core.loc[~proposed, "DecisionStatus"] = "Monitoring"
    if not watch.empty:
        proposed = watch.get("Policy Stage", pd.Series("", index=watch.index)).fillna("").astype(str).str.upper().isin(
            {"PROPOSAL", "PROPOSED", "PROPOSED_OR_MONITORING", "DRAFT", "CONSULTATION", "FORECAST", "ANNOUNCED"}
        )
        watch.loc[proposed, "DecisionStatus"] = "Scenario Analysis"
    visible = pd.concat([action, core, watch, context], ignore_index=True, sort=False)
    helper_cols = ["_score", "_family", "_evidence_rank", "_delta_rank", "_quality_family"]
    visible = visible.drop(columns=helper_cols, errors="ignore")
    layers = {"action": action.drop(columns=helper_cols, errors="ignore"),
              "core": core.drop(columns=helper_cols, errors="ignore"),
              "watch": watch.drop(columns=helper_cols, errors="ignore"),
              "context": context.drop(columns=helper_cols, errors="ignore"),
              "excluded": excluded.drop(columns=helper_cols, errors="ignore")}
    return visible, layers


def concise(v, limit=180) -> str:
    value = s(v)
    return value if len(value) <= limit else value[:limit - 1].rstrip() + "…"


# Evidence-first structured briefing contract. AI-written Summary/Analysis
# may explain a verified fact, but can never create an evidence status.
_UNKNOWN_MARKERS = {
    "", "nan", "none", "n/a", "na", "미확인", "확인 필요", "본문에서 확인 불가",
    "원문에서 확인 불가", "미정", "unknown", "not identified", "not_identified",
}


def known(v) -> bool:
    return s(v).strip().lower() not in _UNKNOWN_MARKERS


def first_known(row: pd.Series, *cols: str, default: str = "확인 필요") -> str:
    for col in cols:
        value = s(row.get(col)).strip()
        if known(value):
            return value
    return default


def evidence_status(row: pd.Series) -> str:
    official_url = first_known(row, "OfficialURL", "OfficialSourceURL", default="")
    source_status = s(row.get("OfficialSourceStatus")).upper()
    body = s(row.get("Body Verified")).upper() == "Y"
    gate = s(row.get("EvidenceGateFlag")).upper() == "Y"
    official = _official_evidence(row) and official_url.startswith(("http://", "https://"))
    if official and (body or gate or source_status in {"VERIFIED", "OFFICIAL", "PRIMARY"}):
        return "VERIFIED"
    if body or known(row.get("Direct Evidence")) or known(row.get("Article Body Evidence")):
        return "SUPPORTED"
    return "PENDING"


def official_link(row: pd.Series) -> str:
    url = first_known(row, "OfficialURL", "OfficialSourceURL", default="")
    if url.startswith(("http://", "https://")):
        return url
    if s(row.get("Content Type")).lower() == "regulation":
        url = s(row.get("URL"))
        if url.startswith(("http://", "https://")):
            return url
    return ""


def analysis_record(row: pd.Series) -> dict[str, str]:
    status = first_known(row, "EvidenceStatus", default="") or evidence_status(row)
    return {
        "evidence": status,
        "effective": first_known(row, "EffectiveDate", "effective_date_hint"),
        "product": first_known(row, "AffectedProduct", "MappedProduct"),
        "hs": first_known(row, "AffectedHS", "MappedHS", "hs_hint"),
        "stage": first_known(row, "PolicyStageDisplay", "Policy Stage", "PolicyDeltaType"),
        "changed": concise(first_known(row, "WhatChanged", "Summary"), 330),
        "impact": concise(first_known(row, "SamsungImpactPath", "AI Analysis"), 360),
        "action": concise(first_known(row, "RecommendedAction", "Action Plan"), 300),
        "missing": concise(first_known(row, "VerificationNeeded", "Missing Facts"), 220),
        "official_url": official_link(row),
    }


def health_summary() -> dict[str, int]:
    result = {"OK_NEW": 0, "NO_NEW": 0, "PARSE_ZERO": 0, "FAIL": 0, "OFFICIAL_PENDING": 0}
    health = read_excel_safe(HEALTH_FILE)
    if health.empty:
        return result
    status_col = next((c for c in ["HealthStatus", "health_status", "final_status", "status", "zero_yield_status"] if c in health), None)
    if not status_col:
        return result
    site_col = next((c for c in [
        "SiteKey", "site_key", "Site ID", "site_id", "Site", "site",
        "SiteName", "site_name", "Agency", "Source", "URL", "url",
    ] if c in health), None)
    time_col = next((c for c in [
        "DiagnosedAt", "diagnosed_at", "CheckedAt", "checked_at",
        "RunDateTime", "run_datetime", "Timestamp", "timestamp",
        "CrawlTime", "crawl_time", "DateTime", "datetime", "Date", "date",
    ] if c in health), None)
    latest = health.copy()
    if site_col:
        latest["_site_key"] = latest[site_col].fillna("").astype(str).str.strip()
        blank = latest["_site_key"].eq("")
        latest.loc[blank, "_site_key"] = [f"__ROW_{i}" for i in latest.index[blank]]
        latest["_row_order"] = range(len(latest))
        if time_col:
            latest["_diagnosed_at"] = pd.to_datetime(latest[time_col], errors="coerce")
            latest = latest.sort_values(
                ["_site_key", "_diagnosed_at", "_row_order"],
                ascending=[True, True, True], na_position="first", kind="stable",
            )
        latest = latest.drop_duplicates("_site_key", keep="last")
    values = latest[status_col].fillna("").astype(str).str.upper().str.strip()
    result["OK_NEW"] = int(values.str.contains(r"^OK|VALID_REGULATION", regex=True).sum())
    result["NO_NEW"] = int(values.str.contains("NO_NEW", regex=False).sum())
    result["PARSE_ZERO"] = int(values.str.contains("PARSE_ZERO|PARTIAL_COVERAGE", regex=True).sum())
    result["FAIL"] = int(values.str.contains("FAIL|BLOCKED|ERROR", regex=True).sum())
    return result


def conclusion_lines(layers: dict[str, pd.DataFrame], health: dict[str, int]) -> list[str]:
    action, core, watch = layers["action"], layers["core"], layers["watch"]
    context = layers["context"]
    new_policy_count = len(action) + len(core) + len(context)
    line1 = f"금일 신규 글로벌 관세·통상 정책 {new_policy_count}건을 확인했습니다."
    line2 = (
        f"삼성전자 거래에 직접 영향을 미치는 확정 정책은 {len(action)}건입니다."
        if len(action)
        else "삼성전자 거래에 직접 영향을 미치는 확정 정책은 없습니다."
    )
    if len(watch):
        headline = concise(watch.iloc[0].get("Headline"), 58)
        line3 = f"긴급 확인 대상 {len(watch)}건 중 우선 검토사항은 ‘{headline}’입니다."
    elif len(core):
        headline = concise(core.iloc[0].get("Headline"), 58)
        line3 = f"공식근거가 확인된 핵심 정책 {len(core)}건 중 우선 검토사항은 ‘{headline}’입니다."
    elif len(context):
        line3 = f"삼성 직접 연결은 미확정이며 글로벌 정책 흐름 {len(context)}건을 모니터링합니다."
    else:
        line3 = "신규 정책 없음과 수집 실패를 구분해 기존 고위험 정책의 변동 여부를 계속 확인합니다."
    return [line1, line2, line3]


def summary_counter(layers: dict[str, pd.DataFrame], health: dict[str, int]) -> str:
    return (f"※ 삼성전자 본·지사 직접 영향 {len(layers['action'])}건, 핵심 정책 {len(layers['core'])}건, "
            f"삼성 관련 확인 후보 {len(layers['watch'])}건, 수집상태 FAIL {health['FAIL']}건, "
            f"PARSE_ZERO {health['PARSE_ZERO']}건, 공식 원문 미확인 {health['OFFICIAL_PENDING']}건.")


def sort_by_country_and_publish_date(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    out = frame.copy()
    out["_country_sort"] = out.get("Country", pd.Series("", index=out.index)).fillna("").astype(str).str.casefold()
    date_values = pd.Series(pd.NaT, index=out.index, dtype="datetime64[ns]")
    # Prefer timestamp-bearing source fields for newest-first ordering. The
    # normalized Original Publish Date may intentionally contain date only.
    for col in ("Publish Date", "Date", "Original Publish Date"):
        if col in out:
            date_values = date_values.fillna(pd.to_datetime(out[col], errors="coerce"))
    out["_date_sort"] = date_values
    return out.sort_values(
        ["_country_sort", "_date_sort"], ascending=[True, False],
        na_position="last", kind="stable",
    ).drop(columns=["_country_sort", "_date_sort"])


def empty_message(label: str) -> str:
    return {
        "action": "금일 삼성전자 거래에 직접 영향과 실행조치가 모두 확인된 정책은 없습니다.",
        "core": "공식근거가 확인된 신규 중요 정책은 없습니다.",
        "watch": "추가 확인이 필요한 삼성전자 관련 관세정책 후보는 없습니다.",
        "context": "보고기준을 충족한 글로벌 관세정책 변화는 없습니다.",
    }[label]


def build_html_v50(layers: dict[str, pd.DataFrame], run_date: str, health: dict[str, int]) -> str:
    action, core, watch, context = (layers[k] for k in ["action", "core", "watch", "context"])
    # Action Queue is not rendered as a permanent empty section.  When a
    # fully confirmed direct-impact case exists, retain its strict action
    # classification but place it first in Core Policy with a clear badge.
    core_display = pd.concat([action, core], ignore_index=True, sort=False).head(5)
    watch = sort_by_country_and_publish_date(watch)
    lines = conclusion_lines(layers, health)
    counter = summary_counter(layers, health)
    def empty(k): return f"<div class='empty'>{esc(empty_message(k))}</div>"
    core_html = []
    for _, r in core_display.iterrows():
        a = analysis_record(r)
        is_direct = s(r.get("ReportSection")).upper() == "ACTION_QUEUE"
        direct_badge = "<span class='badge direct'>직접 영향·즉시 조치</span>" if is_direct else ""
        evidence_badge = f"<span class='badge {a['evidence'].lower()}'>{esc(a['evidence'])}</span>"
        source = a["official_url"] or s(r.get("URL"))
        source_label = "공식 원문" if a["official_url"] else "근거 기사"
        core_html.append(f"""<div class='card'>
        <div>{direct_badge}{evidence_badge}</div>
        <h3><a href='{esc(source)}'>{esc(r.get('Headline'))}</a></h3>
        <p class='meta'>원본 게시일 {esc(row_publication_date(r))} · {esc(r.get('Country'))} · {esc(r.get('Agency') or r.get('Source'))} · <a href='{esc(source)}'>{source_label}</a></p>
        <p><b>무엇이 바뀌었나</b><br>{esc(a['changed'])}</p>
        <div class='facts'><div><b>단계</b><br>{esc(a['stage'])}</div><div><b>시행·적용일</b><br>{esc(a['effective'])}</div><div><b>대상 품목</b><br>{esc(a['product'])}</div><div><b>HS</b><br>{esc(a['hs'])}</div></div>
        <p><b>삼성전자 영향</b><br>{esc(a['impact'])}</p>
        <p><b>권고 조치</b><br>{esc(a['action'])}</p>
        <p class='verify'><b>미확인 사항</b> {esc(a['missing'])}</p></div>""")
    def table(frame, cols):
        heads = "".join(f"<th>{esc(h)}</th>" for h, _ in cols)
        body = []
        for _, r in frame.iterrows():
            cells=[]
            for h,c in cols:
                val = concise(r.get(c), 115)
                if c == "Headline": val=f"<a href='{esc(r.get('URL'))}'>{esc(val)}</a>"
                else: val=esc(val)
                cells.append(f"<td>{val}</td>")
            body.append("<tr>"+"".join(cells)+"</tr>")
        return f"<table><tr>{heads}</tr>{''.join(body)}</table>"
    for frame in (watch, context):
        if not frame.empty:
            frame["_Display Publish Date"] = frame.apply(row_publication_date, axis=1)
    for frame in (watch, context):
        if not frame.empty:
            frame["_Evidence Status"] = frame.apply(evidence_status, axis=1)
            frame["_One Line"] = frame.apply(lambda r: concise(first_known(r, "WhatChanged", "Summary"), 135), axis=1)
            frame["_Samsung Relevance"] = frame.apply(lambda r: concise(first_known(r, "SamsungImpactPath", "AI Analysis"), 135), axis=1)
            frame["_Verify"] = frame.apply(lambda r: concise(first_known(r, "VerificationNeeded", "Missing Facts"), 110), axis=1)
    watch_table = table(watch, [("국가/권역","Country"),("원문 게시일","_Display Publish Date"),("근거","_Evidence Status"),("정책 신호","Headline"),("변화 요약","_One Line"),("삼성 관련성","_Samsung Relevance"),("추가 확인","_Verify")]) if len(watch) else empty("watch")
    context_table = table(context, [("국가/권역","Country"),("원문 게시일","_Display Publish Date"),("정책유형","Issue"),("근거","_Evidence Status"),("정책 동향","Headline"),("변화 요약","_One Line")]) if len(context) else empty("context")
    return f"""<!doctype html><html><head><meta charset='utf-8'><style>
body{{font-family:'SamsungOne-400','SamsungOne',Arial,'Malgun Gothic',sans-serif;color:#172033;max-width:1040px;margin:auto;padding:24px;background:#f5f7fb}}header,.section{{background:#fff;border-radius:12px;padding:22px;margin-bottom:14px}}h1{{margin:0;color:#123b70;font-size:16pt}}h2{{color:#123b70;font-size:14pt}}h3{{margin:10px 0 6px;font-size:16px}}a{{color:#1457a6}}.lead{{font-size:16px;line-height:1.7;border-left:5px solid #1d63b7;padding:12px 16px;background:#eef5ff}}.summary-counter{{font-size:11px;color:#7a828c;line-height:1.55;margin:9px 0 0 16px}}.card{{border:1px solid #dbe4ef;border-radius:10px;padding:16px;margin:12px 0;box-shadow:0 2px 8px rgba(27,55,90,.05)}}.badge{{display:inline-block;margin-right:6px;padding:3px 8px;border-radius:10px;background:#eef1f5;color:#445064;font-size:11px;font-weight:bold}}.badge.verified{{background:#e5f5eb;color:#176b3a}}.badge.supported{{background:#fff2cf;color:#805b00}}.badge.pending{{background:#f4e8e8;color:#8b3434}}.badge.direct{{background:#dcecff;color:#0d4f96}}.facts{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin:10px 0}}.facts div{{background:#f6f8fb;border-radius:7px;padding:9px;font-size:12px}}.meta{{font-size:12px;color:#687386}}.verify{{font-size:12px;color:#6e4c00;background:#fff9e8;padding:8px;border-radius:6px}}table{{width:100%;border-collapse:collapse}}th,td{{padding:8px;border-bottom:1px solid #e4e8ef;text-align:left;font-size:12px;vertical-align:top}}th{{background:#f4f7fb;color:#33455d}}.empty{{padding:16px;background:#f7f8fa;color:#53606f}}@media(max-width:760px){{.facts{{grid-template-columns:repeat(2,minmax(0,1fr))}}}}
</style></head><body><header><h1>[GTI Radar] Global Trade Intelligence</h1><p>{run_date} | Samsung Electronics Customs Executive Brief</p></header>
<section class='section'><h2>1. 요약</h2><div class='lead'>{'<br>'.join(esc(x) for x in lines)}</div><p class='summary-counter'>{esc(counter)}</p></section>
<section class='section'><h2>2. 핵심 정책 분석 ({len(core_display)}건)</h2>{''.join(core_html) if core_html else empty('core')}</section>
<section class='section'><h2>3. Samsung Customs Watch ({len(watch)}건)</h2>{watch_table}</section>
<section class='section'><h2>4. Global Context Radar ({len(context)}건)</h2>{context_table}</section>
<section class='section'><h2>5. 수집·검증 상태</h2><table><tr><th>OK_NEW</th><th>NO_NEW</th><th>PARSE_ZERO</th><th>FAIL</th></tr><tr><td>{health['OK_NEW']}건</td><td>{health['NO_NEW']}건</td><td>{health['PARSE_ZERO']}건</td><td>{health['FAIL']}건</td></tr></table><p class='meta'>공식 원문 미확인 {health['OFFICIAL_PENDING']}건 · 0건과 수집 실패를 구분합니다. 상세 분석·미확인 사항은 첨부 Excel에 보관합니다. Engine {ENGINE_VERSION} · Contract {CONTRACT_VERSION}</p></section></body></html>"""


def write_xlsx_v50(path: Path, visible: pd.DataFrame, layers: dict[str, pd.DataFrame], health: dict[str, int]) -> None:
    core_with_direct = pd.concat([layers["action"], layers["core"]], ignore_index=True, sort=False).head(5)
    if not core_with_direct.empty:
        core_with_direct.insert(0, "Direct Impact", ["Y"] * len(layers["action"]) + ["N"] * min(len(layers["core"]), 5 - len(layers["action"])))
    summary = pd.DataFrame({"Metric": ["Direct Impact","Core Policy","Samsung Watch","Global Context","Total Visible","OK_NEW","NO_NEW","PARSE_ZERO","FAIL","Official Source Pending","Engine","Contract"],
                            "Value": [len(layers['action']),len(layers['core']),len(layers['watch']),len(layers['context']),len(visible),health['OK_NEW'],health['NO_NEW'],health['PARSE_ZERO'],health['FAIL'],health['OFFICIAL_PENDING'],ENGINE_VERSION,CONTRACT_VERSION]})
    brief_rows = []
    for _, r in visible.iterrows():
        a = analysis_record(r)
        brief_rows.append({
            "구분": s(r.get("ReportSection")) or s(r.get("ReportLayer")),
            "원본 게시일": row_publication_date(r), "국가/권역": s(r.get("Country")),
            "정책 신호": s(r.get("Headline")), "근거 상태": a["evidence"],
            "무엇이 바뀌었나": a["changed"], "정책 단계": a["stage"],
            "시행·적용일": a["effective"], "대상 품목": a["product"], "HS": a["hs"],
            "삼성전자 영향": a["impact"], "권고 조치": a["action"],
            "미확인 사항": a["missing"], "공식 원문": a["official_url"], "근거 기사": s(r.get("URL")),
        })
    brief_columns = ["구분", "원본 게시일", "국가/권역", "정책 신호", "근거 상태", "무엇이 바뀌었나",
                     "정책 단계", "시행·적용일", "대상 품목", "HS", "삼성전자 영향", "권고 조치",
                     "미확인 사항", "공식 원문", "근거 기사"]
    brief = pd.DataFrame(brief_rows, columns=brief_columns)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="Executive Summary", index=False)
        brief.to_excel(writer, sheet_name="Executive Brief", index=False)
        core_with_direct.to_excel(writer, sheet_name="Core Policy", index=False)
        layers["watch"].to_excel(writer, sheet_name="Samsung Watch", index=False)
        layers["context"].to_excel(writer, sheet_name="Global Context", index=False)
        visible.to_excel(writer, sheet_name="Details", index=False)
        layers["excluded"].to_excel(writer, sheet_name="Excluded", index=False)
        for ws in writer.book.worksheets:
            ws.freeze_panes = "A2"; ws.auto_filter.ref = ws.dimensions
            for cell in ws[1]:
                cell.font = __import__("openpyxl").styles.Font(bold=True, color="FFFFFF")
                cell.fill = __import__("openpyxl").styles.PatternFill("solid", fgColor="1F4E78")
            for col in ws.columns:
                ws.column_dimensions[col[0].column_letter].width = min(55, max(12, max(len(s(c.value)) for c in col) + 2))
            if ws.title == "Executive Brief":
                for row in ws.iter_rows(min_row=2):
                    for cell in row:
                        cell.alignment = __import__("openpyxl").styles.Alignment(vertical="top", wrap_text=True)
                    ws.row_dimensions[row[0].row].height = 54


def recipients() -> list[str]:
    found = [x.strip() for x in re.split(r"[;,]", os.getenv("GTI_MAIL_TO", "")) if x.strip()]
    if RECIPIENT_FILE.exists():
        try:
            for value in pd.read_excel(RECIPIENT_FILE).astype(str).to_numpy().ravel():
                if re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value.strip()): found.append(value.strip())
        except Exception: pass
    return list(dict.fromkeys(x.lower() for x in found))


def send_mail(body: str, xlsx: Path, run_date: str) -> None:
    to = recipients()
    if not to: raise RuntimeError("MAIL RECIPIENT MISSING: GTI_MAIL_TO or 00.xlsx")
    if not SMTP_USER or not SMTP_PASS: raise RuntimeError("SMTP CREDENTIAL MISSING")
    msg = EmailMessage()
    msg["Subject"] = f"[GTI Radar] Global Trade Intelligence({run_date})"
    msg["From"] = formataddr(("GTI Radar", SMTP_USER)); msg["To"] = ", ".join(to)
    msg.set_content("GTI Radar HTML report and XLSX are attached."); msg.add_alternative(body, subtype="html")
    msg.add_attachment(xlsx.read_bytes(), maintype="application", subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename=xlsx.name)
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ssl.create_default_context(), timeout=30) as server:
        server.login(SMTP_USER, SMTP_PASS); server.send_message(msg)
    print(f"[MAIL SENT] {len(to)} recipients")


def main() -> int:
    ap = argparse.ArgumentParser(); ap.add_argument("--preview", action="store_true"); ap.add_argument("--no-email", action="store_true"); ap.add_argument("--date")
    ap.add_argument("--regulation-input", default=str(REG_FILE)); ap.add_argument("--news-input", default=str(NEWS_FILE)); ap.add_argument("--output-dir", default=str(OUT_DIR))
    args = ap.parse_args()
    # GitHub-hosted Windows runners use UTC.  datetime.now() is naive there
    # and was previously misread as KST, shifting the report window back one
    # extra day and marking all valid prior-KST-day rows as stale.
    now = kst_now()
    run_date = args.date or now.strftime("%Y-%m-%d")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"[GTI Radar] Global Trade Intelligence({run_date})"
    html_path = output_dir / f"{stem}.html"
    xlsx_path = output_dir / f"{stem}.xlsx"
    same_day_rerun = not args.preview and (html_path.exists() or xlsx_path.exists())
    print(f"[INFO] GTI STEP5 {ENGINE_VERSION} START")
    news = read_excel_safe(Path(args.news_input)); reg = read_excel_safe(Path(args.regulation_input))
    news, stale_news = within_24h(news, now)
    # Keep STEP4 exclusions until STEP5 classification. This permits the
    # narrow substantive-customs rescue rule while preserving genuine
    # administrative exclusions in the Excluded audit sheet.
    input_news_count = len(news)
    reg = normalize_regulation(reg)
    reg, stale_reg = within_24h(reg, now)
    stale = pd.concat([stale_news, stale_reg], ignore_index=True, sort=False)
    frames = [x for x in (reg, news) if not x.empty]
    rows = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
    if not rows.empty:
        rows["Original Publish Date"] = rows.apply(row_publication_date, axis=1)
        if "ExecutiveScore" not in rows:
            rows["ExecutiveScore"] = pd.to_numeric(rows.get("Importance Score", 0), errors="coerce").fillna(0)
        rows = rows.sort_values("ExecutiveScore", ascending=False, kind="stable").drop_duplicates("EventKey", keep="first")
    old = pd.DataFrame() if args.preview else read_excel_safe(CUM_FILE)
    if same_day_rerun:
        historical_removed = 0
        print(f"[STEP5 RERUN] existing {run_date} output detected; same-day history suppression bypassed")
    else:
        rows, historical_removed = remove_history(rows, old)
    rows = rows.reset_index(drop=True)
    if not rows.empty:
        summary_ok = rows.apply(lambda r: has_substantive_text(r.get("Summary"), r.get("Headline")), axis=1)
        analysis_ok = rows.apply(lambda r: has_substantive_text(r.get("AI Analysis"), r.get("Headline")), axis=1)
        quality_ok = summary_ok & analysis_ok
        low_quality = rows.loc[~quality_ok].copy()
        if not low_quality.empty:
            print(f"[STEP5 CONTENT GUARD] excluded_title_only={len(low_quality)}")
        rows = rows.loc[quality_ok].reset_index(drop=True)
    visible, layers = classify_report_layers(rows)
    health = health_summary()
    # "Official source pending" means an exact, clickable official document
    # URL is absent. A verified news body is supporting evidence, not an
    # official document, and must not reduce this counter.
    health["OFFICIAL_PENDING"] = int(
        sum(not official_link(r) for _, r in visible.iterrows())
    ) if not visible.empty else 0
    print(f"[STEP5 CONTRACT] news_input={input_news_count} / classified={len(news)} / excluded={len(layers['excluded'])} / stale={len(stale)}")
    print(f"[STEP5 LIVE NOVELTY] removed={historical_removed} / report={len(visible)} / forced_fill=0")
    body = build_html_v50(layers, run_date, health); html_path.write_text(body, encoding="utf-8"); write_xlsx_v50(xlsx_path, visible, layers, health)
    if not args.preview:
        cumulative = pd.concat([old, visible], ignore_index=True, sort=False)
        if not cumulative.empty: cumulative = cumulative.drop_duplicates(["EventKey"], keep="last")
        cumulative.to_excel(CUM_FILE, index=False)
    if not args.preview and not args.no_email: send_mail(body, xlsx_path, run_date)
    else: print("[MAIL SKIP] preview/no-email")
    print(f"[DONE] HTML: {html_path}"); print(f"[DONE] XLSX: {xlsx_path}")
    print(f"[ROWS] total={len(visible)}, action_queue={len(layers['action'])}, core_policy={len(layers['core'])}, samsung_watch={len(layers['watch'])}, global_context={len(layers['context'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
