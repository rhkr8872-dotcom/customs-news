# -*- coding: utf-8 -*-
from __future__ import annotations

import py_compile
import re
import shutil
from datetime import datetime
from pathlib import Path

BASE = Path(r"C:\Temp")
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BACKUP = BASE / f"backup_before_quality_fix_{STAMP}"

FILES = {
    "3-1": BASE / "3-1.regulation_merge.py",
    "3-2": BASE / "3-2.news_merge.py",
    "4-1": BASE / "4-1.regulation_ai_analysis.py",
    "4-2": BASE / "4-2.news_ai_analysis.py",
    "5": BASE / "5.GTI_Mail_Engine.py",
}


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8-sig")


def write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if new in text:
        print(f"[SKIP] 이미 적용됨: {label}")
        return text
    if old not in text:
        raise RuntimeError(f"수정 위치를 찾지 못했습니다: {label}")
    print(f"[PATCH] {label}")
    return text.replace(old, new, 1)


def regex_once(text: str, pattern: str, replacement: str, label: str) -> str:
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.S)
    if count == 0:
        raise RuntimeError(f"수정 위치를 찾지 못했습니다: {label}")
    print(f"[PATCH] {label}")
    return updated


def patch_31(text: str) -> str:
    text = text.replace(
        "GTI v5.4 STEP3-1 REGULATION MERGE START",
        "GTI v5.5 STEP3-1 REGULATION ID DEDUP START",
    )
    text = text.replace(
        "GTI v5.4 STEP3-1 DONE",
        "GTI v5.5 STEP3-1 DONE",
    )

    if "def regulation_number(" not in text:
        marker = "\ndef legal_fingerprint(row: pd.Series) -> str:\n"
        addition = r'''

def regulation_number(v) -> str:
    """관세청고시제2026-50호와 같은 법규번호를 안정적으로 추출한다."""
    t = clean(v).replace(" ", "")
    m = re.search(
        r"(관세청(?:고시|공고|훈령|예규)제?20\d{2}[-–]\d+호)",
        t,
    )
    if m:
        return re.sub(r"[-–]", "-", m.group(1)).lower()

    m = re.search(
        r"([가-힣]{2,20}(?:고시|공고|훈령|예규)제?20\d{2}[-–]\d+호)",
        t,
    )
    return re.sub(r"[-–]", "-", m.group(1)).lower() if m else ""


def clean_gazette_headline(v) -> str:
    """한 관보 제목에 붙은 다른 기관 고시를 제거한다."""
    title = clean(v)
    if "관세청" not in title:
        return title

    m = re.search(
        r"(관세청(?:고시|공고|훈령|예규)제?\s*20\d{2}[-–]\d+호"
        r"\s*\([^)]{3,300}\))",
        title,
    )
    if m:
        return m.group(1)
    return title
'''
        if marker not in text:
            raise RuntimeError("3-1 legal_fingerprint 위치를 찾지 못했습니다.")
        text = text.replace(marker, addition + marker, 1)

    text = replace_once(
        text,
        """def legal_fingerprint(row: pd.Series) -> str:
    # Agency-independent: same regulation reposted by another official site is one item.
    return canonical_regulation_title(row.get('Headline',''))
""",
        """def legal_fingerprint(row: pd.Series) -> str:
    # 기관이나 URL이 달라도 동일 법규번호는 하나의 법규로 처리한다.
    return (
        regulation_number(row.get('Headline', ''))
        or canonical_regulation_title(row.get('Headline', ''))
    )
""",
        "3-1 법규번호 중복키",
    )

    anchor = """    work = df.sort_values(['Date','Headline'], ascending=[False,True]).copy()
    keep = []
"""
    replacement = """    work = df.sort_values(['Date','Headline'], ascending=[False,True]).copy()
    work['Headline'] = work['Headline'].apply(clean_gazette_headline)
    keep = []
"""
    text = replace_once(
        text, anchor, replacement, "3-1 관보 혼합제목 정리"
    )

    old = """            if canonical_a and canonical_a == canonical_b:
                dup = True
                break
"""
    new = """            number_a = regulation_number(row.get('Headline', ''))
            number_b = regulation_number(kr.get('Headline', ''))
            if number_a and number_a == number_b:
                dup = True
                break
            if canonical_a and canonical_a == canonical_b:
                dup = True
                break
"""
    text = replace_once(text, old, new, "3-1 고시번호 우선 중복제거")
    return text


def patch_32(text: str) -> str:
    text = text.replace(
        "GTI v5.3 STEP3-2 BALANCED NEWS CANDIDATE ENGINE START",
        "GTI v5.5 STEP3-2 QUALITY-FIRST NEWS CANDIDATE ENGINE START",
    )

    text = text.replace(
        'floor_target = int(os.getenv("GTI_STEP3_CANDIDATE_FLOOR", "150"))',
        'floor_target = int(os.getenv("GTI_STEP3_CANDIDATE_FLOOR", "0"))',
    )

    text = text.replace(
        "def ensure_candidate_floor(filtered_df: pd.DataFrame, "
        "prefilter_df: pd.DataFrame, floor: int = 150)",
        "def ensure_candidate_floor(filtered_df: pd.DataFrame, "
        "prefilter_df: pd.DataFrame, floor: int = 0)",
    )

    text = replace_once(
        text,
        """    if len(filtered_df) >= floor or prefilter_df.empty:
        return filtered_df
""",
        """    # 보고 품질이 목적이므로 건수를 맞추기 위한 일반뉴스 복원을 하지 않는다.
    if floor <= 0 or len(filtered_df) >= floor or prefilter_df.empty:
        return filtered_df
""",
        "3-2 후보 강제복원 기본 차단",
    )

    text = text.replace(
        """pool["CandidateGate"].astype(str).isin(["CORE", "CONTEXTUAL"])""",
        """pool["CandidateGate"].astype(str).eq("CORE")""",
    )

    issue_filter_anchor = """    if not pool.empty:
        pool = pool[
"""
    if "Never revive a row merely to satisfy volume" not in text:
        addition = """    if "IssueKey" in pool.columns:
        pool = pool[pool["IssueKey"].astype(str).isin([
            "TARIFF", "AD_CVD", "EXPORT_CONTROL", "CBAM_CARBON",
            "ORIGIN_FTA", "HS_CLASSIFICATION", "CUSTOMS"
        ])].copy()

    # Never revive a row merely to satisfy volume.
    pool = pool[pool.apply(has_title_keyword, axis=1)].copy()

"""
        if issue_filter_anchor not in text:
            raise RuntimeError("3-2 후보복원 필터 위치를 찾지 못했습니다.")
        text = text.replace(
            issue_filter_anchor,
            addition + issue_filter_anchor,
            1,
        )

    old_cluster = """        group = group.sort_values(["FinalScore", "SamsungImpactScore", "TopicScore", "FilterDate"],
                                  ascending=[False, False, False, False]).copy()
"""
    new_cluster = """        group = group.copy()
        group["_strong_title"] = group.apply(
            lambda r: 1 if has_title_keyword(r) else 0, axis=1
        )
        group["_official_source"] = group.apply(source_priority, axis=1)
        group = group.sort_values(
            [
                "_strong_title", "SamsungImpactScore", "_official_source",
                "FinalScore", "TopicScore", "FilterDate"
            ],
            ascending=[False, False, False, False, False, False],
        ).copy()
"""
    text = replace_once(
        text, old_cluster, new_cluster, "3-2 관세정책 대표기사 우선"
    )

    old_time = """    run_day = pd.Timestamp(datetime.now()).date()
"""
    new_time = """    now_ts = pd.Timestamp(datetime.now())
    run_day = now_ts.date()
    fresh_cutoff = now_ts - pd.Timedelta(hours=RECENT_HOURS)
"""
    text = replace_once(
        text, old_time, new_time, "3-2 자정 경계 24시간 보호"
    )

    old_mask = """    same_day_mask = seen_dates.dt.date.eq(run_day)
"""
    new_mask = """    # 자정을 넘겨 재실행해도 최근 24시간 기사는 과거 중복으로 제거하지 않는다.
    fresh_window_mask = publish_dates.ge(fresh_cutoff)
    same_day_mask = (
        seen_dates.dt.date.eq(run_day)
        | fresh_window_mask.fillna(False)
    )
"""
    text = replace_once(
        text, old_mask, new_mask, "3-2 최근 24시간 누적 중복 보호"
    )
    return text


def patch_41(text: str) -> str:
    text = text.replace(
        "🚀 GTI STEP4-1 REGULATION AI ONLY FIXED START",
        "GTI STEP4-1 REGULATION AI v8 VERIFIED BODY START",
    )

    old_hs = (
        '    hs = sorted(set(re.findall('
        'r"\\b\\d{4}(?:\\.\\d{2})?(?:\\.\\d{2})?\\b", t)))[:6]'
    )

    if old_hs in text:
        new_hs = r'''    # 4자리 숫자만으로는 HS로 인정하지 않는다.
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
    hs = hs[:6]'''
        text = text.replace(old_hs, new_hs, 1)
        print("[PATCH] 4-1 전화번호 HS 오인식 차단")
    elif "4자리 숫자만으로는 HS로 인정하지 않는다" in text:
        print("[SKIP] 이미 적용됨: 4-1 HS 차단")
    else:
        raise RuntimeError("4-1 HS 추출 위치를 찾지 못했습니다.")

    if "def _is_navigation_or_gazette_shell(" not in text:
        marker = "\ndef _fallback_gti_analysis_from_body("
        addition = '''

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
'''
        if marker not in text:
            raise RuntimeError("4-1 fallback 분석 위치를 찾지 못했습니다.")
        text = text.replace(marker, addition + marker, 1)
        print("[PATCH] 4-1 관보 메뉴본문 차단")

    old_body = """    if not body:
        body, status = fetch_article_body_for_ai(url)

    cache = _ensure_gemini_cache()
"""
    new_body = """    if not body:
        body, status = fetch_article_body_for_ai(url)

    if _is_navigation_or_gazette_shell(body):
        body = ""
        status = f"INVALID_GAZETTE_SHELL:{status}"

    cache = _ensure_gemini_cache()
"""
    text = replace_once(
        text, old_body, new_body, "4-1 잘못된 관보본문 무효화"
    )

    old_output = """        impact = clean(r.get("Samsung Impact", "Watch")) or "Watch"
        rows.append({
"""
    new_output = """        impact = clean(r.get("Samsung Impact", "Watch")) or "Watch"
        headline = clean(r.get("Headline"))
        agency = clean(r.get("Agency"))
        country = ""
        if "관세청" in headline:
            agency = "대한민국 관세청"
            country = "대한민국"
        rows.append({
"""
    text = replace_once(
        text, old_output, new_output, "4-1 국가·기관 보정"
    )

    text = text.replace(
        '"Date": r["Date"], "Headline": r["Headline"],',
        '"Date": r["Date"], "Headline": headline,',
        1,
    )
    text = text.replace(
        '"Country": "", "Agency": r["Agency"],',
        '"Country": country, "Agency": agency,',
        1,
    )
    return text


def patch_42(text: str) -> str:
    text = text.replace(
        "GTI STEP4-2 NEWS AI v27 FAIL-CLOSED START",
        "GTI STEP4-2 NEWS AI v29 ISSUE-VERIFIED START",
    )
    text = text.replace(
        "GTI STEP4-2 NEWS AI v28 PREFLIGHT + FAIL-CLOSED START",
        "GTI STEP4-2 NEWS AI v29 ISSUE-VERIFIED START",
    )

    issue_json = (
        ' "issue": '
        '"TARIFF|AD_CVD|EXPORT_CONTROL|SANCTIONS|CUSTOMS|'
        'HS_CLASSIFICATION|ORIGIN_FTA|CBAM_CARBON|OTHER",'
    )
    if issue_json not in text:
        marker = ' "policy_event": true,\n'
        if marker not in text:
            raise RuntimeError("4-2 Gemini JSON policy_event 위치를 찾지 못했습니다.")
        text = text.replace(marker, issue_json + "\n" + marker, 1)
        print("[PATCH] 4-2 Gemini Issue 재판정")

    old_policy = """    policy_event = as_bool(result.get("policy_event")) and concrete_customs_signal(evidence_text)
    country = clean(result.get("country"))
"""
    new_policy = """    policy_event = (
        as_bool(result.get("policy_event"))
        and concrete_customs_signal(evidence_text)
    )
    issue_out = clean(result.get("issue")).upper()
    allowed_issues = {
        "TARIFF", "AD_CVD", "EXPORT_CONTROL", "SANCTIONS",
        "CUSTOMS", "HS_CLASSIFICATION", "ORIGIN_FTA",
        "CBAM_CARBON",
    }
    if issue_out not in allowed_issues:
        policy_event = False
        issue_out = "OTHER"

    country = clean(result.get("country"))
"""
    text = replace_once(
        text, old_policy, new_policy, "4-2 Issue 적격성 검증"
    )

    old_update = """        "country": country,
    })
"""
    new_update = """        "country": country,
        "issue": issue_out,
    })
"""
    text = replace_once(
        text, old_update, new_update, "4-2 Issue 결과 전달"
    )

    old_row = """        r["Country"] = clean(a.get("country"))
        r["Agency"] = clean(a.get("agency")) or clean(row.get("Publisher"))
"""
    new_row = """        r["Country"] = clean(a.get("country"))
        r["Issue"] = clean(a.get("issue")) or "OTHER"
        r["Agency"] = clean(a.get("agency")) or clean(row.get("Publisher"))
"""
    text = replace_once(
        text, old_row, new_row, "4-2 감사·요약 Issue 반영"
    )
    return text


def patch_5(text: str) -> str:
    text = text.replace(
        'GTI_STEP5_VERSION = "v322 VERIFIED POLICY + CUMULATIVE"',
        'GTI_STEP5_VERSION = "v323 OFFICIAL POLICY + CUMULATIVE"',
    )

    old_issue = """def issue_for(row) -> str:
    issue = clean(row.get("Issue"))
"""
    new_issue = """def issue_for(row) -> str:
    issue = clean(row.get("Issue"))
    canonical_issue = {
        "TARIFF": "관세정책",
        "AD_CVD": "AD/CVD",
        "EXPORT_CONTROL": "수출통제",
        "SANCTIONS": "수출통제/제재",
        "CUSTOMS": "통관/세관",
        "HS_CLASSIFICATION": "HS/품목분류",
        "ORIGIN_FTA": "FTA/원산지",
        "CBAM_CARBON": "CBAM",
        "OTHER": "Watch",
    }
    if issue.upper() in canonical_issue:
        return canonical_issue[issue.upper()]
"""
    text = replace_once(
        text, old_issue, new_issue, "5 Gemini Issue 표준 연결"
    )

    old_exact = """    rows["_exact_key"] = rows.apply(
        lambda r: (
            clean(r.get("Content Type")),
            clean(r.get("URL")).lower(),
            clean(r.get("Headline")).lower(),
            clean(r.get("Date"))[:16],
        ),
        axis=1,
    )
"""
    new_exact = r'''    def _mail_event_key(r: pd.Series):
        title = clean(r.get("Headline"))
        if clean(r.get("Content Type")) == "Regulation":
            m = re.search(
                r"(관세청(?:고시|공고|훈령|예규)제?\s*20\d{2}[-–]\d+호)",
                title,
            )
            if m:
                return (
                    "REG:"
                    + re.sub(r"\s+", "", m.group(1)).lower()
                )
        return "|".join([
            clean(r.get("Content Type")),
            clean(r.get("URL")).lower(),
            title.lower(),
            clean(r.get("Date"))[:16],
        ])

    rows["_exact_key"] = rows.apply(_mail_event_key, axis=1)
'''
    text = replace_once(
        text, old_exact, new_exact, "5 동일 법규번호 중복 차단"
    )

    old_country = """    policy = policy[issue_ok & verified & country_ok & ((event_ok & evidence) | reg_evidence)]
    if policy.empty:
"""
    new_country = r'''    policy = policy[
        issue_ok
        & verified
        & country_ok
        & ((event_ok & evidence) | reg_evidence)
    ]

    # 국가별 정책은 공식기관·공식문서·법원 판결 근거가 있어야 한다.
    official_blob = policy.get(
        "Official Evidence",
        pd.Series("", index=policy.index),
    ).astype(str).str.lower()

    official_ok = official_blob.str.contains(
        r"정부|세관|관세청|상무부|위원회|법원|판결|고시|공고|행정명령|"
        r"government|customs|ministry|commission|court|ruling|official",
        regex=True,
    )
    policy = policy[
        official_ok | policy["Content Type"].eq("Regulation")
    ]

    if policy.empty:
'''
    text = replace_once(
        text, old_country, new_country, "5 국가별 공식정책 제한"
    )
    return text


PATCHERS = {
    "3-1": patch_31,
    "3-2": patch_32,
    "4-1": patch_41,
    "4-2": patch_42,
    "5": patch_5,
}


def main() -> None:
    missing = [str(path) for path in FILES.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "다음 실행파일이 없습니다:\n" + "\n".join(missing)
        )

    BACKUP.mkdir(parents=True, exist_ok=False)

    for path in FILES.values():
        shutil.copy2(path, BACKUP / path.name)

    print(f"[BACKUP] {BACKUP}")

    try:
        for key, path in FILES.items():
            print(f"\n=== {path.name} ===")
            original = read(path)
            updated = PATCHERS[key](original)
            write(path, updated)

        print("\n=== PYTHON 구문검사 ===")
        for path in FILES.values():
            py_compile.compile(str(path), doraise=True)
            print(f"[OK] {path.name}")

    except Exception:
        print("\n[ERROR] 수정 또는 구문검사 실패")
        print("[RESTORE] 기존 파일로 자동 복구합니다.")

        for path in FILES.values():
            backup_file = BACKUP / path.name
            if backup_file.exists():
                shutil.copy2(backup_file, path)

        raise

    print("\n========================================")
    print("GTI 코드 수정 완료")
    print("백업 폴더:", BACKUP)
    print("========================================")
    print("실행 순서:")
    print("python C:\\Temp\\3-1.regulation_merge.py")
    print("python C:\\Temp\\3-2.news_merge.py")
    print("python C:\\Temp\\4-1.regulation_ai_analysis.py")
    print("python C:\\Temp\\4-2.news_ai_analysis.py")
    print("※ 5번은 4-1/4-2 결과 확인 후 실행")


if __name__ == "__main__":
    main()