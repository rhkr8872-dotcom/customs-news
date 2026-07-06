# GTI FINAL CORE v5 - NAVER news candidate collector
# =========================================================
# GTI STEP2 - NAVER NEWS COLLECTOR
# Priority Balanced Collection Version
# =========================================================
# 역할 정리
# - STEP2: 후보 뉴스 수집 단계
#          너무 강하게 자르지 않고, keyword / priority / reason을 붙여 저장
# - STEP3/STEP4: 뉴스 선정/분석 단계
#          동일·유사 뉴스 제거, Top30 선정, 삼성 영향도/Action 정밀 분석 수행
#
# 핵심 개선
# - keyword.xlsx의 Priority 컬럼 반영
# - 결과에 keyword, priority, description, filter_reason 저장
# - 수출/수입/무역 등 broad keyword는 저장은 허용하되 relevance_score를 낮게 부여
# - 명백한 잡뉴스만 STEP2에서 제외
# - 동일 URL/동일 제목은 최고 priority 기준으로 병합
# =========================================================

import os
import re
import html
import time
from datetime import datetime, timedelta
from urllib.parse import urlparse

import pandas as pd
import requests

print("🚀 GTI STEP2 NAVER NEWS-ONLY PRIORITY BALANCED VERSION START")

# =============================
# PATH
# =============================
BASE_PATH = os.getenv("GTI_BASE_DIR", r"C:\temp")
KEYWORD_FILE = os.path.join(BASE_PATH, "keyword.xlsx")
RAW_FILE = os.path.join(BASE_PATH, "2-1.naver_news_raw.xlsx")

# =============================
# NAVER API
# =============================
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "")

if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
    raise RuntimeError(
        "NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 환경변수를 설정해 주세요."
    )

NAVER_URL = "https://openapi.naver.com/v1/search/news.json"
HEADERS = {
    "X-Naver-Client-Id": NAVER_CLIENT_ID,
    "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
}

# =============================
# PARAMETER
# =============================
LOOKBACK_HOURS = int(os.getenv("GTI_LOOKBACK_HOURS", "72"))
NAVER_DISPLAY = int(os.getenv("NAVER_DISPLAY", "100"))
MIN_SAVE_PRIORITY = int(os.getenv("GTI_MIN_SAVE_PRIORITY", "50"))
REQUEST_SLEEP_SEC = float(os.getenv("GTI_NAVER_SLEEP_SEC", "0.1"))

# STEP2는 후보 수집 단계이므로 과도하게 줄이지 않는다.
# 100: 핵심 이슈, 80: 중요 이슈, 50: 일반 정책 후보
CRITICAL_PRIORITY = int(os.getenv("GTI_CRITICAL_PRIORITY", "100"))
HIGH_PRIORITY = int(os.getenv("GTI_HIGH_PRIORITY", "80"))
MID_PRIORITY = int(os.getenv("GTI_MID_PRIORITY", "50"))

# =============================
# TEXT NORMALIZE
# =============================
def clean_html_text(text):
    if not text:
        return ""
    t = html.unescape(str(text))
    t = re.sub(r"<.*?>", "", t)
    t = t.replace("&quot;", '"').replace("&amp;", "&")
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def normalize_title(title):
    t = clean_html_text(title)
    t = re.sub(r"\s*\[.*?\]\s*$", "", t)
    t = re.sub(r"\(.*?\)", "", t)
    t = t.replace('"', "").replace("'", "")
    t = re.sub(r"[…·]", " ", t)
    t = re.sub(r"\s?기자$", "", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def normalize_match_text(text):
    return re.sub(r"\s+", " ", clean_html_text(text)).lower().strip()


def contains_any(text, keywords):
    t = normalize_match_text(text)
    return any(str(k).lower() in t for k in keywords if str(k).strip())

# =============================
# KEYWORD LOAD
# =============================
def load_keyword_master():
    """
    keyword.xlsx 권장 컬럼
    - keyword: 검색어
    - Priority: 100 / 80 / 50 등
    - active: Y/N. 없으면 전체 사용
    - importance: Priority 공백 시 fallback

    fallback:
    - CRITICAL=100, HIGH=80, MEDIUM/MID=50, LOW=30
    """
    if not os.path.exists(KEYWORD_FILE):
        raise FileNotFoundError(f"keyword file not found: {KEYWORD_FILE}")

    df = pd.read_excel(KEYWORD_FILE)
    df.columns = [str(c).strip() for c in df.columns]

    if "keyword" not in df.columns:
        df = df.rename(columns={df.columns[0]: "keyword"})

    if "active" in df.columns:
        df = df[df["active"].fillna("Y").astype(str).str.upper().str.strip() != "N"]

    if "Priority" not in df.columns:
        df["Priority"] = None

    if "importance" in df.columns:
        imp = df["importance"].fillna("").astype(str).str.upper().str.strip()
        fallback_priority = imp.map({
            "CRITICAL": 100,
            "HIGH": 80,
            "MEDIUM": 50,
            "MID": 50,
            "LOW": 30,
        })
    else:
        fallback_priority = pd.Series([50] * len(df), index=df.index)

    df["Priority"] = pd.to_numeric(df["Priority"], errors="coerce")
    df["Priority"] = df["Priority"].fillna(fallback_priority).fillna(50).astype(int)

    df["keyword"] = df["keyword"].fillna("").astype(str).str.strip()
    df = df[df["keyword"] != ""]
    df = df.drop_duplicates(subset=["keyword"], keep="first")
    df = df[df["Priority"] >= MIN_SAVE_PRIORITY]
    df = df.sort_values(by=["Priority", "keyword"], ascending=[False, True]).reset_index(drop=True)

    print(f"🔎 keywords loaded: {len(df)} / min priority: {MIN_SAVE_PRIORITY}")
    return df[["keyword", "Priority"]]

# =============================
# FILTER DICTIONARY
# =============================
# STEP2에서 제외할 것은 명백한 비업무성 잡뉴스 중심으로 제한한다.
STOCK_KEYWORDS = [
    "코스피", "코스닥", "시총", "주가", "급등", "급락", "상승", "하락",
    "증시", "투자", "매수", "매도", "외인", "기관", "개인", "공매도",
    "목표가", "리포트", "특징주", "테마주", "상한가", "하한가",
]

HARD_EXCLUDE_KEYWORDS = [
    "연예", "배우", "아이돌", "결혼", "열애", "이혼",
    "야구", "축구", "골프", "농구", "배구",
    "날씨", "운세", "로또", "맛집",
    "음주운전", "마약", "보이스피싱",
]

# 업무와 무관한 경우가 많은 생활/산업 잡뉴스.
# 단, 관세/통관/FTA/수출통제 등 강한 정책 맥락이 있으면 살린다.
SOFT_NOISE_KEYWORDS = [
    "저작권 침해", "불법복제", "웹툰", "게임 출시", "영화", "드라마",
    "여행상품", "항공권", "호텔", "축제", "관광객", "여행객", "여행업", "크루즈",
    "건강보험", "의료수가", "진료비", "보험료", "병원비",
    "우유", "분유", "농산물 가격", "밥상물가", "생활물가",
]

# 단독 검색 시 잡뉴스가 많지만 후보 수집에는 필요한 broad keyword
BROAD_KEYWORDS = [
    "수출", "수입", "무역", "통상", "세관", "customs", "trade", "import", "export",
    "aduana", "aduanas", "alfândega", "comercio", "comércio",
]

# 강한 관세/통상/규제 맥락
STRONG_POLICY_CONTEXT = [
    "관세", "통관", "세관", "원산지", "fta", "자유무역협정",
    "품목분류", "hs code", "hs코드", "hs 코드", "tariff", "customs",
    "덤핑", "반덤핑", "상계관세", "세이프가드", "무역구제", "관세율", "할당관세",
    "수출통제", "전략물자", "제재", "수입규제", "수출규제", "통상규제",
    "entity list", "denied persons", "bis", "ofac", "ustr", "cbp", "wto",
    "section 301", "301조", "232조", "uflpa", "cbam", "탄소국경",
    "미 상무부", "미국 상무부", "상무부 산업안보국", "eu 집행위", "유럽연합 집행위",
]

# 약한 정책/경제안보 맥락
WEAK_POLICY_CONTEXT = [
    "무역", "수출", "수입", "공급망", "경제안보", "리쇼어링", "디커플링",
    "협정", "epa", "통상", "수출입", "해외시장", "교역", "무역협상",
    "보호무역", "무역장벽", "비관세", "시장접근",
]

# 삼성전자 영향 가능성이 높은 산업/품목 맥락
SECTOR_CONTEXT_KEYWORDS = [
    "반도체", "semiconductor", "chip", "chips", "디스플레이", "배터리",
    "스마트폰", "휴대폰", "전자", "부품", "장비", "핵심광물", "희토류",
    "ai", "인공지능", "데이터센터", "hbm", "메모리", "dram", "낸드", "파운드리",
    "웨이퍼", "소부장", "장비부품", "전기전자", "ict", "서버", "gpu",
]

SPECIFIC_CRITICAL_TERMS = [
    "semiconductor tariff", "export control", "entity list", "forced labor", "uflpa",
    "section 301", "301조", "232조", "cbam", "수출통제", "전략물자", "반덤핑", "상계관세", "세이프가드",
]

BIS_KEYWORDS = ["bis"]

BIS_EXPORT_CONTROL_CONTEXT = [
    "export control", "export controls", "commerce department", "department of commerce",
    "bureau of industry and security", "industry and security", "entity list", "denied persons",
    "수출통제", "수출 통제", "산업안보국", "산업보안국", "미 상무부", "미국 상무부",
    "상무부 산업안보국", "상무부 산업보안국", "전략물자", "제재", "중국 기업",
    "ai chip", "ai chips", "ai 칩", "첨단 칩", "반도체",
]

BIS_FINANCE_NOISE_CONTEXT = [
    "bis 자기자본", "bis비율", "bis 비율", "자기자본비율", "국제결제은행",
    "저축은행", "은행", "금융지주", "자본비율", "건전성", "bank for international settlements",
]

BROAD_WEAK_KEYWORDS = [
    "통상", "세관", "수출입", "수출", "수입", "무역",
    "customs", "trade", "import", "export",
]

TRADE_REMEDY_KEYWORDS = [
    "반덤핑", "덤핑방지관세", "덤핑 방지 관세", "상계관세", "상계 관세",
    "무역구제", "세이프가드", "ad/cvd", "anti-dumping", "antidumping",
    "countervailing", "countervailing duty", "trade remedy", "safeguard",
]


def keyword_equals_any(keyword, terms):
    k = normalize_match_text(keyword)
    return any(k == normalize_match_text(t) for t in terms)


def is_broad_keyword(keyword):
    k = normalize_match_text(keyword)
    return any(k == b.lower() for b in BROAD_KEYWORDS)


def is_weak_broad_keyword(keyword):
    return keyword_equals_any(keyword, BROAD_WEAK_KEYWORDS)


def is_bis_keyword(keyword):
    return keyword_equals_any(keyword, BIS_KEYWORDS)


def has_bis_export_control_context(text):
    return contains_any(text, BIS_EXPORT_CONTROL_CONTEXT)


def is_bis_finance_noise(text):
    return contains_any(text, BIS_FINANCE_NOISE_CONTEXT) and not has_bis_export_control_context(text)


def is_trade_remedy_candidate(keyword, text):
    return contains_any(keyword, TRADE_REMEDY_KEYWORDS) or contains_any(text, TRADE_REMEDY_KEYWORDS)


def has_strong_policy_context(text):
    return contains_any(text, STRONG_POLICY_CONTEXT)


def has_weak_policy_context(text):
    return contains_any(text, WEAK_POLICY_CONTEXT)


def has_sector_context(text):
    return contains_any(text, SECTOR_CONTEXT_KEYWORDS)


def is_specific_critical_keyword(keyword):
    return contains_any(keyword, SPECIFIC_CRITICAL_TERMS)


def is_stock_noise(text):
    return contains_any(text, STOCK_KEYWORDS)


def is_hard_excluded(text, priority):
    # 100점 핵심 키워드는 하드 제외어가 있어도 일단 후보로 보존한다.
    if priority >= CRITICAL_PRIORITY:
        return False
    return contains_any(text, HARD_EXCLUDE_KEYWORDS)


def is_soft_noise_without_policy(text):
    # soft noise라도 강한 정책 맥락이 있으면 STEP2에서는 보존한다.
    return contains_any(text, SOFT_NOISE_KEYWORDS) and not has_strong_policy_context(text)


def calculate_relevance_score(title, description, keyword, priority):
    """
    STEP2용 후보 점수.
    Top30 선정 점수가 아니라, 후속 단계에서 정렬/참고하기 위한 보조 점수다.
    """
    text = f"{title} {description}"
    score = int(priority)
    bis_kw = is_bis_keyword(keyword)
    trade_remedy = is_trade_remedy_candidate(keyword, text)
    strong_policy = has_strong_policy_context(text)
    weak_policy = has_weak_policy_context(text)
    sector = has_sector_context(text)

    if strong_policy:
        score += 25
    elif weak_policy:
        score += 10

    if sector:
        score += 15

    if is_specific_critical_keyword(keyword):
        score += 15

    if trade_remedy:
        score += 35

    if bis_kw:
        if has_bis_export_control_context(text):
            score += 25
        else:
            score -= 60
        if is_bis_finance_noise(text):
            score -= 80

    if is_broad_keyword(keyword):
        score -= 10

    if is_weak_broad_keyword(keyword) and not strong_policy:
        score -= 25

    if is_soft_noise_without_policy(text):
        score -= 30

    return max(0, min(score, 150))


def should_keep_article(title, description, keyword, priority):
    """
    STEP2 Balanced 저장 판단

    원칙:
    - 후보 수집 단계에서는 너무 엄격하게 제외하지 않는다.
    - 명백한 잡뉴스/주식/생활성 기사만 제거한다.
    - broad keyword도 완전 배제하지 않고, filter_reason과 relevance_score로 후속 단계에 넘긴다.
    - 동일/유사 뉴스 제거와 Top30 선정은 STEP3/STEP4에서 수행한다.
    """
    text = f"{title} {description}"

    if len(title) < 10:
        return False, "short_title"

    if is_stock_noise(text):
        return False, "stock_noise"

    if is_hard_excluded(text, priority):
        return False, "hard_excluded_noise"

    if is_soft_noise_without_policy(text):
        return False, "soft_noise_without_policy_context"

    strong_policy = has_strong_policy_context(text)
    weak_policy = has_weak_policy_context(text)
    sector = has_sector_context(text)
    broad_kw = is_broad_keyword(keyword)
    weak_broad_kw = is_weak_broad_keyword(keyword)
    specific_kw = is_specific_critical_keyword(keyword)
    bis_kw = is_bis_keyword(keyword)
    trade_remedy = is_trade_remedy_candidate(keyword, text)

    if bis_kw and is_bis_finance_noise(text):
        return False, "bis_finance_noise"

    if bis_kw and not has_bis_export_control_context(text):
        return False, "bis_without_export_control_context"

    if trade_remedy:
        return True, "trade_remedy_forced_candidate"

    if priority >= CRITICAL_PRIORITY:
        return True, "critical_priority_candidate"

    if priority >= HIGH_PRIORITY:
        if specific_kw:
            return True, "specific_high_priority_candidate"
        if strong_policy:
            return True, "high_priority_with_strong_policy_context"
        if weak_broad_kw:
            return False, "broad_keyword_without_strong_policy_context"
        if weak_policy or sector:
            return True, "high_priority_with_weak_policy_or_sector_context"
        if broad_kw:
            return False, "broad_high_priority_without_context"
        return True, "high_priority_loose_candidate"

    if priority >= MID_PRIORITY:
        if strong_policy:
            return True, "mid_priority_with_strong_policy_context"
        if weak_broad_kw:
            return False, "broad_mid_priority_without_strong_policy_context"
        if weak_policy and sector:
            return True, "mid_priority_with_policy_and_sector_context"
        if broad_kw and (weak_policy or sector):
            return True, "broad_mid_priority_context_candidate"
        return False, "mid_priority_without_policy_context"

    return False, "low_priority"

# =============================
# NAVER COLLECT
# =============================
def collect_naver(keyword_df):
    results = []

    for _, row in keyword_df.iterrows():
        kw = str(row["keyword"]).strip()
        priority = int(row["Priority"])

        params = {
            "query": kw,
            "display": min(max(NAVER_DISPLAY, 1), 100),
            "start": 1,
            "sort": "date",
        }

        try:
            res = requests.get(NAVER_URL, headers=HEADERS, params=params, timeout=15)
        except Exception as e:
            print(f"⚠️ NAVER request error: {kw} / {e}")
            continue

        if res.status_code != 200:
            print(f"⚠️ NAVER API error: {kw} / status={res.status_code} / {res.text[:120]}")
            continue

        items = res.json().get("items", [])

        for item in items:
            try:
                title = normalize_title(item.get("title", ""))
                description = clean_html_text(item.get("description", ""))
                url = item.get("originallink") or item.get("link") or ""
                source = urlparse(url).netloc or url

                pub = item.get("pubDate")
                try:
                    date = datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S +0900")
                except Exception:
                    continue

                keep, filter_reason = should_keep_article(title, description, kw, priority)
                if not keep:
                    continue

                relevance_score = calculate_relevance_score(title, description, kw, priority)

                results.append({
                    "date": date,
                    "title": title,
                    "headline": title,
                    "url": url,
                    "source": source,
                    "keyword": kw,
                    "priority": priority,
                    "relevance_score": relevance_score,
                    "description": description,
                    "filter_reason": filter_reason,
                    "collected_at": datetime.now(),
                })

            except Exception as e:
                print(f"⚠️ item parse error: {e}")
                continue

        time.sleep(REQUEST_SLEEP_SEC)

    print(f"🟢 NAVER collected after balanced filter: {len(results)}")
    return results

# =============================
# DEDUP
# =============================
def deduplicate_keep_highest_priority(df):
    if df.empty:
        return df

    df["title_clean"] = df["title"].astype(str).str.lower().str.strip()
    df["url_clean"] = df["url"].astype(str).str.strip()
    df = df.sort_values(by=["priority", "relevance_score", "date"], ascending=[False, False, False])

    def merge_unique(s):
        vals = []
        for x in s.dropna().astype(str):
            for part in x.split(","):
                part = part.strip()
                if part and part not in vals:
                    vals.append(part)
        return ", ".join(vals)

    agg_map = {
        "date": "max",
        "title": "first",
        "headline": "first",
        "url": "first",
        "source": "first",
        "keyword": merge_unique,
        "priority": "max",
        "relevance_score": "max",
        "description": "first",
        "filter_reason": merge_unique,
        "collected_at": "max",
        "title_clean": "first",
    }

    df_url = df[df["url_clean"] != ""].groupby("url_clean", as_index=False).agg(agg_map)
    df_no_url = df[df["url_clean"] == ""].groupby("title_clean", as_index=False).agg(agg_map)

    out = pd.concat([df_url, df_no_url], ignore_index=True)
    out = out.sort_values(by=["priority", "relevance_score", "date"], ascending=[False, False, False])
    out = out.drop_duplicates(subset=["title_clean"], keep="first")
    return out

# =============================
# MAIN
# =============================
def main():
    keyword_df = load_keyword_master()
    data = collect_naver(keyword_df)
    df = pd.DataFrame(data)

    print(f"📊 TOTAL BALANCED RAW: {len(df)}")

    save_cols = [
        "date", "title", "headline", "url", "source",
        "keyword", "priority", "relevance_score",
        "description", "filter_reason", "collected_at",
    ]

    if df.empty:
        pd.DataFrame(columns=save_cols).to_excel(RAW_FILE, index=False)
        print("⚠️ no NAVER news saved")
        print("💾 saved:", RAW_FILE)
        return

    cutoff = datetime.now() - timedelta(hours=LOOKBACK_HOURS)
    before = len(df)
    df = df[df["date"] >= cutoff]
    print(f"📊 {LOOKBACK_HOURS}h FILTER: {before} -> {len(df)}")

    before = len(df)
    df = deduplicate_keep_highest_priority(df)
    print(f"📊 DEDUP KEEP HIGH PRIORITY: {before} -> {len(df)}")

    df = df.sort_values(by=["priority", "relevance_score", "date"], ascending=[False, False, False])

    for col in save_cols:
        if col not in df.columns:
            df[col] = ""

    df = df[save_cols]
    df.to_excel(RAW_FILE, index=False)

    print("💾 saved:", RAW_FILE)
    print("✅ STEP2 NAVER PRIORITY BALANCED DONE")

# =============================
# RUN
# =============================
if __name__ == "__main__":
    main()
