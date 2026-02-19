# -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief
NO_SUMMARY v2 (24H filter + dedup + TOP3 criteria 표시 + 제시어별 10건 제한)

- Gemini 요약/표시: 이번 버전에서는 사용하지 않음 (요약 품질 이슈로 비활성)
- Google News RSS 기반 (GitHub Actions)
- custom_queries.txt: 제시어(검색어) 목록 로드
- out/에 CSV/XLSX/HTML 저장
- 실무자용 메일(표 포함) + 임원용 메일(표 제외) 분리
"""

import os, re, html, smtplib, urllib.parse
import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import parsedate_to_datetime

import pandas as pd
import feedparser

# ===============================
# ENV
# ===============================
SMTP_SERVER   = os.getenv("SMTP_SERVER")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_EMAIL    = os.getenv("SMTP_EMAIL")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

RECIPIENTS = [x.strip() for x in os.getenv("RECIPIENTS", "").split(",") if x.strip()]
RECIPIENTS_EXEC = [x.strip() for x in os.getenv("RECIPIENTS_EXEC", "").split(",") if x.strip()]

BASE_DIR = os.getenv("BASE_DIR", os.path.join(os.path.dirname(__file__), "out"))
os.makedirs(BASE_DIR, exist_ok=True)

QUERIES_FILE = os.getenv("QUERIES_FILE", os.path.join(os.path.dirname(__file__), "custom_queries.txt"))
NEWS_LOOKBACK_HOURS = int(os.getenv("NEWS_LOOKBACK_HOURS", "24"))
MAX_ITEMS_PER_QUERY = int(os.getenv("MAX_ITEMS_PER_QUERY", "10"))   # 제시어별 표 최대 10건
FETCH_LIMIT_PER_QUERY = int(os.getenv("FETCH_LIMIT_PER_QUERY", "30"))  # RSS에서 먼저 긁는 개수

# ===============================
# TIME
# ===============================
def now_kst():
    return dt.datetime.utcnow() + dt.timedelta(hours=9)

def now_utc():
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

def within_lookback(published_raw: str, hours: int) -> bool:
    if not published_raw:
        return False
    try:
        d = parsedate_to_datetime(published_raw)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d >= (now_utc() - dt.timedelta(hours=hours))
    except Exception:
        return False

# ===============================
# LOAD QUERIES
# ===============================
def load_queries() -> list:
    if not os.path.exists(QUERIES_FILE):
        # fallback
        return ["관세", "FTA", "원산지", "수출통제", "무역구제"]
    out = []
    with open(QUERIES_FILE, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            out.append(s)
    return out or ["관세"]

# ===============================
# POLICY SCORE (간단 룰)
# ===============================
RISK_RULES = [
    ("section 301", 6),
    ("section 232", 6),
    ("ieepa", 6),
    ("export control", 6),
    ("sanction", 6),
    ("entity list", 5),
    ("anti-dumping", 5),
    ("countervailing", 5),
    ("safeguard", 5),
    ("tariff act", 5),
    ("trade expansion act", 5),
    ("international emergency economic powers act", 5),

    ("tariff", 4),
    ("duty", 4),
    ("관세", 4),
    ("관세율", 4),
    ("추가관세", 4),

    ("hs code", 3),
    ("hs", 3),
    ("원산지", 3),
    ("fta", 3),
    ("customs", 3),
    ("통관", 3),

    ("규정", 2),
    ("시행", 2),
    ("개정", 2),
    ("고시", 2),
]

def calc_policy_score(title: str, summary: str) -> int:
    t = f"{title} {summary}".lower()
    score = 1
    for kw, w in RISK_RULES:
        if kw in t:
            score += w
    return min(score, 20)

def score_to_importance(score: int) -> str:
    # 상: 관세율/HS/강제력 높은 규제에 가까운 것
    if score >= 13:
        return "상"
    if score >= 7:
        return "중"
    return "하"

# ===============================
# RELEVANCE FILTER (TOP3용 강화)
# ===============================
TRADE_CORE = [
    "관세", "관세율", "추가관세", "tariff", "duty",
    "hs", "hs code",
    "section 301", "section 232", "ieepa",
    "export control", "sanction", "entity list",
    "anti-dumping", "countervailing", "safeguard",
    "fta", "원산지", "customs", "통관",
    "tariff act", "trade expansion act", "international emergency economic powers act",
]
HARD_BLOCK = [
    # 명백히 무관/치안/사건성
    "narcotics", "drug", "cocaine", "meth", "arrest", "murder",
    "porn", "sexual",
    "sports", "match",
    "immigration", "ice raid",
    "celebrity", "k-pop",
    "wine", "whisky", "whiskey",
]
SAMSUNG_CONTEXT = [
    "samsung", "galaxy", "smartphone", "tablet", "tv", "monitor", "refrigerator",
    "air conditioner", "network", "5g", "base station", "antenna", "x-ray", "medical device",
    "mobile", "consumer electronics",
    "베트남", "인도", "멕시코", "브라질", "터키", "슬로바키아", "폴란드", "중국", "한국",
    "vietnam", "india", "mexico", "brazil", "turkiye", "turkey", "slovakia", "poland", "china", "korea",
]

def normalize_title(title: str) -> str:
    t = (title or "").strip()
    # "xxx - Source" 형태 제거
    t = re.sub(r"\s+-\s+[^-]{2,}$", "", t).strip()
    t = re.sub(r"\s+", " ", t)
    return t.lower()

def is_trade_relevant(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in TRADE_CORE)

def is_hard_blocked(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in HARD_BLOCK)

def is_top3_candidate(title: str, summary: str) -> bool:
    blob = f"{title} {summary}"
    if is_hard_blocked(blob):
        # 단, trade core가 강하게 있으면 예외 허용
        return is_trade_relevant(blob)
    # TOP3는 trade core 최소 1개는 있어야 함
    if not is_trade_relevant(blob):
        return False
    # 삼성/제품/생산지 문맥이 있으면 가산(후단에서 점수로 반영)
    return True

def samsung_affinity(title: str, summary: str) -> int:
    t = f"{title} {summary}".lower()
    hit = sum(1 for k in SAMSUNG_CONTEXT if k in t)
    return min(hit, 5)

# ===============================
# SENSOR (Google News RSS)
# ===============================
def fetch_google_news_rss(query: str) -> list:
    rss = "https://news.google.com/rss/search?" + urllib.parse.urlencode({
        "q": query,
        "hl": "ko",
        "gl": "KR",
        "ceid": "KR:ko"
    })
    feed = feedparser.parse(rss)
    return getattr(feed, "entries", []) or []

def clean_summary(raw: str) -> str:
    s = raw or ""
    s = re.sub(r"<[^>]+>", "", s).strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def run_sensor_build_df() -> pd.DataFrame:
    queries = load_queries()
    rows = []
    seen = set()  # (norm_title, link)

    for q in queries:
        entries = fetch_google_news_rss(q)

        for e in entries[:FETCH_LIMIT_PER_QUERY]:
            title = getattr(e, "title", "").strip()
            link = getattr(e, "link", "").strip()
            published = getattr(e, "published", "") or getattr(e, "updated", "")

            if not within_lookback(published, NEWS_LOOKBACK_HOURS):
                continue

            summary = clean_summary(getattr(e, "summary", "") or "")

            # 제목=요약 문제 방지: 요약이 너무 짧거나 제목과 사실상 동일하면 제외
            norm_t = normalize_title(title)
            norm_s = normalize_title(summary)
            if not summary or len(summary) < 40 or norm_s == norm_t:
                continue

            key = (norm_t, link)
            if key in seen:
                continue
            seen.add(key)

            score = calc_policy_score(title, summary)
            score += samsung_affinity(title, summary)  # 당사 문맥 히트 시 가산
            score = min(score, 20)

            importance = score_to_importance(score)

            rows.append({
                "제시어": q,
                "헤드라인": title,
                "주요내용": summary,          # 이번 버전에서는 메일 표기에 사용 안 함(저장만)
                "발표일": published,
                "출처(URL)": link,
                "중요도": importance,
                "대상 국가": "",              # (확장 가능) 지금은 공란
                "점수": score,
                "비고": "",                   # 실무자 표에서 비고 노출을 최소화하려면 빌드에서 제어
            })

    return pd.DataFrame(rows)

# ===============================
# TOP3 WHY/ACTION (중복 문구 통합)
# ===============================
def build_why_and_action(r: dict) -> tuple[str, str]:
    text = f"{r.get('헤드라인','')} {r.get('주요내용','')}".lower()

    why_parts = []
    act_parts = []

    # WHY
    if any(k in text for k in ["tariff", "관세", "관세율", "추가관세", "duty"]):
        why_parts.append("관세/세율 변동 가능성 → 수입원가·판매가/마진 영향")
        act_parts.append("대상국·대상품목(HS)·세율 변동 여부 확인")
    if any(k in text for k in ["hs", "hs code", "classification", "품목분류"]):
        why_parts.append("HS/품목분류 이슈 → 특혜 적용 실패·추징 리스크")
        act_parts.append("품목분류(HS) 및 분류근거 점검(법인/관세사 공통)")
    if any(k in text for k in ["fta", "원산지", "origin"]):
        why_parts.append("FTA/원산지 요건 변화 → 특혜관세 적용·증빙 리스크")
        act_parts.append("원산지결정기준/CO/증빙 체계 영향 1차 점검")
    if any(k in text for k in ["export control", "sanction", "entity list", "ieepa"]):
        why_parts.append("수출통제/제재 강화 → 거래중단·라이선스·리드타임 영향")
        act_parts.append("제재/통제 대상(거래상대·품목·국가) 스크리닝 및 라이선스 필요성 검토")
    if any(k in text for k in ["anti-dumping", "countervailing", "safeguard"]):
        why_parts.append("무역구제 조치 확대 → 추가비용·가격경쟁력 영향")
        act_parts.append("조치 대상국/품목(HS) 및 적용시점 확인 → 법인별 원가 영향 산정")

    # 삼성 맥락 템플릿 강화(기본 문구)
    why_parts.append("당사 주요 생산·판매법인(한국/중국/베트남/인도/멕시코/브라질/터키/동유럽) 관점에서 공급망 영향 점검 필요")
    act_parts.append("관련 생산/판매법인 우선순위로 영향(원가·마진·리드타임) 1차 산정 후 HQ 공유")

    # 중복 제거 + 합치기
    why = " / ".join(dict.fromkeys([p.strip() for p in why_parts if p.strip()]))
    action = " / ".join(dict.fromkeys([p.strip() for p in act_parts if p.strip()]))

    return why, action

# ===============================
# HTML STYLE (A4 가로 타이트)
# ===============================
STYLE = """
<style>
@page { size: A4 landscape; margin: 10mm; }
body{font-family:Malgun Gothic,Arial; background:#f6f6f6;}
.page{max-width:1120px;margin:auto;background:white;padding:12px;}
h2{margin:0 0 6px 0;}
.box{border:1px solid #ddd;border-radius:8px;padding:10px;margin:10px 0;}
ul{margin:6px 0 0 18px;}
li{margin-bottom:10px;}
.small{font-size:11px;color:#555;line-height:1.35;}
table{border-collapse:collapse;width:100%;table-layout:fixed;}
th,td{border:1px solid #ccc;padding:6px;font-size:12px;vertical-align:top;word-wrap:break-word;}
th{background:#f0f0f0;}
.col-k{width:120px;}
.col-h{width:auto;}
.col-d{width:160px;}
.col-i{width:56px;}
</style>
"""

IMPORTANCE_ORDER = {"하": 1, "중": 2, "상": 3}

def keyword_counts_line(df: pd.DataFrame) -> str:
    if df.empty:
        return ""
    vc = df["제시어"].value_counts()
    parts = [f"{k} {int(v)}건" for k, v in vc.items()]
    return " / ".join(parts)

# ===============================
# BUILD HTML (Executive / Practitioner 공통부)
# ===============================
def build_html_common(df: pd.DataFrame) -> tuple[str, pd.DataFrame]:
    date = now_kst().strftime("%Y-%m-%d")

    # TOP3 후보 필터 + 점수 정렬
    cand = df[df.apply(lambda r: is_top3_candidate(str(r.get("헤드라인","")), str(r.get("주요내용",""))), axis=1)].copy()
    cand["__imp"] = cand["중요도"].map(IMPORTANCE_ORDER).fillna(1).astype(int)

    top3 = cand.sort_values(["점수", "__imp"], ascending=[False, False]).head(3).to_dict("records")

    # TOP3 HTML
    top3_items = ""
    why_items = ""
    chk_items = ""

    for r in top3:
        tag = f"[{r.get('제시어','')} | 점수 {r.get('점수','')} | {r.get('대상 국가','')}]"
        top3_items += f"""
        <li>
          <b>{html.escape(tag)}</b><br/>
          <a href="{html.escape(str(r.get('출처(URL)','#')))}" target="_blank">{html.escape(str(r.get('헤드라인','')))}</a>
        </li>
        """

        why, action = build_why_and_action(r)
        why_items += f"<li><b>{html.escape(tag)}</b><br/>{html.escape(why)}</li>"
        chk_items += f"<li><b>{html.escape(tag)}</b><br/>{html.escape(action)}</li>"

    common_html = f"""
    <h2>관세·무역 뉴스 브리핑 ({date})</h2>

    <div class="box">
      <h3>① 관세·통상 핵심 TOP3</h3>
      <ul>{top3_items if top3_items else "<li>TOP3 후보 없음 (24시간/요약조건 필터 결과)</li>"}</ul>
    </div>

    <div class="box">
      <h3>② 왜 중요한가 (TOP3 이벤트 기반)</h3>
      <ul>{why_items if why_items else "<li>TOP3 후보 없음</li>"}</ul>
    </div>

    <div class="box">
      <h3>③ 당사 관점 체크포인트 (TOP3 이벤트 기반)</h3>
      <ul>{chk_items if chk_items else "<li>TOP3 후보 없음</li>"}</ul>
    </div>
    """
    return common_html, cand.drop(columns=["__imp"], errors="ignore")

# ===============================
# BUILD HTML (Practitioner: 표 포함)
# ===============================
def build_html_practitioner(df: pd.DataFrame) -> str:
    date = now_kst().strftime("%Y-%m-%d")

    common_html, df2 = build_html_common(df)

    # 제시어별 10건 제한(중복제거는 이미 run_sensor에서 수행)
    df2 = df2.copy()
    df2["__imp"] = df2["중요도"].map(IMPORTANCE_ORDER).fillna(1).astype(int)
    df2 = df2.sort_values(["제시어", "__imp", "점수"], ascending=[True, True, False])

    limited = []
    for k, g in df2.groupby("제시어", sort=True):
        limited.append(g.head(MAX_ITEMS_PER_QUERY))
    df3 = pd.concat(limited, ignore_index=True) if limited else df2.head(0)

    counts = keyword_counts_line(df3)

    # 표: 출처 칼럼 삭제, 헤드라인(링크)+주요내용은 이번 버전에서 표시하지 않음(요구: 요약 제외)
    rows = ""
    for _, r in df3.iterrows():
        link = str(r.get("출처(URL)", "#"))
        headline = str(r.get("헤드라인", ""))
        pub = str(r.get("발표일", ""))
        imp = str(r.get("중요도", ""))
        score = str(r.get("점수", ""))

        rows += f"""
        <tr>
          <td>{html.escape(str(r.get("제시어","")))}</td>
          <td>
            <a href="{html.escape(link)}" target="_blank">{html.escape(headline)}</a>
          </td>
          <td>{html.escape(pub)}</td>
          <td>{html.escape(imp)}</td>
        </tr>
        """

    table_html = f"""
    <div class="box">
      <h3>④ 정책 이벤트 표</h3>
      <div class="small">제시어별 주요뉴스 건수: {html.escape(counts) if counts else "0건"}</div>
      <table>
        <tr>
          <th class="col-k">제시어</th>
          <th class="col-h">헤드라인(링크)</th>
          <th class="col-d">발표일</th>
          <th class="col-i">중요도</th>
        </tr>
        {rows if rows else "<tr><td colspan='4'>24시간 이내/요약조건을 만족하는 항목 없음</td></tr>"}
      </table>
    </div>
    """

    return f"""
    <html><head>{STYLE}</head>
    <body><div class="page">
      {common_html}
      {table_html}
    </div></body></html>
    """

# ===============================
# BUILD HTML (Executive: 표 제외)
# ===============================
def build_html_exec(df: pd.DataFrame) -> str:
    date = now_kst().strftime("%Y-%m-%d")
    common_html, _ = build_html_common(df)

    return f"""
    <html><head>{STYLE}</head>
    <body><div class="page">
      <h2>[Executive] 관세·통상 핵심 TOP3 ({date})</h2>
      {common_html}
    </div></body></html>
    """

# ===============================
# WRITE OUTPUTS
# ===============================
def write_outputs(df: pd.DataFrame, html_body_prac: str, html_body_exec: str):
    today = now_kst().strftime("%Y-%m-%d")
    csv_path  = os.path.join(BASE_DIR, f"policy_events_{today}.csv")
    xlsx_path = os.path.join(BASE_DIR, f"policy_events_{today}.xlsx")
    html_path = os.path.join(BASE_DIR, f"policy_events_{today}.html")

    # 저장은 원본 DF 기준(주요내용 포함)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)

    # practitioner HTML만 저장(원하시면 exec도 별도 저장 가능)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_body_prac)

    return csv_path, xlsx_path, html_path

# ===============================
# MAIL
# ===============================
def send_mail_to(recipients, subject, html_body):
    if not recipients:
        return
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_EMAIL
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_EMAIL, SMTP_PASSWORD)
        s.sendmail(SMTP_EMAIL, recipients, msg.as_string())

# ===============================
# MAIN
# ===============================
def main():
    if not (SMTP_SERVER and SMTP_EMAIL and SMTP_PASSWORD):
        raise RuntimeError("SMTP_SERVER/SMTP_EMAIL/SMTP_PASSWORD 환경변수가 필요합니다.")

    df = run_sensor_build_df()
    if df is None or df.empty:
        print("24시간 이내/요약조건 충족 뉴스 없음 → 메일 발송 생략")
        return

    # 실무자 HTML(표 포함), 임원 HTML(표 제외)
    today = now_kst().strftime("%Y-%m-%d")
    html_prac = build_html_practitioner(df)
    html_exec = build_html_exec(df)

    write_outputs(df, html_prac, html_exec)

    send_mail_to(RECIPIENTS, f"관세·무역 뉴스 브리핑 ({today})", html_prac)
    send_mail_to(RECIPIENTS_EXEC, f"[Executive] 관세·통상 핵심 TOP3 ({today})", html_exec)

    print("✅ NO_SUMMARY v2 완료")
    print("BASE_DIR =", BASE_DIR)
    print("OUT_FILES =", os.listdir(BASE_DIR))

if __name__ == "__main__":
    main()
