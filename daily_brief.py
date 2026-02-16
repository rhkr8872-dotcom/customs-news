# -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief
vNext (E2E): Sensor + Outputs + Mail (Practitioner + Executive)
# -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief
vNext STABLE – 07~07 Filter + Exec Insight Integrated
"""

import os, re, html, smtplib
import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
import feedparser
import urllib.parse

# ===============================
# ENV
# ===============================
SMTP_SERVER   = os.getenv("SMTP_SERVER")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_EMAIL    = os.getenv("SMTP_EMAIL")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

RECIPIENTS = [x.strip() for x in os.getenv("RECIPIENTS","").split(",") if x.strip()]
RECIPIENTS_EXEC = [x.strip() for x in os.getenv("RECIPIENTS_EXEC","").split(",") if x.strip()]

BASE_DIR = os.getenv("BASE_DIR", os.path.join(os.path.dirname(__file__), "out"))
os.makedirs(BASE_DIR, exist_ok=True)

# ===============================
# TIME
# ===============================
def now_kst():
    return dt.datetime.utcnow() + dt.timedelta(hours=9)

def window_kst_07_to_07():
    now = now_kst()
    end = now.replace(hour=7, minute=0, second=0, microsecond=0)
    start = end - dt.timedelta(days=1)
    return start, end

def parse_published(entry):
    tp = getattr(entry, "published_parsed", None)
    if tp:
        utc = dt.datetime(*tp[:6])
        return utc + dt.timedelta(hours=9)
    return None

# ===============================
# SCORE
# ===============================
KEYWORDS_HIGH = ["section 301","section 232","ieepa","tariff rate","관세율","추가관세","hs code"]
KEYWORDS_MID  = ["fta","원산지","anti-dumping","countervailing","export control","sanction"]

def calc_score(text):
    t = text.lower()
    score = 1
    for k in KEYWORDS_HIGH:
        if k in t:
            score += 6
    for k in KEYWORDS_MID:
        if k in t:
            score += 3
    return score

def classify_importance(score):
    if score >= 10:
        return "상"
    if score >= 5:
        return "중"
    return "하"

# ===============================
# COUNTRY
# ===============================
COUNTRY_MAP = {
    "usa": "USA",
    "india": "India",
    "vietnam": "Vietnam",
    "mexico": "Mexico",
    "brazil": "Brazil",
    "china": "China",
    "eu": "EU"
}

def detect_country(text):
    t = text.lower()
    for k,v in COUNTRY_MAP.items():
        if k in t:
            return v
    return ""

# ===============================
# SENSOR
# ===============================
def run_sensor():
    query = os.getenv("NEWS_QUERY","관세")
    rss = "https://news.google.com/rss/search?" + urllib.parse.urlencode({
        "q": query,
        "hl": "ko",
        "gl": "KR",
        "ceid": "KR:ko"
    })

    feed = feedparser.parse(rss)
    start,end = window_kst_07_to_07()
    rows=[]

    for e in feed.entries[:50]:
        pub = parse_published(e)
        if not pub:
            continue
        if not(start <= pub < end):
            continue

        title = getattr(e,"title","").strip()
        summary = re.sub(r"<[^>]+>","",getattr(e,"summary","")).strip()

        if summary == title:
            summary = ""

        score = calc_score(title+" "+summary)

        rows.append({
            "헤드라인": title,
            "주요내용": summary,
            "발표일": pub.strftime("%Y-%m-%d %H:%M"),
            "대상 국가": detect_country(title+" "+summary),
            "관련 기관": "",
            "출처(URL)": getattr(e,"link",""),
            "중요도": classify_importance(score),
            "점수": score,
            "비고": ""
        })

    return pd.DataFrame(rows)

# ===============================
# EXEC INSIGHT
# ===============================
def build_exec_block(r):
    trigger = "관세/통상 정책"
    if r["중요도"]=="상":
        trigger="관세율/HS/제재 직결 이슈"

    exposure = "주요 생산법인 우선 스크리닝"
    if r["대상 국가"]:
        exposure = f"{r['대상 국가']} 생산/판매 법인 영향 점검"

    action = """
    1) 대상국/HS 확인<br/>
    2) 생산→판매 법인 원가 영향 산정<br/>
    3) 필요 시 HQ 대응 검토
    """

    return f"""
    <b>Trigger:</b> {trigger}<br/>
    <b>Exposure:</b> {exposure}<br/>
    <b>Action:</b><br/>{action}
    """

# ===============================
# HTML BUILD
# ===============================
def build_html(df):
    date=now_kst().strftime("%Y-%m-%d")
    top=df.sort_values("점수",ascending=False).head(3)

    top_html=""
    for _,r in top.iterrows():
        top_html+=f"""
        <li>
        <b>[{r['대상 국가']} | 중요도 {r['중요도']}]</b><br/>
        <a href="{r['출처(URL)']}" target="_blank">{html.escape(r['헤드라인'])}</a><br/>
        {html.escape(r['주요내용'])}<br/>
        {build_exec_block(r)}
        </li>
        """

    table_rows=""
    for _,r in df.iterrows():
        table_rows+=f"""
        <tr>
        <td>{r['헤드라인']}</td>
        <td>{r['주요내용']}</td>
        <td>{r['발표일']}</td>
        <td>{r['대상 국가']}</td>
        <td>{r['관련 기관']}</td>
        <td>{r['중요도']}</td>
        <td>{r['출처(URL)']}</td>
        <td>{r['비고']}</td>
        </tr>
        """

    return f"""
    <html>
    <body>
    <h2>관세·통상 정책 센서 ({date})</h2>

    <h3>① TOP3 + Executive Insight</h3>
    <ul>{top_html}</ul>

    <h3>② 정책 이벤트 표 (기존 유지)</h3>
    <table border="1">
    <tr>
    <th>헤드라인</th>
    <th>주요내용</th>
    <th>발표일</th>
    <th>대상 국가</th>
    <th>관련 기관</th>
    <th>중요도</th>
    <th>출처</th>
    <th>비고</th>
    </tr>
    {table_rows}
    </table>
    </body>
    </html>
    """

# ===============================
# MAIL
# ===============================
def send_mail(to,subject,body):
    msg=MIMEMultipart("alternative")
    msg["Subject"]=subject
    msg["From"]=SMTP_EMAIL
    msg["To"]=", ".join(to)
    msg.attach(MIMEText(body,"html","utf-8"))

    with smtplib.SMTP(SMTP_SERVER,SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_EMAIL,SMTP_PASSWORD)
        s.sendmail(SMTP_EMAIL,to,msg.as_string())

# ===============================
# MAIN
# ===============================
def main():
    df=run_sensor()
    if df.empty:
        print("No events")
        return

    html_body=build_html(df)
    send_mail(RECIPIENTS,f"관세·통상 정책 센서 ({now_kst().strftime('%Y-%m-%d')})",html_body)

    print("vNext 통합 완료")

if __name__=="__main__":
    main()

✅ Google News RSS 기반 센서 (GitHub Actions 구동)
✅ 수집 기간: 전날 07:00(KST) ~ 당일 07:00(KST)
✅ 메일 발송: 08:00(KST) (cron은 daily.yml에서 설정)
✅ out/에 CSV/XLSX/HTML 저장 + Artifact 업로드
✅ 실무자용(표 중심) + 임원용 TOP3(Trigger/Exposure/Action) 분리
✅ 요약 품질 강화: 원문 og:description / meta description 추출로 제목=요약 문제 개선
"""

# ===============================
# IMPORT
# ===============================
import os
import re
import html
import smtplib
import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import pandas as pd
import feedparser
import urllib.parse

# vNext: 원문 메타 요약 추출
import requests
from bs4 import BeautifulSoup

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

NEWS_QUERY = os.getenv("NEWS_QUERY", "관세")
NEWS_LIMIT = int(os.getenv("NEWS_LIMIT", "60"))  # 시간 필터 후 남는 수량을 고려해 여유
FETCH_META_MAX = int(os.getenv("FETCH_META_MAX", "12"))  # 원문 메타 추출 최대 건수 (과도 호출 방지)
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "10"))

USER_AGENT = os.getenv(
    "HTTP_UA",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
)

# ===============================
# TIME
# ===============================
def now_kst():
    return dt.datetime.utcnow() + dt.timedelta(hours=9)

def window_kst_07_to_07():
    """
    수집기간: 전날 07:00(KST) ~ 당일 07:00(KST)
    """
    now = now_kst()
    end = now.replace(hour=7, minute=0, second=0, microsecond=0)
    # end는 "가장 최근의 07:00" (실행이 08시라면 end는 오늘 07:00)
    if now < end:
        # 07시 이전 실행이면 end는 오늘 07시(미래)라서 그대로 두면 안 됨 -> 어제 07시로 내림
        end = end - dt.timedelta(days=1)
    start = end - dt.timedelta(days=1)
    return start, end

def parse_published_to_kst(entry) -> dt.datetime | None:
    """
    feedparser entry의 published_parsed(struct_time)를 우선 사용.
    없거나 파싱 실패하면 None (추정 금지)
    """
    try:
        tp = getattr(entry, "published_parsed", None)
        if tp:
            utc_dt = dt.datetime(*tp[:6])  # 보통 UTC로 들어옴
            return utc_dt + dt.timedelta(hours=9)  # KST
    except Exception:
        pass
    return None

# ===============================
# POLICY SCORE (고도화)
# ===============================
RISK_RULES = [
    ("tariff act", 8),
    ("trade expansion act", 8),
    ("international emergency economic powers act", 8),
    ("ieepa", 8),

    ("section 301", 6),
    ("section 232", 6),

    ("export control", 6),
    ("sanction", 6),
    ("entity list", 5),

    ("anti-dumping", 5),
    ("countervailing", 5),
    ("safeguard", 5),

    ("tariff", 4),
    ("duty", 4),
    ("관세", 4),
    ("관세율", 5),
    ("추가관세", 5),

    ("hs code", 4),
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
    if score >= 13:
        return "상"
    if score >= 7:
        return "중"
    return "하"

# ===============================
# COUNTRY TAG
# ===============================
COUNTRY_KEYWORDS = {
    "USA": ["u.s.", "united states", "america", "ustr", "section 301", "section 232", "commerce department", "bis"],
    "India": ["india", "cbic", "dgft"],
    "Türkiye": ["turkey", "türkiye"],
    "Vietnam": ["vietnam"],
    "Netherlands": ["netherlands", "dutch"],
    "EU": ["european union", "eu commission", "european commission"],
    "China": ["china"],
    "Mexico": ["mexico"],
    "Brazil": ["brazil"],
    "Indonesia": ["indonesia"],
    "Korea": ["korea", "korean", "south korea", "republic of korea"],
}

def detect_country(text: str) -> str:
    t = (text or "").lower()
    for country, keys in COUNTRY_KEYWORDS.items():
        if any(k in t for k in keys):
            return country
    return ""

# ===============================
# TRIGGER / EXPOSURE / ACTION (EXEC)
# ===============================
def infer_trigger(text: str) -> str:
    t = (text or "").lower()
    if "international emergency economic powers act" in t or "ieepa" in t:
        return "IEEPA(국제비상경제권한법)"
    if "trade expansion act" in t or "section 232" in t:
        return "Trade Expansion Act / Section 232"
    if "section 301" in t:
        return "Section 301(무역법)"
    if "tariff act" in t:
        return "Tariff Act(관세법 체계)"
    if "hs" in t or "hs code" in t:
        return "HS/품목 분류"
    if "tariff" in t or "관세율" in t or "추가관세" in t or "duty" in t:
        return "관세율/추가관세"
    if "export control" in t or "entity list" in t:
        return "수출통제/제재"
    if "anti-dumping" in t or "countervailing" in t or "safeguard" in t:
        return "무역구제(AD/CVD/SG)"
    if "fta" in t or "원산지" in t:
        return "FTA/원산지"
    return "통상 정책"

def infer_exposure(country: str) -> str:
    c = (country or "").strip()
    if c == "USA":
        return "미국 판매/수입 + 제3국 생산(VN/IN/MX) 노출 우선 점검"
    if c == "India":
        return "인도 생산/판매 영향 + HS/관세율 변동 여부 우선 점검"
    if c in ["Vietnam", "Mexico", "Brazil", "Türkiye", "China", "EU", "Indonesia", "Korea", "Netherlands"]:
        return f"{c} 관련 생산/수출입 및 규정 변화 여부 우선 점검"
    return "주요 생산법인(상위 3개) 우선 스크리닝 후 판매법인 확장"

def build_action_48h(trigger: str, country: str) -> str:
    return (
        "1) 적용 시점/대상국/대상품목(HS) 확인<br/>"
        "2) 생산→판매 법인 순서로 원가/마진/리드타임 1차 산정<br/>"
        "3) 필요 시 HS/원산지/가격(계약조건) 시나리오 점검 및 HQ 대응 착수"
    )

# ===============================
# TOP3 POLICY FILTER
# ===============================
ALLOW = [
    "관세", "tariff", "관세율", "hs", "hs code",
    "section 232", "trade expansion act",
    "section 301",
    "ieepa", "international emergency economic powers act",
    "tariff act",
    "fta", "원산지",
    "무역구제", "anti-dumping", "countervailing", "safeguard",
    "수출통제", "export control",
    "sanction", "entity list",
    "통관", "customs",
    "duties", "duty"
]
BLOCK = [
    "시위", "protest", "체포", "arrest", "충돌", "violent",
    "immigration", "ice raid", "연방정부", "주정부"
]

def is_trade_policy_related(title: str, summary: str) -> bool:
    blob = f"{title} {summary}".lower()
    if any(b in blob for b in BLOCK):
        return False
    return any(a in blob for a in ALLOW)

def is_valid_top3(r):
    blob = f"{r.get('헤드라인','')} {r.get('주요내용','')}".lower()
    if any(b in blob for b in BLOCK):
        return False
    return any(a in blob for a in ALLOW)

# ===============================
# vNext: 원문 메타 요약 추출
# ===============================
def safe_text(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def fetch_meta_summary(url: str) -> str:
    """
    원문 페이지에서 og:description / meta[name=description] 추출
    실패하면 "" 반환
    """
    try:
        headers = {"User-Agent": USER_AGENT}
        resp = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT, allow_redirects=True)
        if resp.status_code != 200 or not resp.text:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")

        # og:description 우선
        og = soup.find("meta", property="og:description")
        if og and og.get("content"):
            return safe_text(og["content"])

        # meta description
        md = soup.find("meta", attrs={"name": "description"})
        if md and md.get("content"):
            return safe_text(md["content"])

        return ""
    except Exception:
        return ""

def clean_rss_summary(title: str, summary: str) -> str:
    """
    RSS 특성상 summary가 title과 같거나 title을 포함하는 경우가 많아 중복을 완화.
    """
    t = safe_text(title)
    s = safe_text(re.sub(r"<[^>]+>", "", summary or ""))

    if not s:
        return ""
    if s == t:
        return ""
    if t and t in s:
        s2 = safe_text(re.sub(re.escape(t), "", s)).strip(" -–—|:·")
        if len(s2) >= 20:
            return s2
    return s

# ===============================
# SENSOR (완전 자동 / 07~07 필터 + 메타요약 대체)
# ===============================
def run_sensor_build_df() -> pd.DataFrame:
    """
    Google News RSS 기반 NEWS_QUERY 관련 뉴스 수집 → DF 생성
    수집기간: 전날 07:00(KST) ~ 당일 07:00(KST)
    vNext: 요약이 빈약하면 원문 메타 설명으로 대체
    """
    rss = "https://news.google.com/rss/search?" + urllib.parse.urlencode({
        "q": NEWS_QUERY,
        "hl": "ko",
        "gl": "KR",
        "ceid": "KR:ko"
    })

    feed = feedparser.parse(rss)
    start, end = window_kst_07_to_07()

    rows = []
    meta_fetch_count = 0

    for e in feed.entries[:NEWS_LIMIT]:
        title = safe_text(getattr(e, "title", ""))
        link = safe_text(getattr(e, "link", ""))

        if not link:
            continue

        published_kst = parse_published_to_kst(e)
        if published_kst is None:
            # 추정 금지
            continue

        # 07~07 범위
        if not (start <= published_kst < end):
            continue

        rss_summary_raw = getattr(e, "summary", "") or ""
        rss_summary = clean_rss_summary(title, rss_summary_raw)

        # 정책 관련성 판단은 RSS 기반으로 1차 (원문 호출 전에 불필요 트래픽 줄임)
        if not is_trade_policy_related(title, rss_summary):
            continue

        final_summary = rss_summary

        # vNext: 요약이 없거나 너무 짧으면 메타요약 시도(최대 FETCH_META_MAX건)
        if (not final_summary or len(final_summary) < 60) and meta_fetch_count < FETCH_META_MAX:
            meta = fetch_meta_summary(link)
            meta_fetch_count += 1
            # 메타가 유의미하면 대체
            if meta and meta.lower() != title.lower() and len(meta) >= 60:
                final_summary = meta

        # 그래도 없으면 최소 문구(원하시면 빈칸으로 바꾸셔도 됩니다)
        if not final_summary:
            final_summary = "요약정보 부족(원문 링크 확인 필요)"

        country = detect_country(f"{title} {final_summary}")
        score = calc_policy_score(title, final_summary)
        importance = score_to_importance(score)

        rows.append({
            "제시어": NEWS_QUERY,
            "헤드라인": title,
            "주요내용": final_summary[:600],
            "발표일": published_kst.strftime("%Y-%m-%d %H:%M"),
            "대상 국가": country,
            "관련 기관": "",  # 추정 금지(원문 파싱으로 개선 가능하지만 현재는 빈칸 유지)
            "출처(URL)": link,
            "중요도": importance,
            "비고": "",
            "근거건수": 1,
            "점수": score,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates(subset=["헤드라인", "출처(URL)"], keep="first")
        df = df.sort_values(["점수", "발표일"], ascending=[False, False])

    return df

# ===============================
# SAFE COLUMNS / ORDER
# ===============================
OUT_COLS = [
    "주요내용", "발표일", "대상 국가", "관련 기관", "출처(URL)", "중요도", "비고",
    "제시어", "헤드라인", "근거건수", "점수"
]

def ensure_cols(df):
    df = df.copy()

    if "점수" not in df.columns:
        df["점수"] = 1
    if "중요도" not in df.columns:
        df["중요도"] = df["점수"].apply(score_to_importance)

    for c in ["관련 기관", "비고", "근거건수", "제시어", "헤드라인", "주요내용", "발표일", "대상 국가", "출처(URL)"]:
        if c not in df.columns:
            df[c] = ""

    for c in OUT_COLS:
        if c not in df.columns:
            df[c] = ""

    df = df[OUT_COLS]
    return df

# ===============================
# LINK
# ===============================
def get_link(r):
    for c in ["출처(URL)", "URL", "link", "원본링크", "originallink"]:
        if c in r and pd.notna(r[c]) and str(r[c]).strip():
            return str(r[c]).strip()
    return "#"

# ===============================
# HTML STYLE
# ===============================
STYLE = """
<style>
body{font-family:Malgun Gothic,Arial; background:#f6f6f6;}
.page{max-width:1120px;margin:auto;background:white;padding:14px;}
h2{margin-bottom:4px;}
.box{border:1px solid #ddd;border-radius:8px;padding:12px;margin:12px 0;}
li{margin-bottom:14px;}
table{border-collapse:collapse;width:100%;}
th,td{border:1px solid #ccc;padding:8px;font-size:12px;vertical-align:top;}
th{background:#f0f0f0;}
.small{font-size:11px;color:#555;}
.badge{display:inline-block;padding:2px 8px;border-radius:999px;font-size:11px;border:1px solid #ddd;margin-right:6px;}
</style>
"""

# ===============================
# HTML BUILD (실무자용: 표 중심)
# ===============================
def build_html_practitioner(df):
    date = now_kst().strftime("%Y-%m-%d")
    start, end = window_kst_07_to_07()

    cand = df[df.apply(is_valid_top3, axis=1)] if not df.empty else df
    top3 = cand.sort_values("점수", ascending=False).head(3) if not cand.empty else df.head(3)

    top3_html = ""
    for _, r in top3.iterrows():
        top3_html += f"""
        <li>
          <span class="badge">중요도 {html.escape(str(r.get('중요도','')))}</span>
          <span class="badge">점수 {html.escape(str(r.get('점수','')))}</span>
          <b>[{html.escape(str(r.get('대상 국가','') or 'N/A'))}]</b><br/>
          <a href="{get_link(r)}" target="_blank">{html.escape(str(r.get('헤드라인','')))}</a><br/>
          <div class="small">{html.escape(str(r.get('주요내용',''))[:260])}</div>
        </li>
        """

    rows = ""
    for _, r in df.iterrows():
        rows += f"""
        <tr>
          <td>{html.escape(str(r.get('주요내용',''))[:500])}</td>
          <td>{html.escape(str(r.get('발표일','')))}</td>
          <td>{html.escape(str(r.get('대상 국가','')))}</td>
          <td>{html.escape(str(r.get('관련 기관','')))}</td>
          <td><a href="{get_link(r)}" target="_blank">Link</a></td>
          <td>{html.escape(str(r.get('중요도','')))}</td>
          <td>{html.escape(str(r.get('비고','')))}</td>
        </tr>
        """

    return f"""
    <html>
    <head>{STYLE}</head>
    <body>
    <div class="page">
      <h2>관세·통상 데일리 브리프 ({date})</h2>
      <div class="small">수집기간: {start.strftime("%Y-%m-%d %H:%M")} ~ {end.strftime("%Y-%m-%d %H:%M")} (KST)</div>

      <div class="box">
        <h3>① 오늘의 핵심 TOP3</h3>
        <ul>{top3_html}</ul>
      </div>

      <div class="box">
        <h3>② 정책 센서 표 (실무자용)</h3>
        <table>
          <tr>
            <th>주요내용</th>
            <th>발표일</th>
            <th>대상 국가</th>
            <th>관련 기관</th>
            <th>출처(URL)</th>
            <th>중요도</th>
            <th>비고</th>
          </tr>
          {rows}
        </table>
      </div>
    </div>
    </body>
    </html>
    """

# ===============================
# HTML BUILD (임원용: 의사결정형)
# ===============================
def build_html_exec(df):
    date = now_kst().strftime("%Y-%m-%d")
    start, end = window_kst_07_to_07()

    cand = df[df.apply(is_valid_top3, axis=1)] if not df.empty else df
    top3 = cand.sort_values("점수", ascending=False).head(3) if not cand.empty else df.sort_values("점수", ascending=False).head(3)

    items = ""
    for _, r in top3.iterrows():
        headline = str(r.get("헤드라인",""))
        summary = str(r.get("주요내용",""))
        country = str(r.get("대상 국가","") or "")

        trigger = infer_trigger(headline + " " + summary)
        exposure = infer_exposure(country)
        action = build_action_48h(trigger, country)

        items += f"""
        <li style="margin-bottom:18px;">
          <span class="badge">중요도 {html.escape(str(r.get('중요도','')))}</span>
          <span class="badge">점수 {html.escape(str(r.get('점수','')))}</span>
          <b>[{html.escape(country or 'N/A')}]</b><br/>
          <a href="{get_link(r)}" target="_blank">{html.escape(headline)}</a><br/>
          <div class="small">{html.escape(summary[:240])}</div>
          <div style="margin-top:8px;font-size:12px;">
            <b>Trigger:</b> {html.escape(trigger)}<br/>
            <b>Exposure:</b> {html.escape(exposure)}<br/>
            <b>Action(48h):</b><br/>{action}
          </div>
        </li>
        """

    return f"""
    <html><head>{STYLE}</head>
    <body>
      <div class="page">
        <h2>[Executive] 관세·통상 핵심 TOP3 ({date})</h2>
        <div class="small">수집기간: {start.strftime("%Y-%m-%d %H:%M")} ~ {end.strftime("%Y-%m-%d %H:%M")} (KST)</div>

        <div class="box">
          <ul>{items}</ul>
        </div>

        <div class="box" style="font-size:12px;">
          <b>HQ 공통 체크(요약)</b><br/>
          1) 정책 근거(법/고시/행정명령) 및 시행/적용 시점 구분<br/>
          2) 대상국·대상품목(HS)·거래유형(수입/수출/부품) 매핑<br/>
          3) 생산→판매 법인 순서로 영향(원가/마진/리드타임/특혜관세 실패·추징) 1차 산정
        </div>
      </div>
    </body></html>
    """

# ===============================
# WRITE OUTPUTS (CSV/XLSX/HTML)
# ===============================
def write_outputs(df, html_body):
    today = now_kst().strftime("%Y-%m-%d")
    csv_path  = os.path.join(BASE_DIR, f"policy_events_{today}.csv")
    xlsx_path = os.path.join(BASE_DIR, f"policy_events_{today}.xlsx")
    html_path = os.path.join(BASE_DIR, f"policy_events_{today}.html")

    try:
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    except TypeError:
        df.to_csv(csv_path, index=False)

    df.to_excel(xlsx_path, index=False)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_body)

    return csv_path, xlsx_path, html_path

# ===============================
# MAIL (공용)
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
    today = now_kst().strftime("%Y-%m-%d")

    df = run_sensor_build_df()
    if df is None or df.empty:
        print("오늘 수집된 이벤트/뉴스 없음 (07~07 KST)")
        print("BASE_DIR =", BASE_DIR)
        print("OUT_FILES =", os.listdir(BASE_DIR))
        return

    df = ensure_cols(df)

    # 실무자용
    html_body = build_html_practitioner(df)
    write_outputs(df, html_body)
    send_mail_to(RECIPIENTS, f"관세·통상 데일리 브리프 ({today})", html_body)

    # 임원용
    exec_html = build_html_exec(df)
    send_mail_to(RECIPIENTS_EXEC, f"[Executive] 관세·통상 핵심 TOP3 ({today})", exec_html)

    print("✅ vNext 완료: 메타요약 적용 + 실무/임원 분리 발송")
    print("BASE_DIR =", BASE_DIR)
    print("OUT_FILES =", os.listdir(BASE_DIR))
    print("FETCH_META_MAX =", FETCH_META_MAX)

if __name__ == "__main__":
    main()
