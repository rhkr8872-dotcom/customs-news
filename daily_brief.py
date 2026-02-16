# -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief
E2E: Sensor + Outputs + Mail (Practitioner + Executive)

Fix 포함:
1) 임원용 메일 발송 (RECIPIENTS_EXEC env + workflow 반영 필요)
2) Google RSS summary가 title 반복되는 문제 완화 (clean_summary)
4) 수집 윈도우 07:00~07:00(KST) + 발송은 Actions cron으로 08:00(KST)

NOTE
- 본문까지 크롤링/요약은 vNext에서 확장 권장(차단/로봇/속도 이슈)
"""

import os
import re
import html
import smtplib
import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import parsedate_to_datetime

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

RECIPIENTS = [x.strip() for x in os.getenv("RECIPIENTS", "").split(",") if x.strip()]
RECIPIENTS_EXEC = [x.strip() for x in os.getenv("RECIPIENTS_EXEC", "").split(",") if x.strip()]

BASE_DIR = os.getenv("BASE_DIR", os.path.join(os.path.dirname(__file__), "out"))
os.makedirs(BASE_DIR, exist_ok=True)


# ===============================
# TIME
# ===============================
def now_kst() -> dt.datetime:
    return dt.datetime.utcnow() + dt.timedelta(hours=9)


def window_07_to_07_kst():
    """
    어제 07:00(KST) ~ 오늘 07:00(KST)
    """
    now = now_kst()
    end = now.replace(hour=7, minute=0, second=0, microsecond=0)
    if now < end:
        end = end - dt.timedelta(days=1)
    start = end - dt.timedelta(days=1)
    return start, end


def parse_published_to_kst_naive(published_str: str):
    if not published_str:
        return None
    try:
        dtu = parsedate_to_datetime(published_str)
        if dtu.tzinfo is None:
            dtu = dtu.replace(tzinfo=dt.timezone.utc)
        kst = dt.timezone(dt.timedelta(hours=9))
        dt_kst = dtu.astimezone(kst).replace(tzinfo=None)
        return dt_kst
    except Exception:
        return None


def in_window_07_to_07(published_str: str) -> bool:
    """
    RSS published가 어제07~오늘07(KST) 범위면 True
    published가 없거나 파싱 실패면 True(일단 포함)
    """
    dt_kst = parse_published_to_kst_naive(published_str)
    if dt_kst is None:
        return True
    start, end = window_07_to_07_kst()
    return (start <= dt_kst < end)


# ===============================
# SUMMARY CLEAN (Fix #2)
# ===============================
def clean_summary(title: str, summary: str) -> str:
    title = (title or "").strip()
    summary = (summary or "").strip()

    # HTML 제거
    summary = re.sub(r"<[^>]+>", " ", summary)
    summary = re.sub(r"\s+", " ", summary).strip()

    # 요약이 제목 반복/포함이면 제거 시도
    if title and summary:
        if summary == title:
            summary = ""
        else:
            summary2 = summary.replace(title, "").strip(" -–—:|")
            if len(summary2) >= 40:
                summary = summary2

    # 너무 짧으면 대체 문구
    if len(summary) < 40:
        summary = "요약 정보가 제한적입니다. 원문 링크에서 세부 내용을 확인하세요."

    return summary


# ===============================
# POLICY SCORE
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


# ===============================
# COUNTRY TAG
# ===============================
COUNTRY_KEYWORDS = {
    "USA": ["u.s.", "united states", "america", "section 301", "section 232"],
    "India": ["india"],
    "Türkiye": ["turkey", "türkiye"],
    "Vietnam": ["vietnam"],
    "Netherlands": ["netherlands", "dutch"],
    "EU": ["european union", "eu commission", "european commission"],
    "China": ["china"],
    "Mexico": ["mexico"],
    "Brazil": ["brazil"],
}


def detect_country(text: str) -> str:
    t = (text or "").lower()
    for country, keys in COUNTRY_KEYWORDS.items():
        if any(k in t for k in keys):
            return country
    return ""


# ===============================
# SENSOR (Google News RSS)
# ===============================
def run_sensor_build_df() -> pd.DataFrame:
    query = os.getenv("NEWS_QUERY", "관세")

    rss = "https://news.google.com/rss/search?" + urllib.parse.urlencode({
        "q": query,
        "hl": "ko",
        "gl": "KR",
        "ceid": "KR:ko"
    })

    feed = feedparser.parse(rss)

    rows = []
    for e in feed.entries[:50]:
        title = getattr(e, "title", "").strip()
        link = getattr(e, "link", "").strip()
        published = getattr(e, "published", "")

        # 07~07 필터
        if not in_window_07_to_07(published):
            continue

        raw_summary = getattr(e, "summary", "")
        summary = clean_summary(title, raw_summary)

        country = detect_country(f"{title} {summary}")
        score = calc_policy_score(title, summary)

        rows.append({
            "제시어": query,
            "헤드라인": title,
            "주요내용": summary[:500],
            "대상 국가": country,
            "중요도": "중",
            "발표일": published,
            "출처(URL)": link,
            "근거건수": 1,
            "점수": score,
        })

    return pd.DataFrame(rows)


# ===============================
# SAFE COLUMNS
# ===============================
def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "점수" not in df.columns:
        score_map = {"상": 9, "중": 6, "하": 3}
        df["점수"] = df.get("중요도", "하").map(score_map).fillna(1)

    if "제시어" not in df.columns:
        df["제시어"] = os.getenv("NEWS_QUERY", "관세")

    # 비어있는 컬럼 보정
    for col in ["헤드라인", "주요내용", "발표일", "출처(URL)", "대상 국가", "중요도"]:
        if col not in df.columns:
            df[col] = ""

    return df


def get_link(r) -> str:
    for c in ["출처(URL)", "URL", "link", "원본링크", "originallink"]:
        if c in r and pd.notna(r[c]):
            return str(r[c]).strip()
    return "#"


# ===============================
# TOP3 FILTER
# ===============================
ALLOW = [
    "관세","tariff","관세율","hs","section 232","section 301","ieepa",
    "fta","원산지","무역구제","수출통제","export control","sanction","통관","customs"
]
BLOCK = [
    "시위","protest","체포","arrest","충돌","violent",
    "immigration","ice raid","연방정부","주정부"
]


def is_valid_top3(r) -> bool:
    blob = f"{r.get('헤드라인','')} {r.get('주요내용','')}".lower()
    if any(b in blob for b in BLOCK):
        return False
    return any(a in blob for a in ALLOW)


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
</style>
"""


# ===============================
# HTML BUILD (Practitioner)
# ===============================
def build_html(df: pd.DataFrame) -> str:
    date = now_kst().strftime("%Y-%m-%d")
    start, end = window_07_to_07_kst()

    cand = df[df.apply(is_valid_top3, axis=1)]
    top3 = cand.sort_values("점수", ascending=False).head(3)

    top3_html = ""
    for _, r in top3.iterrows():
        top3_html += f"""
        <li>
          <b>[{html.escape(str(r.get('제시어','')))} | {html.escape(str(r.get('대상 국가','')))} | 점수 {html.escape(str(r.get('점수','')))}]</b><br/>
          <a href="{html.escape(get_link(r))}" target="_blank">{html.escape(str(r.get('헤드라인','')))}</a><br/>
          <div class="small">{html.escape(str(r.get('주요내용',''))[:260])}</div>
        </li>
        """

    rows = ""
    for _, r in df.iterrows():
        headline = html.escape(str(r.get("헤드라인", "")))
        summary = html.escape(str(r.get("주요내용", "")))
        link = html.escape(get_link(r))

        # 표 요구에 맞게: 헤드라인에 링크 + 헤드라인/주요내용 1칸에 표기
        headline_block = f'<a href="{link}" target="_blank">{headline}</a><br/>{summary}'

        rows += f"""
        <tr>
          <td>{html.escape(str(r.get('제시어','')))} ({html.escape(str(r.get('중요도','')))})</td>
          <td>{headline_block}</td>
          <td>{html.escape(str(r.get('발표일','')))}</td>
          <td>{html.escape(str(r.get('대상 국가','')))}</td>
          <td>점수 {html.escape(str(r.get('점수','')))}</td>
        </tr>
        """

    return f"""
    <html>
    <head>{STYLE}</head>
    <body>
    <div class="page">
      <h2>관세·무역 뉴스 브리핑 ({date})</h2>
      <div class="small">수집 범위: {start.strftime('%Y-%m-%d %H:%M')} ~ {end.strftime('%Y-%m-%d %H:%M')} (KST)</div>

      <div class="box">
        <h3>① 오늘의 핵심 정책 이벤트 TOP3</h3>
        <ul>{top3_html}</ul>
      </div>

      <div class="box">
        <h3>② 정책 이벤트 표</h3>
        <table>
          <tr>
            <th>제시어(중요도)</th>
            <th>헤드라인 / 주요내용</th>
            <th>발표일</th>
            <th>국가</th>
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
# HTML BUILD (Executive)
# ===============================
def build_html_exec(df: pd.DataFrame) -> str:
    date = now_kst().strftime("%Y-%m-%d")
    start, end = window_07_to_07_kst()

    cand = df[df.apply(is_valid_top3, axis=1)]
    top3 = cand.sort_values("점수", ascending=False).head(3)

    items = ""
    for _, r in top3.iterrows():
        items += f"""
        <li>
          <b>[{html.escape(str(r.get('대상 국가','')))} | 점수 {html.escape(str(r.get('점수','')))}]</b><br/>
          <a href="{html.escape(get_link(r))}" target="_blank">{html.escape(str(r.get('헤드라인','')))}</a><br/>
          <div class="small">{html.escape(str(r.get('주요내용',''))[:220])}</div>
        </li>
        """

    return f"""
    <html>
    <head>{STYLE}</head>
    <body>
      <div class="page">
        <h2>[Executive] 관세·통상 핵심 TOP3 ({date})</h2>
        <div class="small">수집 범위: {start.strftime('%Y-%m-%d %H:%M')} ~ {end.strftime('%Y-%m-%d %H:%M')} (KST)</div>

        <div class="box">
          <ul>{items}</ul>
        </div>

        <div class="box">
          <b>Action</b><br/>
          1) 대상국/품목(HS) 확인 → 2) 법인 영향(원가/마진/리드타임) 1차 산정 → 3) 필요 시 HQ 리스크 대응 착수
        </div>
      </div>
    </body>
    </html>
    """


# ===============================
# OUTPUTS
# ===============================
def write_outputs(df: pd.DataFrame, html_body: str):
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
# MAIL
# ===============================
def send_mail_to(recipients, subject, html_body):
    if not recipients:
        print(f"[SKIP] recipients empty: {subject}")
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
    # 간단 sanity 로그(비번 노출 없음)
    print("BASE_DIR =", BASE_DIR)
    print("RECIPIENTS count =", len(RECIPIENTS))
    print("RECIPIENTS_EXEC count =", len(RECIPIENTS_EXEC))

    df = run_sensor_build_df()
    if df is None or df.empty:
        print("오늘 수집된 이벤트/뉴스 없음")
        return

    df = ensure_cols(df)

    # Practitioner
    html_body = build_html(df)
    write_outputs(df, html_body)
    send_mail_to(RECIPIENTS, f"관세·무역 뉴스 브리핑 ({now_kst().strftime('%Y-%m-%d')})", html_body)

    # Executive
    exec_html = build_html_exec(df)
    send_mail_to(RECIPIENTS_EXEC, f"[Executive] 관세·통상 핵심 TOP3 ({now_kst().strftime('%Y-%m-%d')})", exec_html)

    print("✅ 완료")


if __name__ == "__main__":
    main()
