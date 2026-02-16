# -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief
# vNext STABLE – 07~07 Filter + Exec Insight Integrated
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
print("RECIPIENTS count =", len(RECIPIENTS))
print("RECIPIENTS_EXEC count =", len(RECIPIENTS_EXEC))
