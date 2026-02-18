# -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief
STABLE - NO SUMMARY VERSION

✔ TOP3 요약 제거
✔ 표 요약 제거
✔ 중복 제거
✔ 제시어별 10건 제한
✔ A4 가로 레이아웃
✔ 임원/실무 분리
"""

import os, re, html, smtplib
import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urlencode

import pandas as pd
import feedparser

# ===============================
# ENV
# ===============================
SMTP_SERVER   = os.getenv("SMTP_SERVER", "")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_EMAIL    = os.getenv("SMTP_EMAIL", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")

RECIPIENTS = [x.strip() for x in os.getenv("RECIPIENTS", "").split(",") if x.strip()]
RECIPIENTS_EXEC = [x.strip() for x in os.getenv("RECIPIENTS_EXEC", "").split(",") if x.strip()]

BASE_DIR = os.getenv("BASE_DIR", os.path.join(os.path.dirname(__file__), "out"))
os.makedirs(BASE_DIR, exist_ok=True)

CUSTOM_QUERIES_FILE = os.path.join(os.path.dirname(__file__), "custom_queries.TXT")

# ===============================
# TIME
# ===============================
def now_kst():
    return dt.datetime.utcnow() + dt.timedelta(hours=9)

def today_str():
    return now_kst().strftime("%Y-%m-%d")

# ===============================
# LOAD QUERIES
# ===============================
def load_queries():
    if not os.path.exists(CUSTOM_QUERIES_FILE):
        return ["관세"]
    with open(CUSTOM_QUERIES_FILE, "r", encoding="utf-8") as f:
        lines = [x.strip() for x in f if x.strip()]
    return list(dict.fromkeys(lines))

# ===============================
# RSS
# ===============================
LANGS = [
    ("ko","KR","KR:ko"),
    ("en","US","US:en"),
    ("fr","FR","FR:fr"),
]

def build_rss(query, hl, gl, ceid):
    return "https://news.google.com/rss/search?" + urlencode({
        "q": query,
        "hl": hl,
        "gl": gl,
        "ceid": ceid
    })

def fetch_news(query):
    rows = []
    for hl, gl, ceid in LANGS:
        feed = feedparser.parse(build_rss(query, hl, gl, ceid))
        for e in feed.entries[:30]:
            rows.append({
                "query": query,
                "title": getattr(e,"title","").strip(),
                "link": getattr(e,"link","").strip(),
                "published": getattr(e,"published",""),
            })
    return rows

# ===============================
# DEDUP
# ===============================
def norm(s):
    s = (s or "").lower()
    s = re.sub(r"\s+"," ",s)
    return re.sub(r"[^\w가-힣]","",s)

def dedup(df):
    df["k"] = df["title"].apply(norm)
    df = df.drop_duplicates(subset=["k"])
    return df.drop(columns=["k"])

# ===============================
# SCORE
# ===============================
KEYWORDS = [
    "관세","tariff","duty","customs",
    "hs","section 232","section 301",
    "ieepa","export control","sanction"
]

def score(title):
    t = title.lower()
    s = 1
    for k in KEYWORDS:
        if k in t:
            s += 3
    return s

# ===============================
# HTML STYLE (A4 가로 느낌)
# ===============================
STYLE = """
<style>
body{font-family:Malgun Gothic,Arial;background:#f6f6f6;}
.page{max-width:1120px;margin:auto;background:white;padding:14px;}
.box{border:1px solid #ddd;border-radius:8px;padding:12px;margin:12px 0;}
table{border-collapse:collapse;width:100%;}
th,td{border:1px solid #ccc;padding:6px;font-size:12px;}
th{background:#f0f0f0;}
</style>
"""

# ===============================
# BUILD TOP3 (요약 없음)
# ===============================
def build_top3(df):
    top3 = df.sort_values("score",ascending=False).head(3)

    items = ""
    for _, r in top3.iterrows():
        items += f"""
        <li>
          <a href="{r['link']}" target="_blank">
          {html.escape(r['title'])}
          </a>
        </li>
        """
    return f"""
    <div class="box">
      <h3>① 관세·통상 핵심 TOP3</h3>
      <ul>{items}</ul>
    </div>
    """

# ===============================
# BUILD TABLE (요약 없음)
# ===============================
def build_table(df):
    df = df.sort_values(["query","score"],ascending=[True,False])
    df = df.groupby("query").head(10)

    counts = df.groupby("query").size().to_dict()
    count_line = ", ".join([f"{k} {v}건" for k,v in counts.items()])

    rows = ""
    for _, r in df.iterrows():
        rows += f"""
        <tr>
          <td>{html.escape(r['query'])}</td>
          <td>
            <a href="{r['link']}" target="_blank">
            {html.escape(r['title'])}
            </a>
          </td>
          <td>{r['published']}</td>
          <td>점수 {r['score']}</td>
        </tr>
        """

    return f"""
    <div class="box">
      <h3>④ 정책 이벤트 표</h3>
      <div style="font-size:11px;margin-bottom:8px;">
        제시어별 주요뉴스 건수: {count_line}
      </div>
      <table>
        <tr>
          <th>제시어</th>
          <th>헤드라인</th>
          <th>발표일</th>
          <th>비고</th>
        </tr>
        {rows}
      </table>
    </div>
    """

# ===============================
# BUILD HTML
# ===============================
def build_html(df, executive=False):
    date = today_str()

    html_body = f"""
    <html><head>{STYLE}</head>
    <body><div class="page">
      <h2>관세·무역 뉴스 브리핑 ({date})</h2>
    """

    html_body += build_top3(df)

    if not executive:
        html_body += build_table(df)

    html_body += "</div></body></html>"
    return html_body

# ===============================
# MAIL
# ===============================
def send_mail(recipients, subject, html_body):
    if not recipients:
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_EMAIL
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_body,"html","utf-8"))

    with smtplib.SMTP(SMTP_SERVER,SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_EMAIL,SMTP_PASSWORD)
        s.sendmail(SMTP_EMAIL,recipients,msg.as_string())

# ===============================
# MAIN
# ===============================
def main():

    queries = load_queries()

    rows = []
    for q in queries:
        rows.extend(fetch_news(q))

    df = pd.DataFrame(rows)

    if df.empty:
        print("No news")
        return

    df = dedup(df)
    df["score"] = df["title"].apply(score)

    html_exec = build_html(df, executive=True)
    html_prac = build_html(df, executive=False)

    today = today_str()

    send_mail(RECIPIENTS_EXEC, f"[Executive] 관세·통상 핵심 TOP3 ({today})", html_exec)
    send_mail(RECIPIENTS, f"관세·무역 뉴스 브리핑 ({today})", html_prac)

    print("DONE")

if __name__ == "__main__":
    main()
