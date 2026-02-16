# -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief
E2E: Sensor + Outputs + Mail (Practitioner + Executive)

- Google News RSS 기반 센서 (PC 없이 GitHub Actions에서 구동)
- out/에 CSV/XLSX/HTML 저장
- 실무자용 메일 + 임원용 TOP3 메일 분리
- 정책성 점수(리스크 스코어) 고도화
"""

# ===============================
# IMPORT
# ===============================
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

RECIPIENTS = [x.strip() for x in os.getenv("RECIPIENTS", "").split(",") if x.strip()]
RECIPIENTS_EXEC = [x.strip() for x in os.getenv("RECIPIENTS_EXEC", "").split(",") if x.strip()]

BASE_DIR = os.getenv("BASE_DIR", os.path.join(os.path.dirname(__file__), "out"))
os.makedirs(BASE_DIR, exist_ok=True)

# ===============================
# TIME
# ===============================
def now_kst():
    return dt.datetime.utcnow() + dt.timedelta(hours=9)

# ===============================
# POLICY SCORE (3) 고도화
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
# COUNTRY TAG (2에서 만든 기능 유지)
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
# SENSOR (완전 자동)
# ===============================
def run_sensor_build_df() -> pd.DataFrame:
    """
    Google News RSS 기반 '관세' 관련 뉴스 수집 → DF 생성
    """
    query = os.getenv("NEWS_QUERY", "관세")

    rss = "https://news.google.com/rss/search?" + urllib.parse.urlencode({
        "q": query,
        "hl": "ko",
        "gl": "KR",
        "ceid": "KR:ko"
    })

    feed = feedparser.parse(rss)

    rows = []
    for e in feed.entries[:30]:
        title = getattr(e, "title", "").strip()
        link = getattr(e, "link", "").strip()
        published = getattr(e, "published", "")

        summary = getattr(e, "summary", "")
        summary = re.sub(r"<[^>]+>", "", summary).strip()

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
# LOAD EVENTS (기존 파일 있으면 활용)
# ===============================
def load_events():
    today = now_kst().strftime("%Y-%m-%d")
    path = os.path.join(BASE_DIR, f"policy_events_{today}.csv")

    if os.path.exists(path):
        return pd.read_csv(path)

    files = sorted(
        f for f in os.listdir(BASE_DIR)
        if f.startswith("policy_events_") and f.endswith(".csv")
    )
    if not files:
        return pd.DataFrame()

    path = os.path.join(BASE_DIR, files[-1])
    return pd.read_csv(path)

# ===============================
# SAFE COLUMNS
# ===============================
def ensure_cols(df):
    df = df.copy()

    # 점수는 센서에서 만들면 유지, 없으면 기본 매핑
    if "점수" not in df.columns:
        score_map = {"상": 9, "중": 6, "하": 3}
        df["점수"] = df.get("중요도", "하").map(score_map).fillna(1)

    if "제시어" not in df.columns:
        for c in ["policy_keyword", "keyword", "카테고리", "분류"]:
            if c in df.columns:
                df["제시어"] = df[c]
                break
        else:
            df["제시어"] = "관세"

    return df

# ===============================
# LINK
# ===============================
def get_link(r):
    for c in ["출처(URL)", "URL", "link", "원본링크", "originallink"]:
        if c in r and pd.notna(r[c]):
            return r[c]
    return "#"

# ===============================
# TOP3 POLICY FILTER
# ===============================
ALLOW = [
    "관세","tariff","관세율","hs","section 232","section 301","ieepa",
    "fta","원산지","무역구제","수출통제","export control","sanction","통관","customs"
]
BLOCK = [
    "시위","protest","체포","arrest","충돌","violent",
    "immigration","ice raid","연방정부","주정부"
]

def is_valid_top3(r):
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
# HTML BUILD (실무자용)
# ===============================
def build_html(df):
    date = now_kst().strftime("%Y-%m-%d")

    cand = df[df.apply(is_valid_top3, axis=1)]
    top3 = cand.sort_values("점수", ascending=False).head(3)

    top3_html = ""
    for _, r in top3.iterrows():
        top3_html += f"""
        <li>
          <b>[{r['제시어']}｜{r.get('대상 국가','')}｜점수 {r['점수']}]</b><br/>
          <a href="{get_link(r)}" target="_blank">{html.escape(str(r['헤드라인']))}</a><br/>
          <div class="small">{html.escape(str(r.get('주요내용',''))[:260])}</div>
        </li>
        """

    why_html = ""
    for _, r in top3.iterrows():
        why_html += f"<li>[{r['제시어']} | 근거 {r.get('근거건수',1)}건] 정책 변화 가능성으로 원가·마진·리드타임 영향</li>"

    chk_html = ""
    for _, r in top3.iterrows():
        chk_html += f"""
        <li>
        [{r['제시어']}｜{r.get('대상 국가','')}｜점수 {r['점수']}]
        영향: 정책 변화 가능성으로 원가·마진·리드타임 영향 →
        조치: 1) HS/대상국 확인 → 2) 법인 영향 산정 → 3) 체크리스트 업데이트
        </li>
        """

    rows = ""
    for _, r in df.iterrows():
        rows += f"""
        <tr>
          <td>{r.get('제시어','')} ({r.get('중요도','')})</td>
          <td>
            <a href="{get_link(r)}" target="_blank">{html.escape(str(r.get('헤드라인','')))}</a><br/>
            {html.escape(str(r.get('주요내용','')))}
          </td>
          <td>{r.get('발표일','')}</td>
          <td>{r.get('대상 국가','')}</td>
          <td>점수 {r.get('점수','')}</td>
        </tr>
        """

    return f"""
    <html>
    <head>{STYLE}</head>
    <body>
    <div class="page">
      <h2>관세·무역 뉴스 브리핑 ({date})</h2>

      <div class="box">
        <h3>① 오늘의 핵심 정책 이벤트 TOP3</h3>
        <ul>{top3_html}</ul>
      </div>

      <div class="box">
        <h3>② 왜 중요한가</h3>
        <ul>{why_html}</ul>
      </div>

      <div class="box">
        <h3>③ 당사 관점 체크포인트</h3>
        <ul>{chk_html}</ul>
      </div>

      <div class="box">
        <h3>📊 정책 센서 전용 표</h3>
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
# HTML BUILD (임원용)
# ===============================
def build_html_exec(df):
    date = now_kst().strftime("%Y-%m-%d")
    cand = df[df.apply(is_valid_top3, axis=1)]
    top3 = cand.sort_values("점수", ascending=False).head(3)

    items = ""
    for _, r in top3.iterrows():
        items += f"""
        <li>
          <b>[{r.get('대상 국가','')} | 점수 {r.get('점수','')}]</b><br/>
          <a href="{get_link(r)}" target="_blank">{html.escape(str(r.get('헤드라인','')))}</a><br/>
          <div class="small">{html.escape(str(r.get('주요내용',''))[:220])}</div>
        </li>
        """

    return f"""
    <html><head>{STYLE}</head>
    <body>
      <div class="page">
        <h2>[Executive] 관세·통상 핵심 TOP3 ({date})</h2>
        <div class="box">
          <ul>{items}</ul>
        </div>
        <div class="box">
          <b>Action</b><br/>
          1) 대상국/품목(HS) 확인 → 2) 법인 영향(원가/마진/리드타임) 1차 산정 → 3) 필요 시 HQ 리스크 대응 착수
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
# MAIL (실무/임원 공용)
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
    today_csv = os.path.join(BASE_DIR, f"policy_events_{today}.csv")

    # 1) 오늘 CSV 있으면 사용, 없으면 센서 실행
    if os.path.exists(today_csv):
        df = load_events()
    else:
        df = run_sensor_build_df()

    if df is None or df.empty:
        print("오늘 수집된 이벤트/뉴스 없음")
        return

    df = ensure_cols(df)

    # 실무자용
    html_body = build_html(df)
    write_outputs(df, html_body)
    send_mail_to(RECIPIENTS, f"관세·무역 뉴스 브리핑 ({today})", html_body)

    # 임원용
    exec_html = build_html_exec(df)
    send_mail_to(RECIPIENTS_EXEC, f"[Executive] 관세·통상 핵심 TOP3 ({today})", exec_html)

    print("✅ 점수 고도화 + 임원/실무 분리 발송 완료")
    print("BASE_DIR =", BASE_DIR)
    print("OUT_FILES =", os.listdir(BASE_DIR))

if __name__ == "__main__":
    main()
