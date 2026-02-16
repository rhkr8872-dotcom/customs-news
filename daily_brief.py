 # -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief
FINAL v5.18.6.6 – FORM FINAL (SAMPLE.mht REPLICA)

✔ Sensor logic: NO CHANGE
✔ Output FORM only refinement
✔ TOP3 policy relevance filter applied
"""

# ===============================
# IMPORT
# ===============================
import os, re, sys, html, smtplib, traceback
import datetime as dt
from typing import List
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd

# ===============================
# ENV
# ===============================
SMTP_SERVER   = os.getenv("SMTP_SERVER")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_EMAIL    = os.getenv("SMTP_EMAIL")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
RECIPIENTS    = [x.strip() for x in os.getenv("RECIPIENTS","").split(",") if x.strip()]
BASE_DIR = os.getenv("BASE_DIR", os.path.join(os.path.dirname(__file__), "out"))
os.makedirs(BASE_DIR, exist_ok=True)
# ===============================
# TIME
# ===============================
def now_kst():
    return dt.datetime.utcnow() + dt.timedelta(hours=9)

# ===============================
# LOAD EVENTS (UNCHANGED)
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
# SAFE COLUMNS (FORM ONLY)
# ===============================
def ensure_cols(df):
    df = df.copy()

    if "점수" not in df.columns:
        score_map = {"상":9,"중":6,"하":3}
        df["점수"] = df.get("중요도","하").map(score_map).fillna(1)

    if "제시어" not in df.columns:
        for c in ["policy_keyword","keyword","카테고리","분류"]:
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
    for c in ["출처(URL)","URL","link","원본링크","originallink"]:
        if c in r and pd.notna(r[c]):
            return r[c]
    return "#"

# ===============================
# TOP3 POLICY FILTER (FORM ONLY)
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
# HTML BUILD
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

def build_html(df):
    date = now_kst().strftime("%Y-%m-%d")

    # ---------- TOP3 ----------
    cand = df[df.apply(is_valid_top3, axis=1)]
    top3 = cand.sort_values("점수", ascending=False).head(3)

    top3_html = ""
    for _, r in top3.iterrows():
        top3_html += f"""
        <li>
          <b>[{r['제시어']}｜{r.get('대상 국가','')}｜점수 {r['점수']}]</b><br/>
          <a href="{get_link(r)}" target="_blank">{html.escape(r['헤드라인'])}</a><br/>
          <div class="small">{html.escape(str(r.get('주요내용',''))[:260])}</div>
        </li>
        """

    # ---------- WHY ----------
    why_html = ""
    for _, r in top3.iterrows():
        why_html += f"<li>[{r['제시어']} | 근거 {r.get('근거건수',1)}건] 정책 변화 가능성으로 원가·마진·리드타임 영향</li>"

    # ---------- CHECK ----------
    chk_html = ""
    for _, r in top3.iterrows():
        chk_html += f"""
        <li>
        [{r['제시어']}｜{r.get('대상 국가','')}｜점수 {r['점수']}]
        영향: 정책 변화 가능성으로 원가·마진·리드타임 영향 →
        조치: 1) HS/대상국 확인 → 2) 법인 영향 산정 → 3) 체크리스트 업데이트
        </li>
        """

    # ---------- TABLE ----------
    rows = ""
    for _, r in df.iterrows():
        rows += f"""
        <tr>
          <td>{r['제시어']} ({r.get('중요도','')})</td>
          <td>
            <a href="{get_link(r)}" target="_blank">{html.escape(r['헤드라인'])}</a><br/>
            {html.escape(str(r.get('주요내용','')))}
          </td>
          <td>{r.get('발표일','')}</td>
          <td>{r.get('대상 국가','')}</td>
          <td>점수 {r['점수']}</td>
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
# MAIL
# ===============================

# ===============================
# WRITE OUTPUTS (CSV/XLSX/HTML)
# ===============================
def write_outputs(df, html_body):
    """
    Save daily outputs into BASE_DIR:
      - policy_events_YYYY-MM-DD.csv
      - policy_events_YYYY-MM-DD.xlsx
      - policy_events_YYYY-MM-DD.html
    """
    today = now_kst().strftime("%Y-%m-%d")
    csv_path  = os.path.join(BASE_DIR, f"policy_events_{today}.csv")
    xlsx_path = os.path.join(BASE_DIR, f"policy_events_{today}.xlsx")
    html_path = os.path.join(BASE_DIR, f"policy_events_{today}.html")

    # CSV / XLSX
    try:
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    except TypeError:
        # pandas older versions may not accept encoding in to_csv on some paths
        df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)

    # HTML
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_body)

    return csv_path, xlsx_path, html_path



def send_mail(html_body):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"관세·무역 뉴스 브리핑 ({now_kst().strftime('%Y-%m-%d')})"
    msg["From"] = SMTP_EMAIL
    msg["To"] = ", ".join(RECIPIENTS)
    msg.attach(MIMEText(html_body,"html","utf-8"))

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_EMAIL, SMTP_PASSWORD)
        s.sendmail(SMTP_EMAIL, RECIPIENTS, msg.as_string())

# ===============================
# MAIN
# ===============================
def main():
    today = now_kst().strftime("%Y-%m-%d")
    today_csv = os.path.join(BASE_DIR, f"policy_events_{today}.csv")

    # 1) 오늘 CSV가 없으면 센서를 실행해서 df 생성
    if not os.path.exists(today_csv):
        df = run_sensor_build_df()
    else:
        df = load_events()

    # 2) 센서/CSV 모두 결과가 없으면 종료 (메일/파일 생성 안 함)
    if df is None or df.empty:
        print("최근 신규/변경 정책 이벤트 없음 (DF empty)")
        return

    # 3) 폼 보정 → HTML → 출력 저장 → 메일 발송
    df = ensure_cols(df)
    html_body = build_html(df)
    write_outputs(df, html_body)
    send_mail(html_body)
    print("✅ 센서+메일러 통합 완료")



if __name__ == "__main__":
    main()
