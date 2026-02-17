# -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief
v6.2 STABLE FINAL (E2E: Sensor + Outputs + Mail)

Fixes:
1) Exec mail not sent -> log recipients + SMTP exception logging
2) Table headline == summary -> normalize; if same then try content; else blank
3) Table format -> remove '출처' column, headline hyperlinked, headline+summary in one cell

Time window:
- Collect: KST 기준 전일 07:00 ~ 금일 07:00
- Send: 08:00에 스케줄러로 실행 권장 (코드는 언제 실행돼도 window 필터링)
"""

import os, re, html, smtplib, traceback
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

NEWS_QUERY = os.getenv("NEWS_QUERY", "관세")
MAX_ENTRIES = int(os.getenv("MAX_ENTRIES", "60"))

# 수집 시간창(기본: 07~07)
WINDOW_START_HOUR = int(os.getenv("WINDOW_START_HOUR", "7"))  # KST 07:00
WINDOW_END_HOUR   = int(os.getenv("WINDOW_END_HOUR", "7"))    # KST 07:00

# ===============================
# TIME
# ===============================
KST = dt.timezone(dt.timedelta(hours=9))

def now_kst():
    return dt.datetime.now(tz=KST)

def get_window_kst(now: dt.datetime):
    """
    KST 기준 전일 WINDOW_START_HOUR:00 ~ 금일 WINDOW_END_HOUR:00
    예) 2026-02-17 08:xx 실행 시 -> 2026-02-16 07:00 ~ 2026-02-17 07:00
    """
    today = now.date()
    start = dt.datetime.combine(today, dt.time(hour=WINDOW_START_HOUR, minute=0, second=0), tzinfo=KST)
    end   = dt.datetime.combine(today, dt.time(hour=WINDOW_END_HOUR, minute=0, second=0), tzinfo=KST)
    # end <= start 인 경우(예: 07~07)는 "전일 start ~ 금일 end"가 되어야 하므로 end는 그대로, start는 전일로
    start = start - dt.timedelta(days=1)
    return start, end

def to_kst_from_entry(entry) -> dt.datetime | None:
    """
    feedparser가 주는 published_parsed(UTC 기반 struct_time)를 KST aware datetime으로 변환
    """
    t = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if not t:
        return None
    # struct_time -> UTC naive -> UTC aware -> KST
    utc_dt = dt.datetime(*t[:6], tzinfo=dt.timezone.utc)
    return utc_dt.astimezone(KST)

# ===============================
# POLICY SCORE (고도화 유지)
# ===============================
RISK_RULES = [
    ("section 301", 6),
    ("section 232", 6),
    ("ieepa", 6),
    ("tariff act", 6),           # Tariff Act
    ("trade expansion act", 6),  # Trade Expansion Act
    ("international emergency economic powers act", 6),  # IEEPA full
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

def score_to_importance(score: int) -> str:
    # 상: 관세율/HS/법령급 키워드가 잡히는 고점 / 중: 통상 이슈 / 하: 참고
    if score >= 13:
        return "상"
    if score >= 7:
        return "중"
    return "하"

# ===============================
# COUNTRY TAG
# ===============================
COUNTRY_KEYWORDS = {
    "USA": ["u.s.", "united states", "america", "section 301", "section 232", "ieepa", "tariff act", "trade expansion act"],
    "India": ["india"],
    "Türkiye": ["turkey", "türkiye"],
    "Vietnam": ["vietnam"],
    "Netherlands": ["netherlands", "dutch"],
    "EU": ["european union", "eu commission", "european commission"],
    "China": ["china"],
    "Mexico": ["mexico"],
    "Brazil": ["brazil"],
    "Korea": ["korea", "korean", "대한민국", "한국"],
    "Japan": ["japan", "japanese", "일본"],
}

def detect_country(text: str) -> str:
    t = (text or "").lower()
    for country, keys in COUNTRY_KEYWORDS.items():
        if any(k in t for k in keys):
            return country
    return ""

# ===============================
# TEXT CLEAN
# ===============================
TAG_RE = re.compile(r"<[^>]+>")

def clean_text(s: str) -> str:
    if not s:
        return ""
    s = TAG_RE.sub("", s)
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def pick_best_summary(entry, title: str) -> str:
    """
    1) entry.summary 정리
    2) title과 동일/유사하면 entry.content에서 시도
    3) 그래도 같으면 빈칸(불명확한 정보 출력 금지)
    """
    t_norm = clean_text(title).lower()

    summary = clean_text(getattr(entry, "summary", "") or "")
    s_norm = summary.lower()

    if summary and s_norm != t_norm:
        return summary

    # content 후보
    content = ""
    c_list = entry.get("content", None)
    if isinstance(c_list, list) and len(c_list) > 0:
        content = clean_text(c_list[0].get("value", "") or "")
    if content and clean_text(content).lower() != t_norm:
        return content

    return ""  # 지침: 정보 없으면 빈칸

# ===============================
# TOP3 FILTER
# ===============================
ALLOW = [
    "관세","tariff","관세율","hs","hs code","section 232","section 301","ieepa",
    "tariff act","trade expansion act","international emergency economic powers act",
    "fta","원산지","무역구제","수출통제","export control","sanction","통관","customs",
    "anti-dumping","countervailing","safeguard"
]
BLOCK = [
    "시위","protest","체포","arrest","충돌","violent",
    "immigration","ice raid","연방정부","주정부"
]

def is_valid_top3_row(r) -> bool:
    blob = f"{r.get('헤드라인','')} {r.get('주요내용','')}".lower()
    if any(b in blob for b in BLOCK):
        return False
    return any(a in blob for a in ALLOW)

# ===============================
# SENSOR
# ===============================
def run_sensor_build_df() -> pd.DataFrame:
    """
    Google News RSS 기반 수집 + 시간창 필터링(KST) + 정리 DF 생성
    """
    rss = "https://news.google.com/rss/search?" + urllib.parse.urlencode({
        "q": NEWS_QUERY,
        "hl": "ko",
        "gl": "KR",
        "ceid": "KR:ko"
    })

    feed = feedparser.parse(rss)
    rows = []

    now = now_kst()
    win_start, win_end = get_window_kst(now)

    for e in feed.entries[:MAX_ENTRIES]:
        title = clean_text(getattr(e, "title", "").strip())
        link = getattr(e, "link", "").strip()

        pub_kst = to_kst_from_entry(e)
        # 발표일 필터: 시간정보가 없으면 제외(출처 불명확 방지)
        if pub_kst is None:
            continue
        if not (win_start <= pub_kst < win_end):
            continue

        summary = pick_best_summary(e, title)
        country = detect_country(f"{title} {summary}")
        score = calc_policy_score(title, summary)
        importance = score_to_importance(score)

        # 비고: 시간창/키워드 트리거 정보를 간단히 남김
        note = []
        trig = [kw for kw, _w in RISK_RULES if kw in f"{title} {summary}".lower()]
        if trig:
            note.append("trigger: " + ", ".join(trig[:6]))
        note.append(f"window: {win_start.strftime('%Y-%m-%d %H:%M')}~{win_end.strftime('%Y-%m-%d %H:%M')} KST")

        rows.append({
            "제시어": NEWS_QUERY,
            "헤드라인": title,
            "주요내용": summary[:500],
            "대상 국가": country,
            "관련 기관": "",  # RSS만으로 확정 곤란 -> 빈칸(지침 준수)
            "중요도": importance,
            "발표일": pub_kst.strftime("%Y-%m-%d %H:%M (KST)"),
            "출처(URL)": link,
            "점수": score,
            "비고": " | ".join(note),
        })

    df = pd.DataFrame(rows)
    return df

# ===============================
# SAFE COLUMNS
# ===============================
def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["제시어","헤드라인","주요내용","대상 국가","관련 기관","중요도","발표일","출처(URL)","점수","비고"]:
        if col not in df.columns:
            df[col] = ""
    # 점수/중요도 보정
    if df["점수"].isna().all():
        df["점수"] = 1
    if df["중요도"].isna().all():
        df["중요도"] = "하"
    return df

def get_link(r) -> str:
    v = r.get("출처(URL)", "") if isinstance(r, dict) else ""
    if not v or (isinstance(v, float) and pd.isna(v)):
        return "#"
    return str(v)

# ===============================
# HTML STYLE
# ===============================
STYLE = """
<style>
body{font-family:Malgun Gothic,Arial; background:#f6f6f6;}
.page{max-width:1120px;margin:auto;background:white;padding:14px;}
h2{margin-bottom:4px;}
.box{border:1px solid #ddd;border-radius:10px;padding:12px;margin:12px 0;}
li{margin-bottom:14px;}
table{border-collapse:collapse;width:100%;}
th,td{border:1px solid #ccc;padding:8px;font-size:12px;vertical-align:top;}
th{background:#f0f0f0;}
.small{font-size:11px;color:#555;}
.badge{display:inline-block;padding:2px 7px;border:1px solid #aaa;border-radius:10px;font-size:11px;margin-right:6px;}
</style>
"""

# ===============================
# EXEC INSIGHT / ACTION (실무자 메일에도 동일 표기)
# ===============================
def build_exec_insight(top3: pd.DataFrame) -> str:
    """
    임원용에서 필요한 '당사 관련성'과 'Action'을 더 명확히 표현
    (기사 본문을 크롤링하지 않으므로, 과도한 추정 없이 '확인/산정/착수' 중심)
    """
    if top3.empty:
        return "<div class='small'>TOP3 해당 없음</div>"

    bullets = []
    for _, r in top3.iterrows():
        c = r.get("대상 국가","")
        s = int(r.get("점수", 1) or 1)
        imp = r.get("중요도","")
        bullets.append(
            f"<li><b>[{c or '-'} | {imp} | 점수 {s}]</b> "
            f"관세/통상 정책 변화 신호 → <u>대상 품목(HS)·적용시점·대상거래(수입/수출)</u> 우선 확인</li>"
        )

    action = """
    <ol>
      <li><b>대상국/기관</b> 및 <b>정책 성격</b>(관세율/추가관세/무역구제/수출통제/제재) 구분</li>
      <li><b>대상품목(HS)</b>·<b>생산/판매 법인</b> 매핑(한국/중국/베트남/인도/인니/터키/슬로바키아/폴란드/멕시코/브라질 우선)</li>
      <li><b>1차 영향 산정</b>: 원가·마진·리드타임·특혜관세(FTA) 적용 실패/추징 리스크</li>
      <li>필요 시 <b>HQ 대응 트랙</b>: HS/원산지/가격/전략물자(통제) 워킹 착수</li>
    </ol>
    """

    return f"""
    <div class="small"><b>Executive Insight</b></div>
    <ul>{''.join(bullets)}</ul>
    <div class="small"><b>Action</b></div>
    <div class="small">{action}</div>
    """

# ===============================
# HTML BUILD (실무자용: 기존 표 유지 + Exec Insight 포함)
# ===============================
def build_html_practitioner(df: pd.DataFrame) -> str:
    now = now_kst()
    win_start, win_end = get_window_kst(now)

    cand = df[df.apply(is_valid_top3_row, axis=1)]
    top3 = cand.sort_values("점수", ascending=False).head(3)

    top3_html = ""
    for _, r in top3.iterrows():
        top3_html += f"""
        <li>
          <span class="badge">{html.escape(str(r.get('중요도','')))}</span>
          <b>[{html.escape(str(r.get('대상 국가','') or '-'))} | 점수 {html.escape(str(r.get('점수','')))}]</b><br/>
          <a href="{html.escape(get_link(r))}" target="_blank">{html.escape(str(r.get('헤드라인','')))}</a><br/>
          <div class="small">{html.escape(str(r.get('주요내용',''))[:260])}</div>
        </li>
        """

    # 표(요구사항): 출처 칸 삭제, 헤드라인 링크 + 헤드라인/요약 한 칸
    rows = ""
    for _, r in df.sort_values("점수", ascending=False).iterrows():
        headline = html.escape(str(r.get("헤드라인","")))
        summary = html.escape(str(r.get("주요내용","")))
        link = html.escape(get_link(r))

        combined = f'<a href="{link}" target="_blank"><b>{headline}</b></a>'
        if summary:
            combined += f"<br/><span class='small'>{summary}</span>"

        rows += f"""
        <tr>
          <td>{combined}</td>
          <td>{html.escape(str(r.get("발표일","")))}</td>
          <td>{html.escape(str(r.get("대상 국가","")))}</td>
          <td>{html.escape(str(r.get("관련 기관","")))}</td>
          <td>{html.escape(str(r.get("중요도","")))}</td>
          <td>{html.escape(str(r.get("비고","")))}</td>
        </tr>
        """

    exec_block = build_exec_insight(top3)

    return f"""
    <html>
    <head>{STYLE}</head>
    <body>
      <div class="page">
        <h2>관세·무역 뉴스 브리핑 ({now.strftime('%Y-%m-%d')})</h2>
        <div class="small">수집 범위: {win_start.strftime('%Y-%m-%d %H:%M')} ~ {win_end.strftime('%Y-%m-%d %H:%M')} (KST)</div>

        <div class="box">
          <h3>① 오늘의 핵심 정책 이벤트 TOP3</h3>
          <ul>{top3_html if top3_html else "<li class='small'>TOP3 해당 없음</li>"}</ul>
        </div>

        <div class="box">
          {exec_block}
        </div>

        <div class="box">
          <h3>② 정책 이벤트 표 (링크 포함 / 출처 칸 제거)</h3>
          <table>
            <tr>
              <th>헤드라인 / 주요내용</th>
              <th>발표일</th>
              <th>대상 국가</th>
              <th>관련 기관</th>
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
# HTML BUILD (임원용)
# ===============================
def build_html_exec(df: pd.DataFrame) -> str:
    now = now_kst()
    win_start, win_end = get_window_kst(now)

    cand = df[df.apply(is_valid_top3_row, axis=1)]
    top3 = cand.sort_values("점수", ascending=False).head(3)

    items = ""
    for _, r in top3.iterrows():
        items += f"""
        <li>
          <span class="badge">{html.escape(str(r.get('중요도','')))}</span>
          <b>[{html.escape(str(r.get('대상 국가','') or '-'))} | 점수 {html.escape(str(r.get('점수','')))}]</b><br/>
          <a href="{html.escape(get_link(r))}" target="_blank">{html.escape(str(r.get('헤드라인','')))}</a><br/>
          <div class="small">{html.escape(str(r.get('주요내용',''))[:220])}</div>
        </li>
        """

    exec_block = build_exec_insight(top3)

    return f"""
    <html><head>{STYLE}</head>
    <body>
      <div class="page">
        <h2>[Executive] 관세·통상 핵심 TOP3 ({now.strftime('%Y-%m-%d')})</h2>
        <div class="small">수집 범위: {win_start.strftime('%Y-%m-%d %H:%M')} ~ {win_end.strftime('%Y-%m-%d %H:%M')} (KST)</div>

        <div class="box">
          <ul>{items if items else "<li class='small'>TOP3 해당 없음</li>"}</ul>
        </div>

        <div class="box">
          {exec_block}
        </div>
      </div>
    </body></html>
    """

# ===============================
# WRITE OUTPUTS (CSV/XLSX/HTML)
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
        print(f"[WARN] SKIP SEND (no recipients): subject={subject}")
        return

    if not SMTP_SERVER or not SMTP_EMAIL or not SMTP_PASSWORD:
        print("[ERROR] SMTP env missing (SMTP_SERVER/SMTP_EMAIL/SMTP_PASSWORD)")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_EMAIL
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as s:
            s.starttls()
            s.login(SMTP_EMAIL, SMTP_PASSWORD)
            s.sendmail(SMTP_EMAIL, recipients, msg.as_string())
        print(f"[OK] SENT: {subject} -> {len(recipients)} recipients")
    except Exception:
        print(f"[ERROR] SEND FAIL: {subject}")
        traceback.print_exc()

# ===============================
# MAIN
# ===============================
def main():
    print("=== CONFIG ===")
    print("BASE_DIR =", BASE_DIR)
    print("NEWS_QUERY =", NEWS_QUERY)
    print("RECIPIENTS =", RECIPIENTS)
    print("RECIPIENTS_EXEC =", RECIPIENTS_EXEC)
    print("SMTP_SERVER =", SMTP_SERVER, "PORT =", SMTP_PORT)
    print("=============")

    df = run_sensor_build_df()
    if df is None or df.empty:
        print("오늘 수집된 이벤트/뉴스 없음 (window 기준)")
        return

    df = ensure_cols(df)

    # 실무자용: 표 유지 + Exec Insight 포함
    html_body = build_html_practitioner(df)
    write_outputs(df, html_body)
    send_mail_to(RECIPIENTS, f"관세·무역 뉴스 브리핑 ({now_kst().strftime('%Y-%m-%d')})", html_body)

    # 임원용: TOP3 + Exec Insight/Action
    exec_html = build_html_exec(df)
    send_mail_to(RECIPIENTS_EXEC, f"[Executive] 관세·통상 핵심 TOP3 ({now_kst().strftime('%Y-%m-%d')})", exec_html)

    print("✅ v6.2 STABLE FINAL 완료")

if __name__ == "__main__":
    main()
