# -*- coding: utf-8 -*-
"""
v6.3 vNext (Excel keywords supported)

Add:
- KEYWORDS_XLSX에서 '제시어' 컬럼 읽어서 여러 제시어 수집
- 제시어별로 수집 후 합치고 중복 제거(같은 링크/같은 헤드라인)
- 나머지는 v6.2 STABLE과 동일(표 형식/임원메일/실무메일 포함)
"""

import os, re, html, smtplib, traceback
import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import pandas as pd
import feedparser
import urllib.parse

SMTP_SERVER   = os.getenv("SMTP_SERVER")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_EMAIL    = os.getenv("SMTP_EMAIL")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

RECIPIENTS = [x.strip() for x in os.getenv("RECIPIENTS", "").split(",") if x.strip()]
RECIPIENTS_EXEC = [x.strip() for x in os.getenv("RECIPIENTS_EXEC", "").split(",") if x.strip()]

BASE_DIR = os.getenv("BASE_DIR", os.path.join(os.path.dirname(__file__), "out"))
os.makedirs(BASE_DIR, exist_ok=True)

MAX_ENTRIES = int(os.getenv("MAX_ENTRIES", "60"))
WINDOW_START_HOUR = int(os.getenv("WINDOW_START_HOUR", "7"))
WINDOW_END_HOUR   = int(os.getenv("WINDOW_END_HOUR", "7"))

# Excel keywords
KEYWORDS_XLSX = os.getenv("KEYWORDS_XLSX", "").strip()
KEYWORDS_SHEET = os.getenv("KEYWORDS_SHEET", "").strip()  # 비우면 첫 시트

KST = dt.timezone(dt.timedelta(hours=9))
TAG_RE = re.compile(r"<[^>]+>")

def now_kst():
    return dt.datetime.now(tz=KST)

def get_window_kst(now: dt.datetime):
    today = now.date()
    start = dt.datetime.combine(today, dt.time(hour=WINDOW_START_HOUR, minute=0, second=0), tzinfo=KST) - dt.timedelta(days=1)
    end   = dt.datetime.combine(today, dt.time(hour=WINDOW_END_HOUR, minute=0, second=0), tzinfo=KST)
    return start, end

def to_kst_from_entry(entry):
    t = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if not t:
        return None
    utc_dt = dt.datetime(*t[:6], tzinfo=dt.timezone.utc)
    return utc_dt.astimezone(KST)

def clean_text(s: str) -> str:
    if not s:
        return ""
    s = TAG_RE.sub("", s)
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

RISK_RULES = [
    ("section 301", 6), ("section 232", 6), ("ieepa", 6),
    ("tariff act", 6), ("trade expansion act", 6), ("international emergency economic powers act", 6),
    ("export control", 6), ("sanction", 6),
    ("entity list", 5), ("anti-dumping", 5), ("countervailing", 5), ("safeguard", 5),
    ("tariff", 4), ("duty", 4), ("관세", 4), ("관세율", 4), ("추가관세", 4),
    ("hs code", 3), ("hs", 3), ("원산지", 3), ("fta", 3), ("customs", 3), ("통관", 3),
    ("규정", 2), ("시행", 2), ("개정", 2), ("고시", 2),
]

def calc_policy_score(title: str, summary: str) -> int:
    t = f"{title} {summary}".lower()
    score = 1
    for kw, w in RISK_RULES:
        if kw in t:
            score += w
    return min(score, 20)

def score_to_importance(score: int) -> str:
    if score >= 13: return "상"
    if score >= 7:  return "중"
    return "하"

COUNTRY_KEYWORDS = {
    "USA": ["u.s.", "united states", "america", "section 301", "section 232", "ieepa", "tariff act", "trade expansion act"],
    "India": ["india"], "Türkiye": ["turkey", "türkiye"], "Vietnam": ["vietnam"],
    "Netherlands": ["netherlands", "dutch"], "EU": ["european union", "eu commission", "european commission"],
    "China": ["china"], "Mexico": ["mexico"], "Brazil": ["brazil"],
    "Korea": ["korea", "korean", "대한민국", "한국"], "Japan": ["japan", "japanese", "일본"],
}

def detect_country(text: str) -> str:
    t = (text or "").lower()
    for country, keys in COUNTRY_KEYWORDS.items():
        if any(k in t for k in keys):
            return country
    return ""

def pick_best_summary(entry, title: str) -> str:
    t_norm = clean_text(title).lower()
    summary = clean_text(getattr(entry, "summary", "") or "")
    if summary and summary.lower() != t_norm:
        return summary
    content = ""
    c_list = entry.get("content", None)
    if isinstance(c_list, list) and c_list:
        content = clean_text(c_list[0].get("value", "") or "")
    if content and content.lower() != t_norm:
        return content
    return ""

ALLOW = [
    "관세","tariff","관세율","hs","hs code","section 232","section 301","ieepa",
    "tariff act","trade expansion act","international emergency economic powers act",
    "fta","원산지","무역구제","수출통제","export control","sanction","통관","customs",
    "anti-dumping","countervailing","safeguard"
]
BLOCK = ["시위","protest","체포","arrest","충돌","violent","immigration","ice raid","연방정부","주정부"]

def is_valid_top3_row(r) -> bool:
    blob = f"{r.get('헤드라인','')} {r.get('주요내용','')}".lower()
    if any(b in blob for b in BLOCK): return False
    return any(a in blob for a in ALLOW)

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

def load_keywords():
    if not KEYWORDS_XLSX:
        return [os.getenv("NEWS_QUERY", "관세")]
    if not os.path.exists(KEYWORDS_XLSX):
        print(f"[WARN] KEYWORDS_XLSX not found: {KEYWORDS_XLSX} -> fallback NEWS_QUERY")
        return [os.getenv("NEWS_QUERY", "관세")]

    dfk = pd.read_excel(KEYWORDS_XLSX, sheet_name=KEYWORDS_SHEET or 0)
    # '제시어' 컬럼 우선, 없으면 첫번째 컬럼
    col = "제시어" if "제시어" in dfk.columns else dfk.columns[0]
    keys = [str(x).strip() for x in dfk[col].dropna().tolist()]
    keys = [k for k in keys if k]
    return keys or [os.getenv("NEWS_QUERY", "관세")]

def run_sensor_for_keyword(keyword: str) -> pd.DataFrame:
    rss = "https://news.google.com/rss/search?" + urllib.parse.urlencode({
        "q": keyword,
        "hl": "ko",
        "gl": "KR",
        "ceid": "KR:ko"
    })

    feed = feedparser.parse(rss)
    now = now_kst()
    win_start, win_end = get_window_kst(now)

    rows = []
    for e in feed.entries[:MAX_ENTRIES]:
        pub_kst = to_kst_from_entry(e)
        if pub_kst is None: 
            continue
        if not (win_start <= pub_kst < win_end):
            continue

        title = clean_text(getattr(e, "title", "").strip())
        link = getattr(e, "link", "").strip()
        summary = pick_best_summary(e, title)

        country = detect_country(f"{title} {summary}")
        score = calc_policy_score(title, summary)
        importance = score_to_importance(score)

        trig = [kw for kw, _w in RISK_RULES if kw in f"{title} {summary}".lower()]
        note = []
        if trig: note.append("trigger: " + ", ".join(trig[:6]))
        note.append(f"window: {win_start.strftime('%Y-%m-%d %H:%M')}~{win_end.strftime('%Y-%m-%d %H:%M')} KST")

        rows.append({
            "제시어": keyword,
            "헤드라인": title,
            "주요내용": summary[:500],
            "대상 국가": country,
            "관련 기관": "",
            "중요도": importance,
            "발표일": pub_kst.strftime("%Y-%m-%d %H:%M (KST)"),
            "출처(URL)": link,
            "점수": score,
            "비고": " | ".join(note),
        })

    return pd.DataFrame(rows)

def dedup_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    # link 우선 dedup, 없으면 headline 기준
    df["__link__"] = df["출처(URL)"].fillna("").astype(str)
    df["__head__"] = df["헤드라인"].fillna("").astype(str)
    df = df.sort_values("점수", ascending=False)
    df = df.drop_duplicates(subset=["__link__"], keep="first")
    df = df.drop_duplicates(subset=["__head__"], keep="first")
    df = df.drop(columns=["__link__","__head__"])
    return df

def build_exec_insight(top3: pd.DataFrame) -> str:
    if top3.empty:
        return "<div class='small'>TOP3 해당 없음</div>"
    bullets = []
    for _, r in top3.iterrows():
        c = r.get("대상 국가","")
        s = int(r.get("점수", 1) or 1)
        imp = r.get("중요도","")
        bullets.append(
            f"<li><b>[{c or '-'} | {imp} | 점수 {s}]</b> 관세/통상 정책 변화 신호 → "
            f"<u>대상 HS·적용시점·대상거래</u> 우선 확인</li>"
        )
    action = """
    <ol>
      <li><b>대상국/기관</b> 및 <b>정책 성격</b>(관세율/추가관세/무역구제/수출통제/제재) 구분</li>
      <li><b>대상품목(HS)</b>·<b>생산/판매 법인</b> 매핑(주요 생산국 우선)</li>
      <li><b>1차 영향 산정</b>: 원가·마진·리드타임·FTA/추징 리스크</li>
      <li>필요 시 <b>HQ 대응 트랙</b> 착수</li>
    </ol>
    """
    return f"""
    <div class="small"><b>Executive Insight</b></div>
    <ul>{''.join(bullets)}</ul>
    <div class="small"><b>Action</b></div>
    <div class="small">{action}</div>
    """

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
          <a href="{html.escape(str(r.get('출처(URL)','#')))}" target="_blank">{html.escape(str(r.get('헤드라인','')))}</a><br/>
          <div class="small">{html.escape(str(r.get('주요내용',''))[:260])}</div>
        </li>
        """

    rows = ""
    for _, r in df.sort_values("점수", ascending=False).iterrows():
        headline = html.escape(str(r.get("헤드라인","")))
        summary = html.escape(str(r.get("주요내용","")))
        link = html.escape(str(r.get("출처(URL)","#")))
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
    <html><head>{STYLE}</head>
    <body><div class="page">
      <h2>관세·무역 뉴스 브리핑 ({now.strftime('%Y-%m-%d')})</h2>
      <div class="small">수집 범위: {win_start.strftime('%Y-%m-%d %H:%M')} ~ {win_end.strftime('%Y-%m-%d %H:%M')} (KST)</div>

      <div class="box"><h3>① 오늘의 핵심 정책 이벤트 TOP3</h3>
        <ul>{top3_html if top3_html else "<li class='small'>TOP3 해당 없음</li>"}</ul>
      </div>

      <div class="box">{exec_block}</div>

      <div class="box"><h3>② 정책 이벤트 표 (링크 포함 / 출처 칸 제거)</h3>
        <table>
          <tr>
            <th>헤드라인 / 주요내용</th><th>발표일</th><th>대상 국가</th><th>관련 기관</th><th>중요도</th><th>비고</th>
          </tr>
          {rows}
        </table>
      </div>
    </div></body></html>
    """

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
          <a href="{html.escape(str(r.get('출처(URL)','#')))}" target="_blank">{html.escape(str(r.get('헤드라인','')))}</a><br/>
          <div class="small">{html.escape(str(r.get('주요내용',''))[:220])}</div>
        </li>
        """

    exec_block = build_exec_insight(top3)
    return f"""
    <html><head>{STYLE}</head><body><div class="page">
      <h2>[Executive] 관세·통상 핵심 TOP3 ({now.strftime('%Y-%m-%d')})</h2>
      <div class="small">수집 범위: {win_start.strftime('%Y-%m-%d %H:%M')} ~ {win_end.strftime('%Y-%m-%d %H:%M')} (KST)</div>
      <div class="box"><ul>{items if items else "<li class='small'>TOP3 해당 없음</li>"}</ul></div>
      <div class="box">{exec_block}</div>
    </div></body></html>
    """

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

def main():
    print("=== CONFIG (v6.3) ===")
    print("BASE_DIR =", BASE_DIR)
    print("KEYWORDS_XLSX =", KEYWORDS_XLSX)
    print("RECIPIENTS =", RECIPIENTS)
    print("RECIPIENTS_EXEC =", RECIPIENTS_EXEC)

    keywords = load_keywords()
    print("KEYWORDS =", keywords)

    dfs = []
    for k in keywords:
        d = run_sensor_for_keyword(k)
        if d is not None and not d.empty:
            dfs.append(d)

    if not dfs:
        print("오늘 수집된 이벤트/뉴스 없음 (window 기준)")
        return

    df = pd.concat(dfs, ignore_index=True)
    df = dedup_df(df)

    html_body = build_html_practitioner(df)
    write_outputs(df, html_body)
    send_mail_to(RECIPIENTS, f"관세·무역 뉴스 브리핑 ({now_kst().strftime('%Y-%m-%d')})", html_body)

    exec_html = build_html_exec(df)
    send_mail_to(RECIPIENTS_EXEC, f"[Executive] 관세·통상 핵심 TOP3 ({now_kst().strftime('%Y-%m-%d')})", exec_html)

    print("✅ v6.3 vNext 완료")

if __name__ == "__main__":
    main()
