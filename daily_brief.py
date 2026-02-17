# -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief
vCURRENT STABLE (E2E): Sensor + Outputs + Mail (Practitioner + Executive)

- Runs on GitHub Actions
- Collection window: 07:00~Next day 07:00 (KST)
- Send time: 08:00 (KST) -> set cron to 23:00 UTC
- Inputs:
  - custom_queries.TXT : list of queries (one per line)
  - sites.xlsx         : official/government/authorized site list (sheet 'sites' preferred)
- Output:
  - out/policy_events_YYYY-MM-DD.(csv/xlsx/html)
- Email:
  - Practitioner: full table (NO '비고' column)
  - Executive: TOP3 + relevance + action (also shown in practitioner mail as "Executive Insight" box)
"""

import os
import re
import html
import smtplib
import urllib.parse
import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import pandas as pd
import feedparser
from dateutil import parser as dtparser
from dateutil.tz import tzoffset

# -------------------------------
# ENV
# -------------------------------
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_EMAIL = os.getenv("SMTP_EMAIL")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

RECIPIENTS = [x.strip() for x in os.getenv("RECIPIENTS", "").split(",") if x.strip()]
RECIPIENTS_EXEC = [x.strip() for x in os.getenv("RECIPIENTS_EXEC", "").split(",") if x.strip()]

BASE_DIR = os.getenv("BASE_DIR", os.path.join(os.path.dirname(__file__), "out"))
os.makedirs(BASE_DIR, exist_ok=True)

QUERIES_FILE = os.getenv("QUERIES_FILE", os.path.join(os.path.dirname(__file__), "custom_queries.TXT"))
SITES_FILE = os.getenv("SITES_FILE", os.path.join(os.path.dirname(__file__), "sites.xlsx"))

NEWS_MAX_PER_QUERY = int(os.getenv("NEWS_MAX_PER_QUERY", "30"))  # per (query, lang)
TOTAL_MAX_ITEMS = int(os.getenv("TOTAL_MAX_ITEMS", "250"))       # cap after merge/dedup

NEWS_LANGS = os.getenv("NEWS_LANGS", "ko,en,fr").split(",")      # multilingual search set
NEWS_REGION = os.getenv("NEWS_REGION", "KR")                    # Google RSS gl
NEWS_QUERY_EXPAND = os.getenv("NEWS_QUERY_EXPAND", "1") == "1"   # expand keywords into translations

# -------------------------------
# TIME / WINDOW
# -------------------------------
KST = tzoffset("KST", 9 * 3600)

def now_kst() -> dt.datetime:
    return dt.datetime.now(tz=KST)

def collection_window_kst(ref: dt.datetime):
    """
    07:00~Next day 07:00 (KST)
    - If run at 08:00 KST, window end is today 07:00 KST, start is yesterday 07:00 KST.
    """
    end = ref.replace(hour=7, minute=0, second=0, microsecond=0)
    if ref.hour < 7:
        # if run before 07:00, end should be yesterday 07:00
        end = end - dt.timedelta(days=1)
    start = end - dt.timedelta(days=1)
    return start, end

def parse_pub_kst(published: str):
    if not published:
        return None
    try:
        d = dtparser.parse(published)
        if d.tzinfo is None:
            # assume UTC if tz missing (rare)
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(KST)
    except Exception:
        return None

# -------------------------------
# TIGHT LOADER
# -------------------------------
def load_custom_queries(path: str) -> list:
    if not os.path.exists(path):
        raise FileNotFoundError(f"custom_queries file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        raw = [line.strip() for line in f.read().splitlines()]

    # remove empty, normalize, dedup preserve order
    out = []
    seen = set()
    for q in raw:
        q = re.sub(r"\s+", " ", q).strip()
        if not q:
            continue
        key = q.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(q)
    return out

def _pick_sites_sheet(xls: pd.ExcelFile):
    # strict preference order
    preferred = ["sites", "SiteList", "SITELIST", "site_list", "sheet1", "Sheet1"]
    for name in preferred:
        for sh in xls.sheet_names:
            if sh == name:
                return sh
    # fallback: first sheet that has name/url
    for sh in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sh)
        cols = [c.strip().lower() for c in df.columns.astype(str).tolist()]
        if "name" in cols and "url" in cols:
            return sh
    return xls.sheet_names[0]

def load_sites_xlsx(path: str):
    """
    Returns:
      - sites_df with normalized columns: name, url
      - domain_to_name map
      - allowed_domains set
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"sites.xlsx not found: {path}")

    xls = pd.ExcelFile(path)
    sh = _pick_sites_sheet(xls)
    df = pd.read_excel(path, sheet_name=sh)

    # normalize columns
    cols_map = {c: str(c).strip().lower() for c in df.columns}
    df = df.rename(columns=cols_map)

    if "name" not in df.columns or "url" not in df.columns:
        raise ValueError(f"sites.xlsx sheet '{sh}' must have columns: name, url")

    df = df[["name", "url"]].copy()
    df["name"] = df["name"].astype(str).str.strip()
    df["url"] = df["url"].astype(str).str.strip()

    df = df[(df["name"] != "") & (df["url"] != "") & (~df["name"].str.lower().eq("nan")) & (~df["url"].str.lower().eq("nan"))]

    def domain(u: str):
        try:
            u2 = u if re.match(r"^https?://", u, re.I) else ("https://" + u)
            host = urllib.parse.urlparse(u2).netloc.lower()
            host = host.split("@")[-1]
            host = host.split(":")[0]
            if host.startswith("www."):
                host = host[4:]
            return host
        except Exception:
            return ""

    df["domain"] = df["url"].apply(domain)
    df = df[df["domain"] != ""]

    domain_to_name = {}
    for _, r in df.iterrows():
        domain_to_name[r["domain"]] = r["name"]

    allowed_domains = set(domain_to_name.keys())
    return df, domain_to_name, allowed_domains

# -------------------------------
# KEYWORD EXPANSION (multilingual)
# - keep deterministic for STABLE (no external translate)
# -------------------------------
KW_TRANSLATIONS = {
    "관세": {"en": ["tariff", "customs duty"], "fr": ["droit de douane", "tarif douanier"]},
    "세관": {"en": ["customs"], "fr": ["douanes"]},
    "전략물자": {"en": ["strategic goods", "export control"], "fr": ["contrôle des exportations", "biens stratégiques"]},
    "보세공장": {"en": ["bonded factory", "bonded zone"], "fr": ["entrepôt sous douane", "zone franche"]},
    "외국환거래": {"en": ["foreign exchange", "FX regulation"], "fr": ["réglementation des changes", "contrôle des changes"]},
    "원산지": {"en": ["rules of origin", "origin"], "fr": ["règles d'origine", "origine"]},
    "fta": {"en": ["FTA", "free trade agreement"], "fr": ["accord de libre-échange"]},
    "a eo": {"en": ["AEO", "authorized economic operator"], "fr": ["OEA", "opérateur économique agréé"]},
    "aeo": {"en": ["AEO", "authorized economic operator"], "fr": ["OEA", "opérateur économique agréé"]},
    "wco": {"en": ["WCO", "World Customs Organization"], "fr": ["OMD", "Organisation mondiale des douanes"]},
}

def expand_query(q: str, lang: str) -> list:
    q_norm = q.strip()
    if not NEWS_QUERY_EXPAND:
        return [q_norm]
    key = q_norm.lower()
    # try direct
    for k, v in KW_TRANSLATIONS.items():
        if k.lower() == key:
            extra = v.get(lang, [])
            base = [q_norm]
            for t in extra:
                if t and t.lower() not in {x.lower() for x in base}:
                    base.append(t)
            return base
    return [q_norm]

# -------------------------------
# URL / DOMAIN HELPERS
# -------------------------------
def unwrap_google_news_url(u: str) -> str:
    """
    Google News RSS sometimes provides redirect links.
    Attempt to extract real URL if it's of form ...?url=...
    """
    if not u:
        return u
    try:
        parsed = urllib.parse.urlparse(u)
        qs = urllib.parse.parse_qs(parsed.query)
        if "url" in qs and qs["url"]:
            return qs["url"][0]
        return u
    except Exception:
        return u

def get_domain(u: str) -> str:
    try:
        u2 = u if re.match(r"^https?://", u, re.I) else ("https://" + u)
        host = urllib.parse.urlparse(u2).netloc.lower()
        host = host.split("@")[-1]
        host = host.split(":")[0]
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""

# -------------------------------
# POLICY SCORE / IMPORTANCE
# -------------------------------
RISK_RULES = [
    ("section 301", 6),
    ("section 232", 6),
    ("international emergency economic powers act", 6),
    ("ieepa", 6),
    ("export control", 6),
    ("sanction", 6),
    ("entity list", 5),
    ("anti-dumping", 5),
    ("antidumping", 5),
    ("countervailing", 5),
    ("safeguard", 5),

    ("tariff act", 4),
    ("trade expansion act", 4),
    ("tariff", 4),
    ("duty", 4),
    ("customs duty", 4),
    ("관세", 4),
    ("관세율", 4),
    ("추가관세", 4),

    ("hs code", 3),
    ("hs", 3),
    ("원산지", 3),
    ("rules of origin", 3),
    ("fta", 3),
    ("customs", 3),
    ("통관", 3),

    ("규정", 2),
    ("시행", 2),
    ("개정", 2),
    ("고시", 2),
]

ALLOW_MUST_SHOW = [
    "관세", "관세율", "tariff", "customs duty",
    "tariff act", "trade expansion act",
    "international emergency economic powers act", "ieepa",
    "section 232", "section 301",
]

BLOCK = ["시위", "protest", "체포", "arrest", "충돌", "violent", "immigration", "ice raid", "연방정부", "주정부"]

def calc_policy_score(title: str, summary: str) -> int:
    t = f"{title} {summary}".lower()
    score = 1
    for kw, w in RISK_RULES:
        if kw in t:
            score += w
    return min(score, 20)

def importance_label(score: int) -> str:
    # strict: High for actual tariff/act/sections
    if score >= 12:
        return "상"
    if score >= 7:
        return "중"
    return "하"

def is_trade_relevant(title: str, summary: str) -> bool:
    blob = f"{title} {summary}".lower()
    if any(b in blob for b in BLOCK):
        return False
    # must be trade-related
    return any(a.lower() in blob for a in ALLOW_MUST_SHOW) or any(k in blob for k, _ in RISK_RULES)

# -------------------------------
# COUNTRY TAG (simple heuristic)
# -------------------------------
COUNTRY_KEYWORDS = {
    "USA": ["u.s.", "united states", "america", "section 301", "section 232", "u.s "],
    "India": ["india"],
    "Türkiye": ["turkey", "türkiye"],
    "Vietnam": ["vietnam"],
    "Netherlands": ["netherlands", "dutch"],
    "EU": ["european union", "eu commission", "european commission"],
    "China": ["china"],
    "Mexico": ["mexico"],
    "Brazil": ["brazil"],
    "Korea": ["korea", "korean", "republic of korea"],
    "Japan": ["japan"],
}

def detect_country(text: str) -> str:
    t = (text or "").lower()
    for country, keys in COUNTRY_KEYWORDS.items():
        if any(k in t for k in keys):
            return country
    return ""

# -------------------------------
# SENSOR (Google News RSS)
# -------------------------------
def google_news_rss(q: str, lang: str) -> str:
    return "https://news.google.com/rss/search?" + urllib.parse.urlencode({
        "q": q,
        "hl": lang,
        "gl": NEWS_REGION,
        "ceid": f"{NEWS_REGION}:{lang}",
    })

def run_sensor(queries: list, allowed_domains: set, domain_to_name: dict, window_start: dt.datetime, window_end: dt.datetime) -> pd.DataFrame:
    rows = []
    for base_q in queries:
        for lang in NEWS_LANGS:
            for q in expand_query(base_q, lang):
                rss = google_news_rss(q, lang)
                feed = feedparser.parse(rss)

                for e in feed.entries[:NEWS_MAX_PER_QUERY]:
                    title = getattr(e, "title", "").strip()
                    link = unwrap_google_news_url(getattr(e, "link", "").strip())
                    published = getattr(e, "published", "") or getattr(e, "updated", "")
                    summary = getattr(e, "summary", "") or getattr(e, "description", "")
                    summary = re.sub(r"<[^>]+>", "", summary).strip()

                    pub_kst = parse_pub_kst(published)

                    # window filter (strict)
                    if pub_kst is not None:
                        if not (window_start <= pub_kst < window_end):
                            continue

                    # relevance filter (strict)
                    if not is_trade_relevant(title, summary):
                        continue

                    dom = get_domain(link)
                    agency = domain_to_name.get(dom, "")

                    # site filter: keep if matches official list, OR if must-show keywords exist
                    blob = f"{title} {summary}".lower()
                    must_show = any(x.lower() in blob for x in ALLOW_MUST_SHOW)
                    if allowed_domains and (dom not in allowed_domains) and (not must_show):
                        continue

                    country = detect_country(f"{title} {summary}")
                    score = calc_policy_score(title, summary)
                    imp = importance_label(score)

                    rows.append({
                        "제시어": base_q,
                        "언어": lang,
                        "헤드라인": title,
                        "주요내용": summary[:700],
                        "발표일": pub_kst.strftime("%Y-%m-%d %H:%M") if pub_kst else "",
                        "대상 국가": country,
                        "관련 기관": agency,
                        "중요도": imp,
                        "점수": score,
                        "URL": link,
                    })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # cap
    if len(df) > TOTAL_MAX_ITEMS:
        df = df.sort_values(["점수"], ascending=False).head(TOTAL_MAX_ITEMS)

    return df

# -------------------------------
# DEDUP
# -------------------------------
def norm_title(t: str) -> str:
    t = (t or "").lower()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"[^\w\s가-힣]", "", t)
    return t.strip()

def dedup_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    # prefer higher score, then earlier published
    df["__title_norm"] = df["헤드라인"].apply(norm_title)
    df["__domain"] = df["URL"].apply(get_domain)
    df["__key"] = df["__domain"].fillna("") + "|" + df["__title_norm"].fillna("")

    df = df.sort_values(["점수", "발표일"], ascending=[False, False])

    df = df.drop_duplicates(subset=["__key"], keep="first").drop(columns=["__title_norm", "__domain", "__key"])
    return df

# -------------------------------
# EXEC INSIGHT (rule-based STABLE)
# - This must also appear in practitioner email (per request)
# -------------------------------
def build_exec_insight_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    cand = df.sort_values("점수", ascending=False).head(3).copy()

    def relevance_reason(r):
        blob = f"{r.get('헤드라인','')} {r.get('주요내용','')}".lower()
        hits = [k for k in ["section 301", "section 232", "ieepa", "tariff act", "trade expansion act", "관세율", "추가관세", "hs", "원산지", "export control", "sanction"] if k in blob]
        if hits:
            return "핵심 키워드: " + ", ".join(hits[:4])
        return "관세/통상 정책성 이슈 가능"

    def action(r):
        c = r.get("대상 국가", "")
        return f"1) {c or '대상국'}·HS/품목 확인  2) 생산/판매법인 원가·마진 영향 1차 산정  3) 필요 시 대응 시나리오/커뮤니케이션 착수"

    cand["당사 연관성(요약)"] = cand.apply(relevance_reason, axis=1)
    cand["권고 Action"] = cand.apply(action, axis=1)
    return cand

# -------------------------------
# HTML BUILDERS
# - Practitioner: keep table, remove 비고 column
# - Table column rule:
#   - "출처" column removed
#   - "헤드라인+주요내용" in ONE cell, with headline as hyperlink
# -------------------------------
STYLE = """
<style>
body{font-family:Malgun Gothic,Arial; background:#f6f6f6;}
.page{max-width:1120px;margin:auto;background:white;padding:14px;}
h2{margin:0 0 6px 0;}
.box{border:1px solid #ddd;border-radius:10px;padding:12px;margin:12px 0;}
table{border-collapse:collapse;width:100%;}
th,td{border:1px solid #ccc;padding:8px;font-size:12px;vertical-align:top;}
th{background:#f0f0f0;}
.small{font-size:11px;color:#555;}
.badge{display:inline-block;padding:2px 8px;border-radius:10px;border:1px solid #ccc;font-size:11px;margin-right:6px;}
</style>
"""

def html_escape(x) -> str:
    return html.escape("" if x is None else str(x))

def build_table_rows(df: pd.DataFrame, include_remark: bool) -> str:
    rows = ""
    for _, r in df.iterrows():
        headline = html_escape(r.get("헤드라인", ""))
        url = html_escape(r.get("URL", ""))
        summary = html_escape(r.get("주요내용", ""))

        headline_cell = f'<a href="{url}" target="_blank">{headline}</a><br/><div class="small">{summary}</div>'

        remark = html_escape(r.get("비고", "")) if include_remark else ""

        rows += "<tr>"
        rows += f"<td>{html_escape(r.get('제시어',''))}</td>"
        rows += f"<td>{headline_cell}</td>"
        rows += f"<td>{html_escape(r.get('발표일',''))}</td>"
        rows += f"<td>{html_escape(r.get('대상 국가',''))}</td>"
        rows += f"<td>{html_escape(r.get('관련 기관',''))}</td>"
        rows += f"<td>{html_escape(r.get('중요도',''))}</td>"
        if include_remark:
            rows += f"<td>{remark}</td>"
        rows += "</tr>"
    return rows

def build_html_practitioner(df: pd.DataFrame) -> str:
    date = now_kst().strftime("%Y-%m-%d")
    exec3 = build_exec_insight_rows(df)

    # Executive insight box must also appear in practitioner mail
    exec_items = ""
    for _, r in exec3.iterrows():
        exec_items += f"""
        <li>
          <b>[{html_escape(r.get('대상 국가',''))} | {html_escape(r.get('중요도',''))} | 점수 {html_escape(r.get('점수',''))}]</b><br/>
          <a href="{html_escape(r.get('URL',''))}" target="_blank">{html_escape(r.get('헤드라인',''))}</a><br/>
          <div class="small">{html_escape(r.get('당사 연관성(요약)',''))}</div>
          <div class="small"><b>Action:</b> {html_escape(r.get('권고 Action',''))}</div>
        </li>
        """

    # Table (no remark)
    rows = build_table_rows(df, include_remark=False)

    return f"""
    <html><head>{STYLE}</head>
    <body>
    <div class="page">
      <h2>관세·통상 정책 센서 (실무) ({date})</h2>

      <div class="box">
        <h3 style="margin:0 0 8px 0;">Executive Insight TOP3 (동일 내용 실무 공유)</h3>
        <ul style="margin:0; padding-left:18px;">
          {exec_items or "<li>TOP3 후보 없음</li>"}
        </ul>
      </div>

      <div class="box">
        <h3 style="margin:0 0 8px 0;">② 정책 이벤트 표</h3>
        <table>
          <tr>
            <th>제시어</th>
            <th>헤드라인 / 주요내용</th>
            <th>발표일</th>
            <th>대상 국가</th>
            <th>관련 기관</th>
            <th>중요도</th>
          </tr>
          {rows}
        </table>
      </div>
    </div>
    </body></html>
    """

def build_html_exec(df: pd.DataFrame) -> str:
    date = now_kst().strftime("%Y-%m-%d")
    exec3 = build_exec_insight_rows(df)

    items = ""
    for _, r in exec3.iterrows():
        items += f"""
        <li>
          <b>[{html_escape(r.get('대상 국가',''))} | {html_escape(r.get('중요도',''))} | 점수 {html_escape(r.get('점수',''))}]</b><br/>
          <a href="{html_escape(r.get('URL',''))}" target="_blank">{html_escape(r.get('헤드라인',''))}</a><br/>
          <div class="small">{html_escape(r.get('당사 연관성(요약)',''))}</div>
          <div class="small"><b>Action:</b> {html_escape(r.get('권고 Action',''))}</div>
        </li>
        """

    return f"""
    <html><head>{STYLE}</head>
    <body>
    <div class="page">
      <h2>[Executive] 관세·통상 핵심 TOP3 ({date})</h2>
      <div class="box">
        <ul style="margin:0; padding-left:18px;">
          {items or "<li>TOP3 후보 없음</li>"}
        </ul>
      </div>
    </div>
    </body></html>
    """

# -------------------------------
# OUTPUTS
# -------------------------------
def write_outputs(df: pd.DataFrame, html_body: str):
    today = now_kst().strftime("%Y-%m-%d")
    csv_path = os.path.join(BASE_DIR, f"policy_events_{today}.csv")
    xlsx_path = os.path.join(BASE_DIR, f"policy_events_{today}.xlsx")
    html_path = os.path.join(BASE_DIR, f"policy_events_{today}.html")

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_body)

    return csv_path, xlsx_path, html_path

# -------------------------------
# MAIL
# -------------------------------
def send_mail_to(recipients, subject, html_body):
    if not recipients:
        print(f"[WARN] No recipients for: {subject}")
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

# -------------------------------
# MAIN
# -------------------------------
def main():
    # 0) load inputs (tight)
    queries = load_custom_queries(QUERIES_FILE)
    _, domain_to_name, allowed_domains = load_sites_xlsx(SITES_FILE)

    # 1) window
    ref = now_kst()
    w_start, w_end = collection_window_kst(ref)
    print(f"[INFO] Window(KST): {w_start} ~ {w_end}")

    # 2) sensor
    df = run_sensor(queries, allowed_domains, domain_to_name, w_start, w_end)
    if df is None or df.empty:
        print("[INFO] No items collected.")
        return

    # 3) dedup + sort
    df = dedup_df(df)
    df = df.sort_values(["점수", "발표일"], ascending=[False, False]).reset_index(drop=True)

    # 4) build + outputs + mail
    html_prac = build_html_practitioner(df)
    write_outputs(df, html_prac)

    today = now_kst().strftime("%Y-%m-%d")
    send_mail_to(RECIPIENTS, f"관세·통상 정책 센서 (실무) ({today})", html_prac)

    html_exec = build_html_exec(df)
    send_mail_to(RECIPIENTS_EXEC, f"[Executive] 관세·통상 핵심 TOP3 ({today})", html_exec)

    print("[OK] STABLE completed.")
    print("BASE_DIR =", BASE_DIR)
    print("OUT_FILES =", os.listdir(BASE_DIR))

if __name__ == "__main__":
    main()
