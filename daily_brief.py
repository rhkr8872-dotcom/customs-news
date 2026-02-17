# -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief
E2E: Sensor + Outputs + Mail (Practitioner + Executive)

vCurrent STABLE (2026-02-17+)
- custom_queries.TXT 기반 제시어 다중 수집
- 07:00~07:00(KST) 검색 윈도우 필터
- 08:00(KST) 발송은 GitHub Actions cron에서 제어
- 강력 dedup
- 한국어 + 영어 + 프랑스어 쿼리 확장
- 실무자 메일에 Executive Insight TOP3 포함
- 정책 이벤트 표: "헤드라인(링크)+요약"을 1칸으로, '출처' 칸 삭제, 비고/불필요 칼럼 제거
"""

import os, re, html, smtplib, unicodedata
import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl

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

QUERIES_FILE = os.getenv("QUERIES_FILE", os.path.join(os.path.dirname(__file__), "custom_queries.TXT"))
SITES_XLSX   = os.getenv("SITES_XLSX", os.path.join(os.path.dirname(__file__), "sites.xlsx"))

# Google News RSS locale
GOOGLE_HL = os.getenv("GOOGLE_HL", "ko")
GOOGLE_GL = os.getenv("GOOGLE_GL", "KR")
GOOGLE_CEID = os.getenv("GOOGLE_CEID", "KR:ko")

# KST window: 07:00 ~ 07:00
WINDOW_START_HH = int(os.getenv("WINDOW_START_HH", "7"))  # 7
WINDOW_END_HH   = int(os.getenv("WINDOW_END_HH", "7"))    # 7 (next day)

MAX_PER_QUERY = int(os.getenv("MAX_PER_QUERY", "30"))
MAX_TOTAL_ROWS = int(os.getenv("MAX_TOTAL_ROWS", "200"))  # safety

# ===============================
# TIME
# ===============================
def now_kst() -> dt.datetime:
    return dt.datetime.utcnow() + dt.timedelta(hours=9)

def window_kst() -> tuple[dt.datetime, dt.datetime]:
    """
    Returns (start_kst, end_kst) for 07:00~07:00.
    If now is after end time (>=07:00), end is today 07:00, start is yesterday 07:00.
    If now is before 07:00, end is yesterday 07:00, start is day before yesterday 07:00.
    """
    now = now_kst()
    today = now.date()

    end = dt.datetime.combine(today, dt.time(WINDOW_END_HH, 0))
    if now < end:
        end = end - dt.timedelta(days=1)

    start = end - dt.timedelta(days=1)
    return start, end

def fmt_kst(d: dt.datetime) -> str:
    return d.strftime("%Y-%m-%d %H:%M")

# ===============================
# POLICY SCORE (룰 기반)
# ===============================
RISK_RULES = [
    ("section 301", 6),
    ("section 232", 6),
    ("ieepa", 6),
    ("export control", 6),
    ("supply chain security", 5),
    ("sanction", 6),
    ("entity list", 5),
    ("anti-dumping", 5),
    ("antidumping", 5),
    ("countervailing", 5),
    ("c-v", 4),
    ("safeguard", 5),

    ("tariff", 4),
    ("duty", 4),
    ("customs duty", 4),
    ("관세", 4),
    ("관세율", 4),
    ("추가관세", 4),
    ("보복관세", 4),

    ("hs code", 3),
    ("hs", 2),
    ("origin", 3),
    ("원산지", 3),
    ("fta", 3),
    ("customs", 3),
    ("통관", 3),
    ("bonded", 3),
    ("보세", 3),

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
    if score >= 12:
        return "상"
    if score >= 7:
        return "중"
    return "하"

# ===============================
# COUNTRY TAG
# ===============================
COUNTRY_KEYWORDS = {
    "USA": ["u.s.", "united states", "america", "section 301", "section 232", "u.s.-", "us "],
    "India": ["india"],
    "Türkiye": ["turkey", "türkiye"],
    "Vietnam": ["vietnam"],
    "Netherlands": ["netherlands", "dutch"],
    "EU": ["european union", "eu commission", "european commission"],
    "China": ["china", "prc"],
    "Mexico": ["mexico"],
    "Brazil": ["brazil"],
    "UK": ["united kingdom", "uk "],
    "Japan": ["japan"],
    "Korea": ["korea", "korean"],
}

def detect_country(text: str) -> str:
    t = (text or "").lower()
    for country, keys in COUNTRY_KEYWORDS.items():
        if any(k in t for k in keys):
            return country
    return ""

# ===============================
# MULTI-LANGUAGE QUERY EXPANSION
# ===============================
# 최소/안전 확장: "제시어" 하나당 (ko + en + fr) 확장 키워드 묶음 생성
QUERY_TRANSLATION = {
    "관세": ["tariff", "customs duty", "droits de douane", "tarif douanier"],
    "세관": ["customs", "douane"],
    "수출입": ["import export", "imports exports", "importation exportation"],
    "전략물자": ["export control", "contrôle des exportations"],
    "외국환거래": ["foreign exchange", "devises", "réglementation des changes"],
    "보세공장": ["bonded factory", "bonded zone", "zone sous douane"],
    "원산지": ["rules of origin", "origin", "règles d'origine"],
    "fta": ["fta", "free trade agreement", "accord de libre-échange"],
    "aeo": ["aeo", "authorized economic operator", "opérateur économique agréé"],
    "wco": ["wco", "world customs organization", "organisation mondiale des douanes"],
}

def expand_query(base: str) -> list[str]:
    """
    base 제시어를 받아 ko/en/fr 확장 쿼리 리스트 반환.
    - base가 사전에 있으면 사전 기반 확장
    - 없으면 base 자체 + (base 포함시 common terms)로 제한 확장
    """
    b = (base or "").strip()
    if not b:
        return []

    out = [b]

    # 사전 기반 확장
    if b in QUERY_TRANSLATION:
        out.extend(QUERY_TRANSLATION[b])

    # 추가적으로 "관세/통관/원산지/fta" 같은 핵심 키워드와 OR 조합을 만들기 쉽게
    # (너무 공격적으로 확장하면 노이즈가 늘어서, 여기서는 보수적으로 유지)
    # ex) '보세공장'은 bonded zone과 같이 검색

    # 중복 제거
    seen = set()
    uniq = []
    for q in out:
        q2 = " ".join(q.split()).strip()
        if q2 and q2.lower() not in seen:
            seen.add(q2.lower())
            uniq.append(q2)
    return uniq

def build_google_news_rss(q: str) -> str:
    return "https://news.google.com/rss/search?" + urlencode({
        "q": q,
        "hl": GOOGLE_HL,
        "gl": GOOGLE_GL,
        "ceid": GOOGLE_CEID
    })

# ===============================
# TIGHT LOADERS
# ===============================
def load_custom_queries_txt(path: str) -> list[str]:
    """
    custom_queries.TXT: 한 줄 = 제시어
    - 공백/빈줄 제거
    - 중복 제거(대소문자/공백 정규화)
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"custom_queries.TXT not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        raw = f.read().splitlines()

    cleaned = []
    seen = set()
    for line in raw:
        s = " ".join((line or "").strip().split())
        if not s:
            continue
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(s)

    if not cleaned:
        raise ValueError(f"custom_queries.TXT is empty after cleaning: {path}")

    return cleaned

def load_sites_xlsx(path: str) -> pd.DataFrame:
    """
    sites.xlsx 구조(스크린샷 기준)
    - Sheet: SiteList
    - Required columns: name, url
    """
    if not os.path.exists(path):
        # 운영상 없어도 뉴스 센서는 돌 수 있게: 빈 DF 반환
        return pd.DataFrame(columns=["name", "url"])

    xls = pd.ExcelFile(path)
    if "SiteList" not in xls.sheet_names:
        raise ValueError(f"sites.xlsx must contain sheet 'SiteList'. Found: {xls.sheet_names}")

    df = pd.read_excel(path, sheet_name="SiteList")
    cols = [c.strip() for c in df.columns.astype(str).tolist()]
    df.columns = cols

    required = ["name", "url"]
    for r in required:
        if r not in df.columns:
            raise ValueError(f"sites.xlsx[SiteList] missing required column '{r}'. Columns: {df.columns.tolist()}")

    df = df[required].copy()
    df["name"] = df["name"].astype(str).str.strip()
    df["url"] = df["url"].astype(str).str.strip()

    df = df[(df["name"] != "") & (df["url"] != "")]
    df = df.drop_duplicates(subset=["url"], keep="first").reset_index(drop=True)
    return df

# ===============================
# DEDUP HELPERS
# ===============================
def normalize_title(t: str) -> str:
    t = (t or "").strip()
    t = unicodedata.normalize("NFKC", t)
    t = re.sub(r"\s+", " ", t)
    t = t.lower()
    # Google News가 붙이는 잡문구 최소 제거(보수적)
    t = t.replace(" - google 뉴스", "").replace(" - google news", "")
    return t

def canonicalize_url(u: str) -> str:
    if not u:
        return ""
    try:
        parts = urlsplit(u)
        # query에서 tracking 일부 제거(보수적으로 utm만)
        q = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
             if not k.lower().startswith("utm_")]
        new_query = urlencode(q, doseq=True)
        clean = urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, ""))  # fragment drop
        return clean
    except Exception:
        return u.strip()

# ===============================
# TOP3 FILTER (노이즈 컷)
# ===============================
ALLOW = [
    "관세","tariff","customs duty","관세율","hs","section 232","section 301","ieepa",
    "fta","원산지","rules of origin","무역구제","anti-dumping","countervailing",
    "수출통제","export control","sanction","통관","customs","bonded","보세"
]
BLOCK = [
    "시위","protest","체포","arrest","충돌","violent",
    "immigration","ice raid","연방정부","주정부",
    "murder","homicide","assault","robbery"  # 뉴스 노이즈 보수적 차단
]

def is_valid_top3(row: dict) -> bool:
    blob = f"{row.get('헤드라인','')} {row.get('주요내용','')}".lower()
    if any(b in blob for b in BLOCK):
        return False
    return any(a in blob for a in ALLOW)

# ===============================
# SENSOR
# ===============================
def parse_entry_time_kst(entry) -> dt.datetime | None:
    """
    feedparser가 제공하는 published_parsed / updated_parsed가 있으면 사용.
    RSS 시간은 보통 UTC 기반 튜플이므로 dt로 변환 후 KST로 보정.
    """
    t = None
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        t = entry.published_parsed
    elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
        t = entry.updated_parsed

    if not t:
        return None

    try:
        utc_dt = dt.datetime(*t[:6])
        return utc_dt + dt.timedelta(hours=9)
    except Exception:
        return None

def run_sensor_build_df(queries: list[str]) -> pd.DataFrame:
    """
    custom_queries.TXT 제시어 기반으로 (ko+en+fr 확장) Google News RSS 수집 → DF 생성
    + 07:00~07:00(KST) 윈도우 필터
    + dedup
    """
    start_kst, end_kst = window_kst()

    rows = []
    seen = set()

    for base in queries:
        expanded = expand_query(base)

        # base 제시어 하나당, 확장 쿼리를 OR로 묶어 "한 번"만 던지는 방식도 가능하지만
        # RSS 결과 품질/노이즈를 고려해 여기선 쿼리별로 가져온 뒤 dedup합니다.
        for q in expanded:
            rss = build_google_news_rss(q)
            feed = feedparser.parse(rss)

            for e in feed.entries[:MAX_PER_QUERY]:
                title = getattr(e, "title", "").strip()
                link = canonicalize_url(getattr(e, "link", "").strip())
                published_txt = getattr(e, "published", "") or getattr(e, "updated", "")

                summary = getattr(e, "summary", "") or ""
                summary = re.sub(r"<[^>]+>", "", summary).strip()
                summary = re.sub(r"\s+", " ", summary)

                ts_kst = parse_entry_time_kst(e)
                if ts_kst is not None:
                    if not (start_kst <= ts_kst < end_kst):
                        continue

                # dedup key: canonical link 우선, 없으면 title 기반
                key_link = link.lower().strip()
                key_title = normalize_title(title)
                key = key_link if key_link else key_title

                # 보조키: (source, title)
                src = getattr(e, "source", None)
                src_title = ""
                try:
                    src_title = getattr(src, "title", "") or ""
                except Exception:
                    src_title = ""
                key2 = (src_title.strip().lower(), key_title)

                if (key and key in seen) or (key2 in seen):
                    continue
                if key:
                    seen.add(key)
                seen.add(key2)

                country = detect_country(f"{title} {summary}")
                score = calc_policy_score(title, summary)
                importance = score_to_importance(score)

                rows.append({
                    "제시어": base,               # “파일의 제시어” 기준으로 묶음 유지
                    "검색어": q,                 # 디버깅/품질추적용(엑셀에는 남김, 메일 표에는 숨김)
                    "헤드라인": title,
                    "주요내용": summary[:500],
                    "대상 국가": country,
                    "중요도": importance,
                    "발표일": published_txt if published_txt else (fmt_kst(ts_kst) if ts_kst else ""),
                    "출처(URL)": link,
                    "점수": score,
                })

                if len(rows) >= MAX_TOTAL_ROWS:
                    break
            if len(rows) >= MAX_TOTAL_ROWS:
                break
        if len(rows) >= MAX_TOTAL_ROWS:
            break

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # 정렬: 점수 desc, 발표일/텍스트는 불완전하므로 점수 중심
    df = df.sort_values(["점수"], ascending=[False]).reset_index(drop=True)
    return df

# ===============================
# SAFE COLUMNS
# ===============================
def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for c in ["제시어","헤드라인","주요내용","대상 국가","중요도","발표일","출처(URL)","점수"]:
        if c not in df.columns:
            df[c] = ""

    # 점수 없으면 중요도로 기본 매핑
    if df["점수"].isna().all():
        score_map = {"상": 9, "중": 6, "하": 3}
        df["점수"] = df["중요도"].map(score_map).fillna(1)

    # 중요도 없으면 점수로 생성
    if df["중요도"].isna().all() or (df["중요도"].astype(str).str.strip() == "").all():
        df["중요도"] = df["점수"].apply(lambda x: score_to_importance(int(x) if str(x).isdigit() else 1))

    # 국가 없으면 감지
    df.loc[df["대상 국가"].astype(str).str.strip() == "", "대상 국가"] = df.apply(
        lambda r: detect_country(f"{r.get('헤드라인','')} {r.get('주요내용','')}"),
        axis=1
    )
    return df

# ===============================
# LINK
# ===============================
def get_link(r: dict) -> str:
    for c in ["출처(URL)", "URL", "link", "원본링크", "originallink"]:
        v = r.get(c, "")
        if isinstance(v, str) and v.strip():
            return v.strip()
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
.mono{font-family:Consolas,Menlo,monospace;}
</style>
"""

# ===============================
# EXEC TOP3 (공용)
# ===============================
def select_top3(df: pd.DataFrame) -> pd.DataFrame:
    cand = df[df.apply(lambda r: is_valid_top3(r.to_dict()), axis=1)]
    if cand.empty:
        cand = df.copy()
    return cand.sort_values("점수", ascending=False).head(3)

def build_exec_top3_html(top3: pd.DataFrame) -> str:
    items = ""
    for _, r in top3.iterrows():
        items += f"""
        <li>
          <b>[{html.escape(str(r.get('제시어','')))} | {html.escape(str(r.get('대상 국가','')))} | 점수 {html.escape(str(r.get('점수','')))}]</b><br/>
          <a href="{html.escape(get_link(r.to_dict()))}" target="_blank">{html.escape(str(r.get('헤드라인','')))}</a><br/>
          <div class="small">{html.escape(str(r.get('주요내용',''))[:220])}</div>
        </li>
        """
    return f"""
    <div class="box">
      <h3>① Executive Insight TOP3</h3>
      <ul>{items}</ul>
    </div>
    <div class="box">
      <h3>② 왜 중요한가</h3>
      <div class="small">
        관세/FTA/원산지/수출통제 등 정책성 이벤트는 <b>수입원가·판매가·마진·리드타임</b>에 직접 영향.
        적용 시점/대상국/대상품목(HS) 기준으로 법인 영향(생산→판매) 우선 스크리닝 권고.
      </div>
    </div>
    <div class="box">
      <h3>③ Action</h3>
      <div class="small">
        1) 대상국/품목(HS) 확인 → 2) 법인 영향(원가/마진/리드타임) 1차 산정 → 3) 필요 시 HQ 리스크 대응 착수
      </div>
    </div>
    """

# ===============================
# HTML BUILD (실무자용)
# ===============================
def build_html_practitioner(df: pd.DataFrame) -> str:
    date = now_kst().strftime("%Y-%m-%d")
    start_kst, end_kst = window_kst()

    top3 = select_top3(df)
    exec_block = build_exec_top3_html(top3)

    # "정책 센서 전용 표" (요구 반영)
    # - 출처 칼럼 삭제
    # - 헤드라인에 링크
    # - 헤드라인/주요내용을 1칸에 표기
    # - 비고/불필요 칼럼 제거
    rows = ""
    for _, r in df.iterrows():
        headline = html.escape(str(r.get("헤드라인","")))
        summary = html.escape(str(r.get("주요내용","")))
        link = html.escape(get_link(r.to_dict()))
        rows += f"""
        <tr>
          <td>{html.escape(str(r.get("제시어","")))} ({html.escape(str(r.get("중요도","")))})</td>
          <td>
            <a href="{link}" target="_blank">{headline}</a><br/>
            <div class="small">{summary}</div>
          </td>
          <td class="mono">{html.escape(str(r.get("발표일","")))}</td>
          <td>{html.escape(str(r.get("대상 국가","")))}</td>
          <td>점수 {html.escape(str(r.get("점수","")))}</td>
        </tr>
        """

    return f"""
    <html>
    <head>{STYLE}</head>
    <body>
    <div class="page">
      <h2>관세·통상 뉴스 브리핑 ({date})</h2>
      <div class="small">수집 윈도우(KST): {fmt_kst(start_kst)} ~ {fmt_kst(end_kst)}</div>

      {exec_block}

      <div class="box">
        <h3>④ 정책 이벤트 표</h3>
        <table>
          <tr>
            <th>제시어(중요도)</th>
            <th>헤드라인 / 주요내용</th>
            <th>발표일</th>
            <th>국가</th>
            <th>점수</th>
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
    date = now_kst().strftime("%Y-%m-%d")
    start_kst, end_kst = window_kst()
    top3 = select_top3(df)

    items = ""
    for _, r in top3.iterrows():
        items += f"""
        <li>
          <b>[{html.escape(str(r.get('대상 국가','')))} | 점수 {html.escape(str(r.get('점수','')))}]</b><br/>
          <a href="{html.escape(get_link(r.to_dict()))}" target="_blank">{html.escape(str(r.get('헤드라인','')))}</a><br/>
          <div class="small">{html.escape(str(r.get('주요내용',''))[:220])}</div>
        </li>
        """

    return f"""
    <html><head>{STYLE}</head>
    <body>
      <div class="page">
        <h2>[Executive] 관세·통상 핵심 TOP3 ({date})</h2>
        <div class="small">수집 윈도우(KST): {fmt_kst(start_kst)} ~ {fmt_kst(end_kst)}</div>
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
def write_outputs(df: pd.DataFrame, html_body: str) -> tuple[str,str,str]:
    today = now_kst().strftime("%Y-%m-%d")
    csv_path  = os.path.join(BASE_DIR, f"policy_events_{today}.csv")
    xlsx_path = os.path.join(BASE_DIR, f"policy_events_{today}.xlsx")
    html_path = os.path.join(BASE_DIR, f"policy_events_{today}.html")

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_body)

    return csv_path, xlsx_path, html_path

# ===============================
# MAIL
# ===============================
def send_mail_to(recipients: list[str], subject: str, html_body: str):
    if not recipients:
        return
    if not SMTP_SERVER or not SMTP_EMAIL or not SMTP_PASSWORD:
        raise ValueError("SMTP env missing: SMTP_SERVER/SMTP_EMAIL/SMTP_PASSWORD")

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
    # 0) 타이트 로더
    queries = load_custom_queries_txt(QUERIES_FILE)
    _sites_df = load_sites_xlsx(SITES_XLSX)  # vNext에서 본격 사용 (지금은 로드/검증만)

    # 1) 센서
    df = run_sensor_build_df(queries)
    if df is None or df.empty:
        print("오늘 수집된 이벤트/뉴스 없음")
        return

    df = ensure_cols(df)

    # 2) 실무자 메일(임원 TOP3 포함)
    today = now_kst().strftime("%Y-%m-%d")
    start_kst, end_kst = window_kst()
    html_body = build_html_practitioner(df)
    write_outputs(df, html_body)

    subj_prac = f"관세·통상 뉴스 브리핑 ({today})"
    send_mail_to(RECIPIENTS, subj_prac, html_body)

    # 3) 임원 메일(TOP3)
    exec_html = build_html_exec(df)
    subj_exec = f"[Executive] 관세·통상 핵심 TOP3 ({today})"
    send_mail_to(RECIPIENTS_EXEC, subj_exec, exec_html)

    print("✅ STABLE 완료: 제시어 기반 수집 + 07~07 필터 + dedup + 다국어 + 표 정리 + 임원/실무 분리")
    print("WINDOW(KST) =", fmt_kst(start_kst), "~", fmt_kst(end_kst))
    print("BASE_DIR =", BASE_DIR)
    print("OUT_FILES =", os.listdir(BASE_DIR))

if __name__ == "__main__":
    main()
