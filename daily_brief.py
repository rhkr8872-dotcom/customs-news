# -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief
vCurrent STABLE (GitHub Actions)

목표: "일단 매일 안정적으로 돌아가게" (A)

핵심:
- custom_queries.TXT(제시어) + sites.xlsx(정부/공인 사이트 목록) 기반 센서
- Google News RSS로 다국어(ko/en/fr) 검색어 확장
- 07:00~익일 07:00(KST) 수집 윈도우 적용
- 중복 제거(링크/제목 기반)
- 실무자 메일(표 중심) + 임원 메일(TOP3 + Why/Action) 분리
- 표에서 "출처" 컬럼 삭제: 헤드라인에 링크, 요약은 같은 칸에 표기
- out/에 CSV/XLSX/HTML 저장 + (옵션) repo에 커밋

※ Gemini 분석/요약/당사연관성은 vNext(확장형)에서 강화.
   다만, GEMINI_API_KEY가 있으면 '요약(한글)' 보강에만 선택적으로 사용(실패해도 계속 진행).
"""

from __future__ import annotations

import os
import re
import html
import json
import time
import math
import smtplib
import traceback
import datetime as dt
from typing import Dict, List, Tuple, Optional
from urllib.parse import urlencode, urlparse

import pandas as pd
import feedparser

# -------------------------------
# ENV
# -------------------------------
SMTP_SERVER   = os.getenv("SMTP_SERVER")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_EMAIL    = os.getenv("SMTP_EMAIL")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

RECIPIENTS       = [x.strip() for x in os.getenv("RECIPIENTS", "").split(",") if x.strip()]
RECIPIENTS_EXEC  = [x.strip() for x in os.getenv("RECIPIENTS_EXEC", "").split(",") if x.strip()]

BASE_DIR = os.getenv("BASE_DIR", os.path.join(os.path.dirname(__file__), "out"))
os.makedirs(BASE_DIR, exist_ok=True)

CUSTOM_QUERIES_FILE = os.getenv("CUSTOM_QUERIES_FILE", os.path.join(os.path.dirname(__file__), "custom_queries.TXT"))
SITES_FILE          = os.getenv("SITES_FILE", os.path.join(os.path.dirname(__file__), "sites.xlsx"))

NEWS_PER_KEYWORD    = int(os.getenv("NEWS_PER_KEYWORD", "30"))   # RSS에서 키워드별 최대 수집
MAX_PER_KEYWORD_OUT = int(os.getenv("MAX_PER_KEYWORD_OUT", "10"))# 표 출력 시 키워드별 상한
TOTAL_MAX_OUT       = int(os.getenv("TOTAL_MAX_OUT", "120"))     # 표 전체 상한 (안정성)

# 수집 윈도우 (KST) : 07:00 ~ 익일 07:00
WINDOW_START_HOUR_KST = int(os.getenv("WINDOW_START_HOUR_KST", "7"))

# 다국어 검색 on/off
ENABLE_MULTI_LANG = os.getenv("ENABLE_MULTI_LANG", "1").strip() not in ("0", "false", "False")

# Gemini (옵션)
GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL     = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")  # 필요 시 사용자가 변경
GEMINI_TIMEOUT_S = int(os.getenv("GEMINI_TIMEOUT_S", "20"))

# -------------------------------
# TIME
# -------------------------------
def now_kst() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).astimezone(dt.timezone(dt.timedelta(hours=9)))

def kst_window() -> Tuple[dt.datetime, dt.datetime]:
    """07:00~익일 07:00(KST)"""
    now = now_kst()
    today_0700 = now.replace(hour=WINDOW_START_HOUR_KST, minute=0, second=0, microsecond=0)
    if now < today_0700:
        end = today_0700
        start = end - dt.timedelta(days=1)
    else:
        start = today_0700
        end = start + dt.timedelta(days=1)
    return start, end

def fmt_kst(d: dt.datetime) -> str:
    if d is None:
        return ""
    return d.astimezone(dt.timezone(dt.timedelta(hours=9))).strftime("%Y-%m-%d %H:%M")

# -------------------------------
# TIGHT LOADERS
# -------------------------------
def load_custom_queries_txt(path: str) -> List[str]:
    """custom_queries.TXT: 1줄=1 제시어. 공백/중복 제거."""
    if not os.path.exists(path):
        print(f"[WARN] custom_queries file not found: {path}")
        return ["관세"]

    items: List[str] = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            q = line.strip()
            if not q:
                continue
            items.append(q)

    # 중복 제거(원형 유지)
    seen = set()
    out = []
    for q in items:
        k = q.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(q)
    return out or ["관세"]

def _normalize_url(u) -> str:
    """NaN/float/None 안전 + 공백 제거."""
    if u is None:
        return ""
    # pandas NaN
    try:
        if pd.isna(u):
            return ""
    except Exception:
        pass
    # 숫자/float 등은 문자열로
    if not isinstance(u, str):
        u = str(u)
    u = u.strip()
    if not u:
        return ""
    # 엑셀 하이퍼링크/텍스트가 URL 아닌 경우 제거(예: '월간 통상')
    if not re.match(r"^https?://", u, re.I):
        return ""
    return u

def load_sites_xlsx(path: str) -> Tuple[Dict[str, str], set]:
    """
    sites.xlsx 구조(사용자 제공):
      - Sheet: 'SiteList'
      - Columns: 'name', 'url' (필수)
    반환:
      - domain_to_name: {domain: 기관명}
      - allowed_domains: set(domains)
    """
    if not os.path.exists(path):
        print(f"[WARN] sites.xlsx not found: {path}")
        return {}, set()

    # sheet name 고정(타이트)
    sheet = "SiteList"
    xls = pd.ExcelFile(path)
    if sheet not in xls.sheet_names:
        raise ValueError(f"sites.xlsx must contain sheet '{sheet}'. Found: {xls.sheet_names}")

    df = pd.read_excel(path, sheet_name=sheet)

    # 컬럼 타이트 체크
    cols = [c.strip() for c in df.columns.astype(str)]
    df.columns = cols
    required = {"name", "url"}
    missing = required - set(cols)
    if missing:
        raise ValueError(f"sites.xlsx '{sheet}' sheet must have columns {sorted(required)}. Missing: {sorted(missing)}")

    df["name"] = df["name"].astype(str).str.strip()
    df["url"]  = df["url"].apply(_normalize_url)

    df = df[(df["name"] != "") & (df["url"] != "")].copy()

    # 도메인 추출
    def _domain(u: str) -> str:
        try:
            return urlparse(u).netloc.lower()
        except Exception:
            return ""

    df["domain"] = df["url"].apply(_domain)
    df = df[df["domain"] != ""].copy()

    # 같은 domain 중복은 첫 항목 우선
    domain_to_name: Dict[str, str] = {}
    for _, r in df.iterrows():
        dom = r["domain"]
        if dom not in domain_to_name:
            domain_to_name[dom] = r["name"]

    allowed_domains = set(domain_to_name.keys())
    return domain_to_name, allowed_domains

# -------------------------------
# SEARCH QUERY EXPANSION (ko/en/fr)
# -------------------------------
EXPANSIONS = {
    # KR -> (EN, FR)
    "관세": ("tariff OR customs duty OR duties", "tarif OR droits de douane"),
    "관세율": ("tariff rate OR duty rate", "taux de droit OR taux tarifaire"),
    "세관": ("customs OR customs authority", "douane OR administration des douanes"),
    "통관": ("customs clearance", "dédouanement"),
    "수출입": ("import OR export", "importation OR exportation"),
    "수출통제": ("export control", "contrôle des exportations"),
    "전략물자": ("strategic goods OR dual-use", "biens à double usage"),
    "원산지": ("rules of origin OR origin", "règles d'origine OR origine"),
    "fta": ("FTA OR free trade agreement", "accord de libre-échange"),
    "a eo": ("AEO OR authorized economic operator", "OEA OR opérateur économique agréé"),
    "aeo": ("AEO OR authorized economic operator", "OEA OR opérateur économique agréé"),
    "wco": ("WCO OR World Customs Organization", "OMD OR Organisation mondiale des douanes"),
}

def build_google_news_rss_query(keyword: str) -> str:
    """
    Google News RSS query string.
    기본: keyword(그대로)
    다국어: keyword OR (영문 확장) OR (불문 확장)
    """
    k = (keyword or "").strip()
    if not k:
        k = "관세"

    if not ENABLE_MULTI_LANG:
        return k

    key_norm = k.lower().strip()
    en, fr = ("", "")
    # 유사 키 매칭
    for base, (en0, fr0) in EXPANSIONS.items():
        if base in key_norm:
            en, fr = en0, fr0
            break

    if not en:
        # 모르는 키워드라도 기본 관세/통상 키워드를 함께 걸어서 누락 방지
        en = "tariff OR customs OR trade"
        fr = "tarif OR douane OR commerce"

    # query는 따옴표로 묶지 않음(뉴스에서 phrase는 때로 누락)
    return f"({k}) OR ({en}) OR ({fr})"

# -------------------------------
# PARSE PUBLISHED TIME
# -------------------------------
def parse_published_dt(published: str) -> Optional[dt.datetime]:
    """
    RSS published 문자열 파싱(대부분 RFC822).
    실패 시 None.
    """
    if not published:
        return None
    try:
        # feedparser가 parsedate 제공하는 경우가 많음
        t = feedparser._parse_date(published)
        if t:
            return dt.datetime(*t[:6], tzinfo=dt.timezone.utc)
    except Exception:
        pass
    # 최후: email.utils
    try:
        from email.utils import parsedate_to_datetime
        d = parsedate_to_datetime(published)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(dt.timezone.utc)
    except Exception:
        return None

# -------------------------------
# DEDUP
# -------------------------------
_PUB_SUFFIX = re.compile(r"\s*-\s*[^-]{2,50}$")  # "Title - Publisher"
_WS = re.compile(r"\s+")

def norm_title(t: str) -> str:
    t = (t or "").strip()
    t = html.unescape(t)
    t = _PUB_SUFFIX.sub("", t)
    t = _WS.sub(" ", t)
    return t.lower()

def canon_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    # google news redirect라도 동일 링크는 동일로 처리
    # tracking param 제거(가능한 범위)
    try:
        p = urlparse(u)
        q = p.query
        # 너무 공격적으로 제거하지 않음
        return (p.scheme + "://" + p.netloc + p.path).lower()
    except Exception:
        return u.lower()

# -------------------------------
# POLICY SCORE (간단/안정형)
# -------------------------------
RISK_RULES = [
    ("section 301", 6),
    ("section 232", 6),
    ("tariff act", 6),
    ("trade expansion act", 6),
    ("ieepa", 6),
    ("international emergency economic powers act", 6),
    ("export control", 6),
    ("sanction", 6),
    ("entity list", 5),
    ("anti-dumping", 5),
    ("countervailing", 5),
    ("safeguard", 5),

    ("tariff", 4),
    ("duty", 4),
    ("customs duty", 4),
    ("관세", 4),
    ("관세율", 4),
    ("추가관세", 4),

    ("hs code", 3),
    ("hs", 3),
    ("origin", 3),
    ("rules of origin", 3),
    ("원산지", 3),
    ("fta", 3),
    ("customs", 3),
    ("통관", 3),

    ("amend", 2),
    ("revision", 2),
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

# -------------------------------
# COUNTRY TAG (heuristic)
# -------------------------------
COUNTRY_KEYWORDS = {
    "Korea": ["korea", "korean", "대한민국", "한국"],
    "USA": ["u.s.", "united states", "america", "section 301", "section 232"],
    "EU": ["european union", "eu commission", "european commission"],
    "China": ["china", "chinese"],
    "Vietnam": ["vietnam"],
    "India": ["india"],
    "Indonesia": ["indonesia"],
    "Türkiye": ["turkey", "türkiye"],
    "Slovakia": ["slovakia"],
    "Poland": ["poland"],
    "Mexico": ["mexico"],
    "Brazil": ["brazil"],
    "Netherlands": ["netherlands", "dutch"],
}

def detect_country(text: str) -> str:
    t = (text or "").lower()
    for country, keys in COUNTRY_KEYWORDS.items():
        if any(k in t for k in keys):
            return country
    return ""

# -------------------------------
# Gemini (optional) - 요약(한글)만 보강
# -------------------------------
def gemini_summarize_ko(title: str, snippet: str) -> str:
    """
    GEMINI_API_KEY가 있을 때만 호출.
    실패/미설정 시 snippet 그대로 반환.
    """
    base = (snippet or "").strip()
    if not GEMINI_API_KEY:
        return base

    # 너무 긴 텍스트는 제한
    text = (title or "").strip()
    if base:
        text += "\n\n" + base[:1200]

    # REST 호출 (requests 없으면 실패 -> 그대로)
    try:
        import requests
    except Exception:
        return base

    prompt = f"""아래 뉴스 제목/요약을 바탕으로 '관세/통상 관점'에서 2~3문장 한국어 요약을 작성해줘.
- 사실확인이 불가능한 내용은 추정하지 말고, 원문에 있는 범위에서만 요약.
- 관세, 관세율, Tariff Act, Trade Expansion Act, IEEPA, 수출통제/제재, FTA/원산지, 무역구제(AD/CVD/SG) 언급이 있으면 강조.
제목/요약:
{text}
"""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    headers = {"Content-Type": "application/json"}
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 200},
    }
    try:
        r = requests.post(url, params={"key": GEMINI_API_KEY}, headers=headers, json=payload, timeout=GEMINI_TIMEOUT_S)
        if r.status_code != 200:
            return base
        data = r.json()
        cand = data.get("candidates", [])
        if not cand:
            return base
        parts = cand[0].get("content", {}).get("parts", [])
        if not parts:
            return base
        out = (parts[0].get("text") or "").strip()
        # 결과가 너무 짧거나 영어-only면 fallback
        if len(out) < 20:
            return base
        return out
    except Exception:
        return base

# -------------------------------
# SENSOR
# -------------------------------
def fetch_google_news(keyword: str, window_start: dt.datetime, window_end: dt.datetime) -> List[dict]:
    """
    Google News RSS fetch.
    """
    q = build_google_news_rss_query(keyword)
    rss_url = "https://news.google.com/rss/search?" + urlencode({
        "q": q,
        "hl": "ko",
        "gl": "KR",
        "ceid": "KR:ko",
    })
    feed = feedparser.parse(rss_url)

    out = []
    for e in feed.entries[:NEWS_PER_KEYWORD]:
        title = getattr(e, "title", "").strip()
        link  = getattr(e, "link", "").strip()
        published = getattr(e, "published", "") or getattr(e, "updated", "")
        pub_dt_utc = parse_published_dt(published)
        if pub_dt_utc:
            pub_kst = pub_dt_utc.astimezone(dt.timezone(dt.timedelta(hours=9)))
            if not (window_start <= pub_kst < window_end):
                continue

        summary = getattr(e, "summary", "") or getattr(e, "description", "")
        summary = re.sub(r"<[^>]+>", "", summary or "").strip()

        # title==summary 문제 방지
        if norm_title(summary) == norm_title(title):
            summary = ""

        out.append({
            "제시어": keyword,
            "헤드라인": title,
            "요약(원문)": summary[:1200],
            "발표일_raw": published,
            "발표일_dt": pub_dt_utc,  # UTC
            "출처(URL)": link,
        })
    return out

def build_events_df(queries: List[str], domain_to_name: Dict[str, str], allowed_domains: set) -> pd.DataFrame:
    w_start, w_end = kst_window()
    rows: List[dict] = []

    for kw in queries:
        items = fetch_google_news(kw, w_start, w_end)
        for it in items:
            title = it["헤드라인"]
            snippet = it["요약(원문)"]
            link = it["출처(URL)"]

            dom = urlparse(link).netloc.lower() if link else ""
            기관 = domain_to_name.get(dom, "")

            # 국가/점수
            country = detect_country(f"{title} {snippet}")
            score = calc_policy_score(title, snippet)

            # 중요도(상/중/하) - 안정형 룰
            if score >= 12:
                imp = "상"
            elif score >= 7:
                imp = "중"
            else:
                imp = "하"

            # 한글 요약(옵션 gemini)
            summary_ko = gemini_summarize_ko(title, snippet)
            summary_ko = (summary_ko or "").strip()

            rows.append({
                "제시어": it["제시어"],
                "중요도": imp,
                "점수": score,
                "헤드라인": title,
                "요약(한글)": summary_ko,
                "발표일": fmt_kst(it["발표일_dt"]) if it["발표일_dt"] else "",
                "대상 국가": country,
                "관련 기관": 기관,
                "출처(URL)": link,
                "도메인": dom,
                "정부/공인도메인": "Y" if dom in allowed_domains else "",
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # 중복 제거(우선순위: 링크 > 제목)
    df["__urlkey"] = df["출처(URL)"].apply(canon_url)
    df["__titlekey"] = df["헤드라인"].apply(norm_title)
    df["__dedupkey"] = df.apply(lambda r: r["__urlkey"] or r["__titlekey"], axis=1)

    # 동일 키에서 점수 높은 것 우선
    df = df.sort_values(["__dedupkey", "점수"], ascending=[True, False])
    df = df.drop_duplicates(subset=["__dedupkey"], keep="first")

    # 출력 전, 키워드별 제한
    imp_order = {"하": 1, "중": 2, "상": 3}
    df["__imp"] = df["중요도"].map(imp_order).fillna(0).astype(int)

    df = df.sort_values(["제시어", "__imp", "점수"], ascending=[True, True, False])

    # 키워드별 10건 제한
    df = df.groupby("제시어", group_keys=False).head(MAX_PER_KEYWORD_OUT)

    # 전체 제한
    df = df.head(TOTAL_MAX_OUT).reset_index(drop=True)

    # cleanup
    df = df.drop(columns=["__urlkey", "__titlekey", "__dedupkey", "__imp"], errors="ignore")

    return df

# -------------------------------
# TOP3 FILTER
# -------------------------------
ALLOW = [
    "관세","tariff","관세율","hs","hs code","section 232","section 301","ieepa",
    "tariff act","trade expansion act","international emergency economic powers act",
    "fta","원산지","anti-dumping","countervailing","safeguard","무역구제",
    "수출통제","export control","sanction","제재","통관","customs"
]
BLOCK = [
    "시위","protest","체포","arrest","충돌","violent",
    "immigration","ice raid","연방정부","주정부"
]

def is_valid_top3(r: pd.Series) -> bool:
    blob = f"{r.get('헤드라인','')} {r.get('요약(한글)','')} {r.get('요약(원문)','')}".lower()
    if any(b in blob for b in BLOCK):
        return False
    return any(a in blob for a in ALLOW)

# -------------------------------
# HTML
# -------------------------------
STYLE = """
<style>
  body{font-family:Malgun Gothic,Arial; background:#f6f6f6; margin:0; padding:0;}
  .page{max-width:1120px;margin:auto;background:white;padding:14px;}
  h2{margin:6px 0 4px 0;}
  h3{margin:0 0 6px 0;}
  .box{border:1px solid #ddd;border-radius:8px;padding:12px;margin:12px 0;}
  li{margin-bottom:12px;}
  table{border-collapse:collapse;width:100%;}
  th,td{border:1px solid #ccc;padding:8px;font-size:12px;vertical-align:top;}
  th{background:#f0f0f0;}
  .small{font-size:11px;color:#555;line-height:1.35;}
  .muted{color:#777;}
  .counts{font-size:12px;color:#333;margin:6px 0 10px 0;}
  /* A4 landscape print */
  @page { size: A4 landscape; margin: 10mm; }
</style>
"""

def _escape(x) -> str:
    return html.escape("" if x is None else str(x))

def build_counts_line(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return ""
    c = df.groupby("제시어").size().sort_index()
    parts = [f"{k} {int(v)}건" for k, v in c.items()]
    return ", ".join(parts)

def build_table_rows(df: pd.DataFrame) -> str:
    rows = []
    for _, r in df.iterrows():
        link = r.get("출처(URL)", "#")
        headline = _escape(r.get("헤드라인", ""))
        summary = _escape(r.get("요약(한글)", ""))

        # headline + summary in one cell
        cell = f'<a href="{_escape(link)}" target="_blank">{headline}</a>'
        if summary:
            cell += f'<div class="small">{summary}</div>'

        rows.append(f"""
        <tr>
          <td>{_escape(r.get("제시어",""))}</td>
          <td>{_escape(r.get("중요도",""))}</td>
          <td>{cell}</td>
          <td>{_escape(r.get("발표일",""))}</td>
          <td>{_escape(r.get("대상 국가",""))}</td>
          <td>{_escape(r.get("관련 기관",""))}</td>
        </tr>
        """)
    return "\n".join(rows)

def build_html_practitioner(df: pd.DataFrame) -> str:
    ws, we = kst_window()
    date_line = f"{ws.strftime('%Y-%m-%d %H:%M')} ~ {we.strftime('%Y-%m-%d %H:%M')} (KST)"
    counts = build_counts_line(df)

    rows_html = build_table_rows(df)

    return f"""
    <html><head>{STYLE}</head>
    <body>
      <div class="page">
        <h2>관세·통상 정책 센서 (실무자용)</h2>
        <div class="small muted">수집범위: {date_line}</div>
        <div class="counts"><b>제시어별 수집:</b> { _escape(counts) }</div>

        <div class="box">
          <h3>④ 정책 이벤트 표</h3>
          <div class="small muted" style="margin:6px 0 8px 0;">(제시어별 중복 제거 후 최대 {MAX_PER_KEYWORD_OUT}건)</div>
          <table>
            <tr>
              <th>제시어</th>
              <th>중요도</th>
              <th>헤드라인 / 요약(한글)</th>
              <th>발표일</th>
              <th>대상 국가</th>
              <th>관련 기관</th>
            </tr>
            {rows_html}
          </table>
        </div>
      </div>
    </body></html>
    """

def build_exec_sections(top3: pd.DataFrame) -> Tuple[str, str, str]:
    """
    ① TOP3 / ② 왜 중요한가 / ③ 체크포인트
    - 중복 문구를 줄이고, '제품/생산지' 관점 문장을 포함(템플릿 기반).
    """
    if top3 is None or top3.empty:
        empty = "<div class='small muted'>해당 구간에 유의미한 TOP3 이벤트가 없습니다.</div>"
        return empty, empty, empty

    # TOP3 list
    li = []
    for _, r in top3.iterrows():
        link = r.get("출처(URL)", "#")
        headline = _escape(r.get("헤드라인", ""))
        summary = _escape(r.get("요약(한글)", ""))
        country = _escape(r.get("대상 국가", ""))
        score = _escape(r.get("점수", ""))
        kw = _escape(r.get("제시어", ""))

        line = f"<b>[{kw} | {country} | 점수 {score}]</b><br/>"
        line += f'<a href="{_escape(link)}" target="_blank">{headline}</a>'
        if summary:
            line += f"<div class='small'>{summary}</div>"
        li.append(f"<li>{line}</li>")

    top3_html = "<ul>" + "\n".join(li) + "</ul>"

    # why / checkpoint (통합형 템플릿)
    # 제품/생산지 관점 텍스트 삽입
    why_lines = []
    chk_lines = []

    for _, r in top3.iterrows():
        country = (r.get("대상 국가") or "").strip()
        kw = (r.get("제시어") or "").strip()
        score = r.get("점수", "")
        # 간단한 원인 키워드 추출
        blob = f"{r.get('헤드라인','')} {r.get('요약(한글)','')}".lower()
        cause = []
        for k in ["관세", "tariff", "section 301", "section 232", "ieepa", "export control", "sanction", "anti-dumping", "countervailing", "safeguard", "fta", "원산지", "hs"]:
            if k in blob:
                cause.append(k)
        cause_txt = ", ".join(cause[:4]) if cause else "관세/통상 정책"
        # why
        why_lines.append(
            f"<li><b>[{_escape(kw)} | { _escape(country) } | 점수 { _escape(score) }]</b> "
            f"{_escape(cause_txt)} 변화 가능성 → 수입원가/판매가(마진) 및 리드타임 영향 가능. "
            f"생산/판매법인(한국·중국·베트남·인도·인니·튀르키예·슬로바키아·폴란드·멕시코·브라질) 중 해당국 연계 여부 우선 점검.</li>"
        )
        # checkpoint
        chk_lines.append(
            f"<li><b>[{_escape(kw)} | { _escape(country) }]</b> "
            f"1) 적용시점·대상품목(HS)·세율/규정(Section 301/232, IEEPA 등) 확인 → "
            f"2) 해당 생산/판매법인 영향(원가·마진·납기) 1차 산정 → "
            f"3) 필요 시 FTA/원산지·수출통제/제재·무역구제 리스크 트리거 업데이트.</li>"
        )

    why_html = "<ul>" + "\n".join(why_lines) + "</ul>"
    chk_html = "<ul>" + "\n".join(chk_lines) + "</ul>"

    return top3_html, why_html, chk_html

def build_html_exec(df: pd.DataFrame) -> str:
    ws, we = kst_window()
    date_line = f"{ws.strftime('%Y-%m-%d %H:%M')} ~ {we.strftime('%Y-%m-%d %H:%M')} (KST)"

    cand = df[df.apply(is_valid_top3, axis=1)].copy()
    cand = cand.sort_values(["점수", "정부/공인도메인"], ascending=[False, False])
    top3 = cand.head(3)

    top3_html, why_html, chk_html = build_exec_sections(top3)

    return f"""
    <html><head>{STYLE}</head>
    <body>
      <div class="page">
        <h2>[Executive] 관세·통상 핵심 TOP3</h2>
        <div class="small muted">수집범위: {date_line}</div>

        <div class="box">
          <h3>① 관세·통상 핵심 TOP3</h3>
          {top3_html}
        </div>

        <div class="box">
          <h3>② 왜 중요한가 (TOP3 이벤트 기반)</h3>
          {why_html}
        </div>

        <div class="box">
          <h3>③ 당사 관점 체크포인트 (TOP3 이벤트 기반)</h3>
          {chk_html}
        </div>

        <div class="box">
          <h3>Action (요약)</h3>
          <div class="small">
            1) 대상국/품목(HS)·세율/규정(Section 301/232, Tariff Act, IEEPA 등) 확인<br/>
            2) 생산/판매법인 영향(원가·마진·리드타임) 1차 산정<br/>
            3) 필요 시 HQ 대응 착수(리스크 트리거, 법인 가이드, 대외 커뮤니케이션)
          </div>
        </div>
      </div>
    </body></html>
    """

# -------------------------------
# OUTPUTS
# -------------------------------
def write_outputs(df: pd.DataFrame, html_prac: str, html_exec: str) -> Tuple[str, str, str, str]:
    today = now_kst().strftime("%Y-%m-%d")
    csv_path  = os.path.join(BASE_DIR, f"policy_events_{today}.csv")
    xlsx_path = os.path.join(BASE_DIR, f"policy_events_{today}.xlsx")
    html_path = os.path.join(BASE_DIR, f"policy_events_{today}.html")
    html_exec_path = os.path.join(BASE_DIR, f"exec_top3_{today}.html")

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_prac)

    with open(html_exec_path, "w", encoding="utf-8") as f:
        f.write(html_exec)

    return csv_path, xlsx_path, html_path, html_exec_path

# -------------------------------
# MAIL
# -------------------------------
def send_mail_to(recipients: List[str], subject: str, html_body: str):
    if not recipients:
        print(f"[INFO] skip send: no recipients for {subject}")
        return
    if not (SMTP_SERVER and SMTP_EMAIL and SMTP_PASSWORD):
        raise RuntimeError("SMTP env is missing. Check SMTP_SERVER/SMTP_EMAIL/SMTP_PASSWORD.")

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
    print("BASE_DIR =", BASE_DIR)
    print("CUSTOM_QUERIES_FILE =", CUSTOM_QUERIES_FILE)
    print("SITES_FILE =", SITES_FILE)

    # 1) 로더
    queries = load_custom_queries_txt(CUSTOM_QUERIES_FILE)
    domain_to_name, allowed_domains = load_sites_xlsx(SITES_FILE)

    print(f"[INFO] loaded queries: {len(queries)}")
    print(f"[INFO] loaded allowed domains: {len(allowed_domains)}")

    # 2) 센서 실행
    df = build_events_df(queries, domain_to_name, allowed_domains)

    if df is None or df.empty:
        print("[INFO] no events in window. exit without sending.")
        return

    # 3) HTML 생성
    html_prac = build_html_practitioner(df)
    html_exec = build_html_exec(df)

    # 4) 출력 저장
    paths = write_outputs(df, html_prac, html_exec)
    print("[INFO] outputs:", paths)

    # 5) 메일 발송 (08시 스케줄은 workflow cron에서 처리)
    today = now_kst().strftime("%Y-%m-%d")
    send_mail_to(RECIPIENTS, f"관세·통상 정책 센서 (실무자용) ({today})", html_prac)
    send_mail_to(RECIPIENTS_EXEC, f"[Executive] 관세·통상 핵심 TOP3 ({today})", html_exec)

    print("✅ vCurrent STABLE 완료")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[ERROR]", e)
        traceback.print_exc()
        raise
