# -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief
vCurrent STABLE (E2E): Sensor + Outputs + Mail (Practitioner + Executive)

- custom_queries.TXT (제시어) 로드 (타이트)
- sites.xlsx (정부/공인 사이트 목록) 로드 (타이트)
- Google News RSS 수집 (ko/en/fr 기본)
- 07~07 (KST) 기준 최근 24h 필터
- 중복 제거
- 실무자/임원 메일 분리 발송
- out/에 CSV/XLSX/HTML 저장

ENV (GitHub Secrets/Variables)
  SMTP_SERVER, SMTP_PORT, SMTP_EMAIL, SMTP_PASSWORD
  RECIPIENTS              (실무자 수신자, 콤마)
  RECIPIENTS_EXEC         (임원 수신자, 콤마)  [없어도 OK]
  BASE_DIR                (기본: ./out)
  QUERIES_FILE            (기본: custom_queries.TXT)
  SITES_FILE              (기본: sites.xlsx)
  NEWS_LANGS              (기본: "ko,en,fr")
  MAX_ITEMS_PER_QUERY     (기본: 30)
  MAX_PER_KEYWORD         (표에서 제시어별 최대, 기본 10)
"""

import os
import re
import html as html_lib
import smtplib
import hashlib
import datetime as dt
from urllib.parse import urlencode, urlparse
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import parsedate_to_datetime

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
SITES_FILE   = os.getenv("SITES_FILE", os.path.join(os.path.dirname(__file__), "sites.xlsx"))

NEWS_LANGS = [x.strip() for x in os.getenv("NEWS_LANGS", "ko,en,fr").split(",") if x.strip()]
MAX_ITEMS_PER_QUERY = int(os.getenv("MAX_ITEMS_PER_QUERY", "30"))
MAX_PER_KEYWORD = int(os.getenv("MAX_PER_KEYWORD", "10"))


# ===============================
# TIME
# ===============================
def now_kst() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).astimezone(dt.timezone(dt.timedelta(hours=9)))

def window_kst_24h(now: dt.datetime):
    # “전날 07:00 ~ 당일 07:00”을 의도하셨지만,
    # 실행(08:00 KST) 시점 기준으로는 '최근 24h'가 가장 안정적입니다.
    # 필요 시 아래를 07:00 anchor 방식으로 바꿀 수 있습니다.
    end = now
    start = now - dt.timedelta(hours=24)
    return start, end

def safe_parse_published(published_str: str):
    if not published_str:
        return None
    try:
        d = parsedate_to_datetime(published_str)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(dt.timezone(dt.timedelta(hours=9)))
    except Exception:
        return None


# ===============================
# TIGHT LOADER: custom_queries.TXT
# ===============================
MULTI_LANG_MAP = {
    # ko -> (en/fr 확장)
    "관세": ["tariff", "customs duty", "droits de douane", "tarif douanier"],
    "관세율": ["tariff rate", "duty rate", "taux de droit de douane"],
    "세관": ["customs", "douanes"],
    "통관": ["customs clearance", "dédouanement"],
    "수출입": ["import export", "imports exports", "importation exportation"],
    "원산지": ["rules of origin", "origin", "règles d'origine", "origine"],
    "fta": ["FTA", "free trade agreement", "accord de libre-échange"],
    "전략물자": ["export control", "strategic goods", "contrôle des exportations"],
    "수출통제": ["export control", "contrôle des exportations"],
    "제재": ["sanctions", "sanction", "sanctions économiques"],
    "보세공장": ["bonded factory", "bonded zone", "zone sous douane"],
    "a﻿eo": ["AEO", "authorized economic operator", "opérateur économique agréé"],
    "aeo": ["AEO", "authorized economic operator", "opérateur économique agréé"],
    "wco": ["WCO", "world customs organization", "OMD", "organisation mondiale des douanes"],
}

def load_custom_queries(path: str) -> list[str]:
    if not os.path.exists(path):
        # 파일이 없으면 최소 안전 기본값
        return ["관세", "관세율", "세관", "통관", "원산지", "FTA", "수출통제", "제재"]

    queries = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            q = (line or "").strip()
            if not q:
                continue
            q = re.sub(r"\s+", " ", q)
            queries.append(q)

    # 중복 제거 (대소문자/공백 정규화)
    seen = set()
    out = []
    for q in queries:
        key = q.lower().strip()
        if key in seen:
            continue
        seen.add(key)
        out.append(q)
    return out

def expand_queries_multilang(base_queries: list[str], langs: list[str]) -> list[dict]:
    """
    반환: [{"keyword": "관세", "q": "...", "lang": "ko"}, ...]
    - ko/en/fr에 대해 검색쿼리를 구성
    - ko 제시어는 MULTI_LANG_MAP로 en/fr 확장어를 추가
    """
    items = []

    for kw in base_queries:
        kw_norm = kw.strip()
        kw_low = kw_norm.lower()

        expanded_terms = [kw_norm]
        # ko 제시어면 en/fr 확장어를 추가
        for k, terms in MULTI_LANG_MAP.items():
            if k.lower() == kw_low:
                expanded_terms = list(dict.fromkeys([kw_norm] + terms))  # preserve order
                break

        # lang별: ko는 원문 + (필요 시 OR로 묶어 확장), en/fr은 확장어 위주
        for lang in langs:
            if lang == "ko":
                q = " OR ".join([f'"{t}"' if " " in t else t for t in expanded_terms])
            else:
                # en/fr은 확장어 중 해당 언어로 “추정”되는 것만 고르기 어렵기 때문에
                # 안전하게 전체 확장어를 OR로 사용 (노이즈는 아래 필터가 줄여줌)
                q = " OR ".join([f'"{t}"' if " " in t else t for t in expanded_terms])

            items.append({"keyword": kw_norm, "q": q, "lang": lang})

    return items


# ===============================
# TIGHT LOADER: sites.xlsx
# ===============================
def _normalize_col(s: str) -> str:
    return re.sub(r"\s+", "", (s or "").strip().lower())

def _normalize_url(u):
    # 사이트.xlsx에서 float(빈칸 NaN)이 들어오면 여기서 터지던 문제 해결
    if u is None:
        return ""
    if isinstance(u, float):
        # NaN 포함
        if pd.isna(u):
            return ""
        return str(u).strip()
    if not isinstance(u, str):
        return str(u).strip()

    u = u.strip()
    if not u:
        return ""
    # 엑셀에서 '월간 통상'처럼 url에 텍스트만 있는 경우가 있어도 안전하게 제외
    if not re.match(r"^https?://", u, flags=re.I):
        return ""
    return u

def load_sites_xlsx(path: str):
    """
    기대 구조(타이트):
      - 최소 'name' / 'url' 컬럼이 있어야 함 (대소문자/공백/한글 컬럼명 일부 허용)
      - sheet는 첫 번째 시트를 기본으로 읽음
    반환:
      domain_to_name: { "customs.go.kr": "관세청", ... }
      allowed_domains: set(...)
    """
    domain_to_name = {}
    allowed = set()

    if not os.path.exists(path):
        return domain_to_name, allowed

    xl = pd.read_excel(path, sheet_name=None)
    # 첫 시트 사용(사용자가 SiteList 쓰는 케이스 많음)
    first_sheet_name = list(xl.keys())[0]
    df = xl[first_sheet_name].copy()

    # 컬럼명 정규화 매핑
    col_map = {c: _normalize_col(c) for c in df.columns}
    inv = {v: k for k, v in col_map.items()}

    # 허용 컬럼 후보
    name_col = None
    url_col = None

    for cand in ["name", "기관", "기관명", "사이트", "sitename"]:
        if cand in inv:
            name_col = inv[cand]
            break
    for cand in ["url", "링크", "주소", "siteurl"]:
        if cand in inv:
            url_col = inv[cand]
            break

    # 질문에서 보인 구조: A=name, B=url 이므로 우선 그걸 타이트하게 잡음
    if name_col is None and "name" in df.columns:
        name_col = "name"
    if url_col is None and "url" in df.columns:
        url_col = "url"

    if name_col is None or url_col is None:
        # 컬럼 구조가 다르면 사이트 매핑은 비활성 (실행은 계속)
        return domain_to_name, allowed

    df = df[[name_col, url_col]].rename(columns={name_col: "name", url_col: "url"})
    df["name"] = df["name"].astype(str).fillna("").apply(lambda x: (x or "").strip())
    df["url"] = df["url"].apply(_normalize_url)

    # url 비거나 http(s) 아닌 것은 제거
    df = df[df["url"].astype(str).str.len() > 0].copy()

    for _, r in df.iterrows():
        name = (r["name"] or "").strip()
        url = (r["url"] or "").strip()
        if not url:
            continue
        dom = urlparse(url).netloc.lower()
        if not dom:
            continue
        allowed.add(dom)
        if name and dom not in domain_to_name:
            domain_to_name[dom] = name

    return domain_to_name, allowed


# ===============================
# FILTER / SCORING
# ===============================
ALLOW_STRICT = [
    # 사용자가 “반드시 보여줄 것”으로 명시한 키워드 포함
    "관세", "관세율", "tariff", "duty",
    "tariff act", "trade expansion act", "international emergency economic powers act",
    "section 232", "section 301", "ieepa",
    "hs", "hs code",
    "origin", "rules of origin", "원산지",
    "export control", "수출통제", "strategic",
    "sanction", "제재",
    "customs", "세관", "통관",
    "fta",
    "anti-dumping", "countervailing", "safeguard",
]

BLOCK_NOISE = [
    "protest", "시위", "arrest", "체포", "violent", "충돌",
    "celebrity", "연예", "sports", "스포츠",
]

RISK_RULES = [
    ("trade expansion act", 8),
    ("international emergency economic powers act", 8),
    ("tariff act", 8),

    ("section 301", 7),
    ("section 232", 7),
    ("ieepa", 7),

    ("anti-dumping", 6),
    ("countervailing", 6),
    ("safeguard", 6),

    ("export control", 6),
    ("sanction", 6),
    ("entity list", 5),

    ("tariff", 4),
    ("duty", 4),
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

    ("개정", 2),
    ("시행", 2),
    ("고시", 2),
    ("regulation", 2),
]

def is_relevant_trade(text: str) -> bool:
    t = (text or "").lower()
    if any(b in t for b in BLOCK_NOISE):
        return False
    return any(a in t for a in ALLOW_STRICT)

def calc_policy_score(title: str, summary: str) -> int:
    t = f"{title} {summary}".lower()
    score = 1
    for kw, w in RISK_RULES:
        if kw in t:
            score += w
    return min(score, 20)

COUNTRY_KEYWORDS = {
    "USA": ["u.s.", "united states", "america", "section 301", "section 232", "us "],
    "India": ["india", "indian"],
    "Türkiye": ["turkey", "türkiye"],
    "Vietnam": ["vietnam"],
    "Netherlands": ["netherlands", "dutch"],
    "EU": ["european union", "eu commission", "european commission", "brussels"],
    "China": ["china", "prc", "beijing"],
    "Mexico": ["mexico"],
    "Brazil": ["brazil"],
    "Korea": ["korea", "korean", "seoul", "republic of korea"],
}

def detect_country(text: str) -> str:
    t = (text or "").lower()
    for country, keys in COUNTRY_KEYWORDS.items():
        if any(k in t for k in keys):
            return country
    return ""


# ===============================
# RSS FETCH
# ===============================
LANG_PARAMS = {
    "ko": {"hl": "ko", "gl": "KR", "ceid": "KR:ko"},
    "en": {"hl": "en", "gl": "US", "ceid": "US:en"},
    "fr": {"hl": "fr", "gl": "FR", "ceid": "FR:fr"},
}

def google_news_rss_url(q: str, lang: str) -> str:
    p = LANG_PARAMS.get(lang, LANG_PARAMS["en"]).copy()
    p["q"] = q
    return "https://news.google.com/rss/search?" + urlencode(p)

def clean_summary(title: str, raw_summary: str) -> str:
    s = re.sub(r"<[^>]+>", "", raw_summary or "").strip()
    s = re.sub(r"\s+", " ", s).strip()
    t = (title or "").strip()

    # 제목이 요약에 그대로 반복되는 경우 제거
    if t and s.lower().startswith(t.lower()):
        s = s[len(t):].strip(" -–—:|")

    # 그래도 동일/너무 짧으면 안전 문구
    if not s or (t and s.strip().lower() == t.strip().lower()):
        return "(요약 정보 부족 — 원문 링크 확인 필요)"
    return s

def normalize_text_for_hash(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"<[^>]+>", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def make_dedupe_key(title: str, summary: str, link: str) -> str:
    t = normalize_text_for_hash(title)
    s = normalize_text_for_hash(summary)
    l = (link or "").strip().lower()
    base = f"{t}|{s}|{l}"
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()

def infer_agency_from_link(link: str, domain_to_name: dict) -> str:
    if not link:
        return ""
    dom = urlparse(link).netloc.lower()
    if not dom:
        return ""
    # 정확 매칭
    if dom in domain_to_name:
        return domain_to_name[dom]
    # 서브도메인 매칭(예: www.customs.go.kr)
    for k, v in domain_to_name.items():
        if dom.endswith(k):
            return v
    return ""


def run_sensor_build_df(queries: list[str], domain_to_name: dict, allowed_domains: set) -> pd.DataFrame:
    now = now_kst()
    start, end = window_kst_24h(now)

    expanded = expand_queries_multilang(queries, NEWS_LANGS)

    rows = []
    seen = set()

    for item in expanded:
        kw = item["keyword"]
        q = item["q"]
        lang = item["lang"]

        rss = google_news_rss_url(q, lang)
        feed = feedparser.parse(rss)

        for e in feed.entries[:MAX_ITEMS_PER_QUERY]:
            title = getattr(e, "title", "").strip()
            link = getattr(e, "link", "").strip()
            published = getattr(e, "published", "") or getattr(e, "updated", "")
            raw_summary = getattr(e, "summary", "")

            if not title or not link:
                continue

            pub_dt = safe_parse_published(published)
            # 시간 필터: pub_dt 파싱 안 되면 제외(정확성 우선)
            if pub_dt is None:
                continue
            if not (start <= pub_dt <= end):
                continue

            # 관련성 필터(노이즈 차단)
            blob = f"{title} {raw_summary}"
            if not is_relevant_trade(blob):
                continue

            summary = clean_summary(title, raw_summary)

            country = detect_country(f"{title} {summary}")
            score = calc_policy_score(title, summary)
            agency = infer_agency_from_link(link, domain_to_name)

            # allowed_domains를 “공식/공인 우선”으로 쓰되, Google News는 일반 언론도 포함되므로
            # agency가 있으면 “공식/공인” 플래그를, 없으면 공란으로 둠
            memo = "공식/공인" if agency else ""

            key = make_dedupe_key(title, summary, link)
            if key in seen:
                continue
            seen.add(key)

            # 중요도(상/중/하) 간단 매핑: 점수 기반
            if score >= 14:
                importance = "상"
            elif score >= 8:
                importance = "중"
            else:
                importance = "하"

            rows.append({
                "제시어": kw,
                "헤드라인": title,
                "주요내용": summary[:600],
                "발표일": pub_dt.strftime("%Y-%m-%d %H:%M"),  # KST
                "대상 국가": country,
                "관련 기관": agency,
                "중요도": importance,
                "점수": score,
                "출처(URL)": link,
                "비고": memo,
                "언어": lang,
            })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    return df


# ===============================
# POST-PROCESS: 정렬/제시어별 10개 제한
# ===============================
IMPORTANCE_ORDER = {"상": 0, "중": 1, "하": 2}

def postprocess_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 필수 컬럼 보정
    for col in ["제시어","헤드라인","주요내용","발표일","대상 국가","관련 기관","중요도","점수","출처(URL)"]:
        if col not in df.columns:
            df[col] = ""

    df["중요도_정렬"] = df["중요도"].map(IMPORTANCE_ORDER).fillna(9).astype(int)

    # 제시어 + 중요도 오름차순 + 점수 내림차순(동일 중요도 내 우선순위)
    df = df.sort_values(["제시어", "중요도_정렬", "점수"], ascending=[True, True, False])

    # 제시어별 중복 제거 후 상위 10건
    kept = []
    for kw, g in df.groupby("제시어", dropna=False):
        # 중복 제거(제목+링크 기준)
        g = g.copy()
        g["dup_key"] = (g["헤드라인"].astype(str).str.lower().str.strip()
                        + "||" + g["출처(URL)"].astype(str).str.lower().str.strip())
        g = g.drop_duplicates("dup_key", keep="first")
        kept.append(g.head(MAX_PER_KEYWORD))

    out = pd.concat(kept, ignore_index=True) if kept else df
    out = out.drop(columns=["중요도_정렬"], errors="ignore")
    out = out.drop(columns=["dup_key"], errors="ignore")
    return out


# ===============================
# TOP3 선택(빈 경우 fallback)
# ===============================
def pick_top3(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    # “정책성 높은 것” 우선: 점수 내림차순
    cand = df.sort_values(["점수"], ascending=[False]).copy()
    top3 = cand.head(3)
    return top3


# ===============================
# HTML
# ===============================
STYLE = """
<style>
body{font-family:Malgun Gothic,Arial; background:#f6f6f6;}
.page{max-width:1120px;margin:auto;background:white;padding:14px;}
h2{margin-bottom:6px;}
.box{border:1px solid #ddd;border-radius:8px;padding:12px;margin:12px 0;}
ul{margin:6px 0 0 18px;}
li{margin-bottom:12px;}
table{border-collapse:collapse;width:100%;}
th,td{border:1px solid #ccc;padding:8px;font-size:12px;vertical-align:top;}
th{background:#f0f0f0;}
.small{font-size:11px;color:#555;}
.hr{margin:8px 0;border-top:1px solid #e5e5e5;}
</style>
"""

def esc(x) -> str:
    return html_lib.escape(str(x) if x is not None else "")

def build_keyword_count_line(df: pd.DataFrame) -> str:
    if df is None or df.empty or "제시어" not in df.columns:
        return ""
    counts = df["제시어"].value_counts()
    parts = [f"{k} {int(v)}건" for k, v in counts.items()]
    return ", ".join(parts)

def build_table_rows(df: pd.DataFrame, include_memo: bool) -> str:
    # 표 요구: 출처 컬럼 삭제, 헤드라인(링크)+요약을 한 칸에 표기
    rows = []
    for _, r in df.iterrows():
        headline = esc(r.get("헤드라인",""))
        link = r.get("출처(URL)","")
        summary = esc(r.get("주요내용",""))
        merged = f'<a href="{esc(link)}" target="_blank">{headline}</a><br/><div class="small">{summary}</div>'

        memo_cell = f"<td>{esc(r.get('비고',''))}</td>" if include_memo else ""

        rows.append(f"""
        <tr>
          <td>{esc(r.get("제시어",""))} ({esc(r.get("중요도",""))})</td>
          <td>{merged}</td>
          <td>{esc(r.get("발표일",""))}</td>
          <td>{esc(r.get("대상 국가",""))}</td>
          <td>{esc(r.get("관련 기관",""))}</td>
          {memo_cell}
        </tr>
        """)
    return "\n".join(rows)

def build_html_practitioner(df: pd.DataFrame, top3: pd.DataFrame) -> str:
    date = now_kst().strftime("%Y-%m-%d")
    count_line = build_keyword_count_line(df)

    # TOP3 리스트
    top3_html = ""
    for _, r in top3.iterrows():
        top3_html += f"""
        <li>
          <b>[{esc(r.get('제시어'))}｜{esc(r.get('대상 국가'))}｜{esc(r.get('중요도'))}｜점수 {esc(r.get('점수'))}]</b><br/>
          <a href="{esc(r.get('출처(URL)'))}" target="_blank">{esc(r.get('헤드라인'))}</a><br/>
          <div class="small">{esc(r.get('주요내용'))}</div>
        </li>
        """

    if not top3_html:
        top3_html = "<li>TOP3로 분류할 이벤트가 없었습니다. (원문 표 확인)</li>"

    # ②/③은 TOP3 기반으로 항상 채우기(빈 경우 fallback 문구)
    why_html = ""
    chk_html = ""
    if top3 is not None and not top3.empty:
        for _, r in top3.iterrows():
            kw = esc(r.get("제시어"))
            country = esc(r.get("대상 국가"))
            why_html += f"<li><b>{kw}</b> ({country}) : 관세/통상 정책 변화 가능성 → 원가·마진·리드타임·공급망 영향</li>"
            chk_html += f"<li><b>{kw}</b> ({country}) : ① 대상국/품목(HS) 확인 → ② 적용시점/대상범위 확인 → ③ 법인 영향 1차 산정 → ④ 필요 시 HQ 대응 트리거</li>"
    else:
        why_html = "<li>표에 포함된 이슈 중 정책성 높은 건이 부족하여, 표 기반으로 확인이 필요합니다.</li>"
        chk_html = "<li>당일 정책성 이벤트가 확인되면 HS/대상국/시행시점 기준으로 영향산정 후 대응 착수 권장</li>"

    # 표(실무자): “비고” 삭제 요구 반영 -> include_memo=False
    rows = build_table_rows(df, include_memo=False)

    return f"""
    <html>
    <head>{STYLE}</head>
    <body>
    <div class="page">
      <h2>관세·무역 뉴스 브리핑 (실무) ({date})</h2>

      <div class="box">
        <h3>① 오늘의 핵심 정책 이벤트 TOP3</h3>
        <ul>{top3_html}</ul>
      </div>

      <div class="box">
        <h3>② 왜 중요한가 (TOP3 기반)</h3>
        <ul>{why_html}</ul>
      </div>

      <div class="box">
        <h3>③ 당사 관점 체크포인트 (TOP3 기반)</h3>
        <ul>{chk_html}</ul>
      </div>

      <div class="box">
        <h3>④ 정책 이벤트 표</h3>
        <div class="small">{esc(count_line)}</div>
        <div class="hr"></div>
        <table>
          <tr>
            <th>제시어(중요도)</th>
            <th>헤드라인 / 요약 (링크 포함)</th>
            <th>발표일(KST)</th>
            <th>대상 국가</th>
            <th>관련 기관</th>
          </tr>
          {rows}
        </table>
      </div>

      <div class="small">* 본 메일은 Google News RSS 기반 자동 수집이며, 발표일은 RSS 게시 시각(KST) 기준입니다.</div>
    </div>
    </body>
    </html>
    """

def build_html_exec(df: pd.DataFrame, top3: pd.DataFrame) -> str:
    date = now_kst().strftime("%Y-%m-%d")

    items = ""
    for _, r in top3.iterrows():
        items += f"""
        <li>
          <b>[{esc(r.get('대상 국가'))}｜{esc(r.get('제시어'))}｜{esc(r.get('중요도'))}｜점수 {esc(r.get('점수'))}]</b><br/>
          <a href="{esc(r.get('출처(URL)'))}" target="_blank">{esc(r.get('헤드라인'))}</a><br/>
          <div class="small">{esc(r.get('주요내용'))}</div>
        </li>
        """

    if not items:
        items = "<li>금일 TOP3로 분류할 정책성 이슈가 제한적입니다. (실무 표 참조)</li>"

    # ②/③ 항상 채우기
    why = ""
    chk = ""
    if top3 is not None and not top3.empty:
        for _, r in top3.iterrows():
            kw = esc(r.get("제시어"))
            country = esc(r.get("대상 국가"))
            why += f"<li><b>{kw}</b> ({country}) : 규제/관세·통상 변경 시 가격·원가·수급/리드타임에 직접 영향 가능</li>"
            chk += f"<li><b>{kw}</b> ({country}) : (1) 대상 HS/품목군 확인 → (2) 적용시점·면제/예외 확인 → (3) 생산/판매법인 영향 1차 산정 → (4) 필요 시 대응 Taskforce 착수</li>"
    else:
        why = "<li>정책성 이벤트가 부족하여, 표 기반 확인이 필요합니다.</li>"
        chk = "<li>정책성 이벤트 확인 시 HS/대상국/적용시점 기준으로 HQ 대응 트리거 권장</li>"

    return f"""
    <html><head>{STYLE}</head>
    <body>
      <div class="page">
        <h2>[Executive] 관세·통상 핵심 TOP3 ({date})</h2>

        <div class="box">
          <h3>① TOP3 (요약 포함)</h3>
          <ul>{items}</ul>
        </div>

        <div class="box">
          <h3>② 왜 중요한가 (TOP3 기반)</h3>
          <ul>{why}</ul>
        </div>

        <div class="box">
          <h3>③ 당사 관점 체크포인트 (TOP3 기반)</h3>
          <ul>{chk}</ul>
        </div>

        <div class="box">
          <b>Action</b><br/>
          1) 대상국/품목(HS) 확인 → 2) 법인 영향(원가/마진/리드타임) 1차 산정 → 3) 필요 시 HQ 대응 착수
        </div>
      </div>
    </body></html>
    """


# ===============================
# OUTPUTS
# ===============================
def write_outputs(df: pd.DataFrame, html_prac: str):
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
        f.write(html_prac)

    return csv_path, xlsx_path, html_path


# ===============================
# MAIL
# ===============================
def send_mail_to(recipients: list[str], subject: str, html_body: str):
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
    # 0) 로더 (타이트)
    queries = load_custom_queries(QUERIES_FILE)
    domain_to_name, allowed_domains = load_sites_xlsx(SITES_FILE)

    # 1) 센서 실행
    df = run_sensor_build_df(queries, domain_to_name, allowed_domains)

    if df is None or df.empty:
        print("오늘 수집된 이벤트/뉴스 없음 (필터 통과 0)")
        return

    # 2) 후처리(정렬/제시어별 10개/중복제거)
    df = postprocess_df(df)

    # 3) TOP3
    top3 = pick_top3(df)

    # 4) HTML 생성
    html_prac = build_html_practitioner(df, top3)
    html_exec = build_html_exec(df, top3)

    # 5) 파일 저장
    write_outputs(df, html_prac)

    # 6) 메일 발송
    today = now_kst().strftime("%Y-%m-%d")
    send_mail_to(RECIPIENTS, f"관세·무역 뉴스 브리핑 ({today})", html_prac)
    send_mail_to(RECIPIENTS_EXEC, f"[Executive] 관세·통상 핵심 TOP3 ({today})", html_exec)

    print("✅ vCurrent STABLE 완료")
    print("BASE_DIR =", BASE_DIR)
    print("OUT_FILES =", os.listdir(BASE_DIR))


if __name__ == "__main__":
    main()
