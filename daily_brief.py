# -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief
vCurrent STABLE (GitHub Actions friendly)

목표(안정 운영):
- custom_queries.TXT(제시어) + sites.xlsx(정부/공인 사이트 목록) 기반 수집
- Google News RSS 센서(키워드/다국어 확장) → 중복 제거 → 스코어링
- out/에 CSV/XLSX/HTML 저장 + (실무자/임원) 2개 메일 발송
- Gemini API(선택): TOP3 요약 품질 보강(키 없으면 규칙 기반 fallback)

필수 환경변수(Secrets 권장):
- SMTP_SERVER, SMTP_PORT, SMTP_EMAIL, SMTP_PASSWORD
- RECIPIENTS (실무자)
- RECIPIENTS_EXEC (임원)

선택 환경변수:
- GEMINI_API_KEY (있으면 TOP3/표 요약을 한글로 강제)
- NEWS_QUERY_LANGS (예: "ko,en,fr" 기본 ko,en)
- MAX_PER_KEYWORD (기본 10)

파일:
- custom_queries.TXT : 제시어(한 줄 1개)
- sites.xlsx : 시트명 'SiteList' (또는 자동 감지) / 컬럼: name, url

시간:
- GitHub Actions cron은 workflow(yml)에서 제어 (예: KST 08:00 발송)
- 본 스크립트는 '전날 07:00 ~ 오늘 07:00' 범위로 RSS 검색 쿼리에 시간 힌트를 포함
  (Google News RSS가 시간 필터를 완전 보장하진 않으나, 운영 안정성 우선)
"""

from __future__ import annotations

import os
import re
import html
import smtplib
import hashlib
import datetime as dt
from typing import Dict, List, Tuple, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import pandas as pd
import feedparser
import urllib.parse

# ===============================
# CONFIG / ENV
# ===============================
SMTP_SERVER   = os.getenv("SMTP_SERVER")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_EMAIL    = os.getenv("SMTP_EMAIL")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

RECIPIENTS = [x.strip() for x in os.getenv("RECIPIENTS", "").split(",") if x.strip()]
RECIPIENTS_EXEC = [x.strip() for x in os.getenv("RECIPIENTS_EXEC", "").split(",") if x.strip()]

BASE_DIR = os.getenv("BASE_DIR", os.path.join(os.path.dirname(__file__), "out"))
os.makedirs(BASE_DIR, exist_ok=True)

CUSTOM_QUERIES_FILE = os.getenv("CUSTOM_QUERIES_FILE", os.path.join(os.path.dirname(__file__), "custom_queries.TXT"))
SITES_FILE = os.getenv("SITES_FILE", os.path.join(os.path.dirname(__file__), "sites.xlsx"))

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
LANGS = [x.strip().lower() for x in os.getenv("NEWS_QUERY_LANGS", "ko,en").split(",") if x.strip()]
MAX_PER_KEYWORD = int(os.getenv("MAX_PER_KEYWORD", "10"))

# ===============================
# TIME
# ===============================
KST = dt.timezone(dt.timedelta(hours=9))

def now_kst() -> dt.datetime:
    return dt.datetime.now(tz=KST)

# 검색 시간창: 전날 07:00 ~ 오늘 07:00 (KST)

def window_kst() -> Tuple[dt.datetime, dt.datetime]:
    now = now_kst()
    end = now.replace(hour=7, minute=0, second=0, microsecond=0)
    if now < end:
        # 새벽/이른 아침이면 end는 오늘 07:00, start는 전날 07:00
        pass
    else:
        # 07:00 이후이면 end는 오늘 07:00(이미 지났음)로 고정
        end = end
    start = end - dt.timedelta(days=1)
    return start, end

# ===============================
# KEYWORDS / RULES
# ===============================
# 정책/관세 관련 키워드(최소 요건)
TRADE_TERMS = [
    "관세", "관세율", "추가관세", "세율", "hs", "hs code", "tariff", "customs duty", "duty rate",
    "tariff act", "trade expansion act", "international emergency economic powers act", "ieepa",
    "section 301", "section 232", "anti-dumping", "countervailing", "safeguard",
    "export control", "sanction", "entity list", "origin", "rules of origin", "원산지", "fta", "통관", "customs",
]

# 제외(명백히 무관한 이슈)
BLOCK_TERMS = [
    "wine", "와인", "baseball", "soccer", "k-pop", "concert", "celebrity",
    "시위", "protest", "폭동", "riot", "체포", "arrest", "murder", "earthquake",
]

# 당사/업(삼성전자) 연관성 힌트
COMPANY_TERMS = [
    "samsung", "삼성", "galaxy", "smartphone", "mobile", "tablet", "watch", "earbuds",
    "tv", "monitor", "refrigerator", "air conditioner", "oven", "vacuum", "home appliance",
    "5g", "base station", "network equipment", "antenna", "x-ray", "medical device",
    "apple", "lg", "whirlpool", "ge", "general electric",
]

PRODUCTION_COUNTRIES = [
    "korea", "republic of korea", "south korea", "china", "vietnam", "india", "indonesia",
    "turkey", "türkiye", "slovakia", "poland", "mexico", "brazil",
    "한국", "중국", "베트남", "인도", "인도네시아", "터키", "슬로바키아", "폴란드", "멕시코", "브라질",
]

COUNTRY_KEYWORDS: Dict[str, List[str]] = {
    "USA": ["u.s.", "united states", "america", "section 301", "section 232", "u.s. trade", "cbp"],
    "EU": ["european union", "eu commission", "european commission", "eu"],
    "China": ["china", "prc"],
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

# 스코어 규칙
RISK_RULES: List[Tuple[str, int]] = [
    ("section 301", 7),
    ("section 232", 7),
    ("ieepa", 7),
    ("international emergency economic powers act", 7),
    ("tariff act", 6),
    ("trade expansion act", 6),
    ("export control", 6),
    ("sanction", 6),
    ("entity list", 6),
    ("anti-dumping", 5),
    ("countervailing", 5),
    ("safeguard", 5),
    ("관세율", 5),
    ("추가관세", 5),
    ("tariff", 4),
    ("customs duty", 4),
    ("duty rate", 4),
    ("관세", 4),
    ("hs code", 3),
    ("hs", 3),
    ("원산지", 3),
    ("rules of origin", 3),
    ("fta", 3),
    ("통관", 3),
    ("customs", 3),
    ("개정", 2),
    ("amend", 2),
    ("시행", 2),
    ("effective", 2),
]

# ===============================
# UTIL
# ===============================

def _clean_text(s: str) -> str:
    s = s or ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _normalize_url(u) -> str:
    # pandas에서 NaN/float 들어오는 케이스 방어
    if u is None:
        return ""
    try:
        if isinstance(u, float):
            return ""
        u = str(u)
    except Exception:
        return ""
    u = (u or "").strip()
    if not u:
        return ""
    # scheme 없으면 https 가정
    if not re.match(r"^https?://", u, flags=re.I):
        u = "https://" + u
    # 트래킹 파라미터 최소 제거
    u = re.sub(r"(#.*)$", "", u)
    return u


def _domain(url: str) -> str:
    try:
        m = re.search(r"https?://([^/]+)/?", url)
        return (m.group(1) if m else "").lower()
    except Exception:
        return ""


def detect_country(text: str) -> str:
    t = (text or "").lower()
    for c, keys in COUNTRY_KEYWORDS.items():
        if any(k in t for k in keys):
            return c
    return ""


def calc_policy_score(title: str, snippet: str, url: str, allowed_domains: set) -> int:
    blob = f"{title} {snippet}".lower()
    score = 1
    for kw, w in RISK_RULES:
        if kw in blob:
            score += w
    # 정부/공인 사이트 우대
    if _domain(url) in allowed_domains:
        score += 4
    # 당사/제품/생산지 힌트 우대
    if any(k in blob for k in COMPANY_TERMS):
        score += 2
    if any(k in blob for k in PRODUCTION_COUNTRIES):
        score += 1
    return max(1, min(score, 30))


def is_trade_related(title: str, snippet: str) -> bool:
    blob = f"{title} {snippet}".lower()
    if any(b in blob for b in BLOCK_TERMS):
        # 단, 관세/통상 용어가 강하게 있으면 예외적으로 통과
        if not any(t in blob for t in ["tariff", "customs", "관세", "section 301", "section 232", "ieepa"]):
            return False
    return any(t in blob for t in TRADE_TERMS)


def company_relevance_score(title: str, snippet: str, url: str, allowed_domains: set) -> int:
    blob = f"{title} {snippet}".lower()
    s = 0
    if _domain(url) in allowed_domains:
        s += 4
    if any(k in blob for k in COMPANY_TERMS):
        s += 3
    if any(k in blob for k in PRODUCTION_COUNTRIES):
        s += 2
    # 통상 키워드 강도
    s += sum(1 for t in ["관세", "tariff", "section 301", "section 232", "ieepa", "export control", "sanction"] if t in blob)
    return s


def _fingerprint(title: str, url: str) -> str:
    # title + domain 기준 (구글뉴스 리다이렉트 URL이 달라지는 문제 완화)
    d = _domain(url)
    key = (re.sub(r"\s+", " ", (title or "").strip().lower()) + "|" + d).encode("utf-8", errors="ignore")
    return hashlib.sha1(key).hexdigest()[:16]


# ===============================
# LOADERS (tight)
# ===============================

def load_custom_queries(path: str) -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"custom_queries file not found: {path}")
    out: List[str] = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            if s.startswith("#"):
                continue
            out.append(s)
    # 중복 제거(순서 유지)
    seen = set()
    uniq = []
    for q in out:
        k = q.lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(q)
    return uniq


def load_sites_xlsx(path: str) -> Tuple[Dict[str, str], set]:
    """sites.xlsx 로딩(단단하게):
    - 시트명 우선순위: SiteList, sites, Sheet1, 첫번째 시트
    - 필수 컬럼: name, url (대소문자/공백/한글 변형 약간 허용)
    반환:
      domain_to_name: {domain: name}
      allowed_domains: set(domains)
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"sites.xlsx not found: {path}")

    xls = pd.ExcelFile(path)
    sheet_candidates = []
    for s in xls.sheet_names:
        sheet_candidates.append(s)
    # 우선순위 정렬
    preferred = ["SiteList", "sites", "SITELIST", "Sheet1"]
    ordered = []
    for p in preferred:
        if p in sheet_candidates:
            ordered.append(p)
    for s in sheet_candidates:
        if s not in ordered:
            ordered.append(s)

    last_err = None
    for sheet in ordered:
        try:
            df = pd.read_excel(path, sheet_name=sheet)
            if df is None or df.empty:
                continue
            # 컬럼명 정규화
            cols = {str(c).strip().lower(): c for c in df.columns}
            # 허용 변형
            name_col = None
            url_col = None
            for k in ["name", "기관명", "site", "사이트", "기관", "source"]:
                if k in cols:
                    name_col = cols[k]
                    break
            for k in ["url", "링크", "주소", "link"]:
                if k in cols:
                    url_col = cols[k]
                    break
            if not name_col or not url_col:
                continue

            df = df[[name_col, url_col]].copy()
            df.columns = ["name", "url"]
            df["name"] = df["name"].astype(str).fillna("").apply(lambda x: str(x).strip())
            df["url"] = df["url"].apply(_normalize_url)
            df = df[df["url"].astype(bool)]

            domain_to_name: Dict[str, str] = {}
            for _, r in df.iterrows():
                d = _domain(r["url"])
                if not d:
                    continue
                # 같은 도메인 중복이면 첫 값 유지
                domain_to_name.setdefault(d, r["name"])
            allowed = set(domain_to_name.keys())
            return domain_to_name, allowed
        except Exception as e:
            last_err = e

    raise ValueError(f"sites.xlsx parse failed. sheets={xls.sheet_names}. last_error={last_err}")


# ===============================
# MULTI-LANG QUERY EXPANSION
# ===============================

def expand_query(base_kw: str, langs: List[str]) -> List[str]:
    """제시어를 언어별로 확장.
    - ko: 원문
    - en/fr: 제시어가 한글이면 보수적으로 'tariff/customs/FTA' 계열을 OR로 섞어 검색폭 확대
    운영 안정성 우선: 완벽 번역 대신 안전한 범용 키워드로 확장
    """
    kw = base_kw.strip()
    out = [kw]

    # 공통으로 붙일 통상 키워드(언어별)
    en_extra = ["tariff", "customs", "customs duty", "duty rate", "section 301", "section 232", "IEEPA", "export control", "sanctions", "rules of origin", "FTA"]
    fr_extra = ["tarif douanier", "douane", "droit de douane", "sanctions", "contr\u00f4le des exportations", "accord de libre-\u00e9change", "origine"]

    if "en" in langs:
        if re.search(r"[\u3131-\uD79D]", kw):
            out.append("(" + kw + " OR " + " OR ".join(en_extra) + ")")
        else:
            out.append("(" + kw + " OR tariff OR customs)")

    if "fr" in langs:
        if re.search(r"[\u3131-\uD79D]", kw):
            out.append("(" + kw + " OR " + " OR ".join(fr_extra) + ")")
        else:
            out.append("(" + kw + " OR tarif douanier OR douane)")

    # 중복 제거
    seen = set()
    uniq = []
    for q in out:
        k = q.strip().lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(q)
    return uniq


# ===============================
# GEMINI (optional)
# ===============================

def gemini_available() -> bool:
    return bool(GEMINI_API_KEY)


def gemini_summarize_ko(title: str, snippet: str, url: str, max_lines: int = 3) -> Optional[str]:
    """Gemini로 한국어 요약(가능하면 2~3줄). 키 없으면 None."""
    if not gemini_available():
        return None

    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-1.5-flash")
        prompt = (
            "당신은 삼성전자 관세/통상 담당자용 뉴스 요약 봇입니다.\n"
            "아래 뉴스의 제목/스니펫/URL을 참고해, 한국어로 핵심만 2~3줄로 요약하세요.\n"
            "정책/관세/무역 이슈 관점에서 요약하고, 불확실하면 스니펫 기반으로만 요약하세요.\n\n"
            f"[제목]\n{title}\n\n"
            f"[스니펫]\n{snippet}\n\n"
            f"[URL]\n{url}\n"
        )
        resp = model.generate_content(prompt)
        txt = getattr(resp, "text", "") or ""
        txt = _clean_text(txt)
        if not txt:
            return None
        # 줄 수 제한
        lines = [l.strip() for l in re.split(r"\n+", txt) if l.strip()]
        return "<br/>".join(lines[:max_lines])
    except Exception:
        return None


# ===============================
# SENSOR
# ===============================

def google_news_rss(query: str) -> str:
    # 시간 힌트 포함: Google News는 'when:' 연산자 지원이 제한적이지만, 쿼리 힌트로 사용
    start, end = window_kst()
    # 힌트 문자열(영문) - 정확 필터 아님
    hint = f" after:{start.date().isoformat()} before:{end.date().isoformat()}"

    q = (query + hint).strip()
    return "https://news.google.com/rss/search?" + urllib.parse.urlencode({
        "q": q,
        "hl": "ko",
        "gl": "KR",
        "ceid": "KR:ko",
    })


def fetch_entries(query: str) -> List[dict]:
    rss = google_news_rss(query)
    feed = feedparser.parse(rss)
    return list(getattr(feed, "entries", []) or [])


def build_df(keywords: List[str], domain_to_name: Dict[str, str], allowed_domains: set) -> pd.DataFrame:
    rows = []
    for kw in keywords:
        for q in expand_query(kw, LANGS):
            entries = fetch_entries(q)
            for e in entries[:50]:
                title = _clean_text(getattr(e, "title", ""))
                link = _normalize_url(getattr(e, "link", ""))
                published = _clean_text(getattr(e, "published", ""))

                # snippet 후보들
                snippet = _clean_text(getattr(e, "summary", ""))
                if not snippet and hasattr(e, "description"):
                    snippet = _clean_text(getattr(e, "description", ""))
                # content 필드
                if (not snippet) and getattr(e, "content", None):
                    try:
                        snippet = _clean_text(e.content[0].value)
                    except Exception:
                        pass

                if not title or not link:
                    continue
                if not is_trade_related(title, snippet):
                    continue

                country = detect_country(f"{title} {snippet}")
                score = calc_policy_score(title, snippet, link, allowed_domains)
                rel = company_relevance_score(title, snippet, link, allowed_domains)
                src_domain = _domain(link)
                src_name = domain_to_name.get(src_domain, "")

                rows.append({
                    "제시어": kw,
                    "헤드라인": title,
                    "주요내용": snippet,
                    "발표일": published,
                    "출처(URL)": link,
                    "대상 국가": country,
                    "관련 기관": src_name,
                    "점수": score,
                    "연관성": rel,
                    "_fp": _fingerprint(title, link),
                })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # 1차 중복 제거(fp)
    df = df.drop_duplicates(subset=["_fp"], keep="first").reset_index(drop=True)

    # 2차: 제목 유사 중복(간단)
    df["_tkey"] = df["헤드라인"].str.lower().str.replace(r"\s+", " ", regex=True).str.strip()
    df = df.drop_duplicates(subset=["제시어", "_tkey"], keep="first").reset_index(drop=True)

    return df


# ===============================
# RANKING / SELECTION
# ===============================

def importance_label(score: int) -> str:
    # 상/중/하
    if score >= 18:
        return "상"
    if score >= 11:
        return "중"
    return "하"


def pick_top3(df: pd.DataFrame, allowed_domains: set) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    # TOP3는 "정책/관세" 강도 + 당사 연관성 필터를 더 강하게
    tmp = df.copy()
    tmp["중요도"] = tmp["점수"].apply(importance_label)

    def ok_row(r) -> bool:
        title = str(r.get("헤드라인", ""))
        snip = str(r.get("주요내용", ""))
        url = str(r.get("출처(URL)", ""))
        # 무관(와인 등) 강제 제외
        if not is_trade_related(title, snip):
            return False
        # 당사 연관성 기준: allowed domain OR 연관성 점수 >= 4
        return (_domain(url) in allowed_domains) or (int(r.get("연관성", 0)) >= 4)

    tmp = tmp[tmp.apply(ok_row, axis=1)].copy()
    if tmp.empty:
        # 너무 빡세면 완화
        tmp = df.copy()
        tmp["중요도"] = tmp["점수"].apply(importance_label)

    # 정렬: 점수 desc, 연관성 desc
    tmp = tmp.sort_values(["점수", "연관성"], ascending=[False, False]).head(3).reset_index(drop=True)
    return tmp


# ===============================
# OUTPUTS
# ===============================

def write_outputs(df: pd.DataFrame, html_prac: str, html_exec: str) -> Tuple[str, str, str, str]:
    today = now_kst().strftime("%Y-%m-%d")
    csv_path  = os.path.join(BASE_DIR, f"policy_events_{today}.csv")
    xlsx_path = os.path.join(BASE_DIR, f"policy_events_{today}.xlsx")
    html_path = os.path.join(BASE_DIR, f"policy_events_{today}.html")
    html_exec_path = os.path.join(BASE_DIR, f"policy_events_exec_{today}.html")

    # CSV/XLSX는 원본 보존(출처 URL 포함)
    try:
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    except TypeError:
        df.to_csv(csv_path, index=False)

    df.to_excel(xlsx_path, index=False)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_prac)
    with open(html_exec_path, "w", encoding="utf-8") as f:
        f.write(html_exec)

    return csv_path, xlsx_path, html_path, html_exec_path


# ===============================
# MAIL
# ===============================

def send_mail_to(recipients: List[str], subject: str, html_body: str) -> None:
    if not recipients:
        return
    if not (SMTP_SERVER and SMTP_EMAIL and SMTP_PASSWORD):
        raise RuntimeError("SMTP env missing. Check SMTP_SERVER/SMTP_EMAIL/SMTP_PASSWORD")

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
# HTML
# ===============================

STYLE = """
<style>
@page { size: A4 landscape; margin: 10mm; }
body{font-family:Malgun Gothic,Arial; background:#f6f6f6;}
.page{width:277mm; margin:0 auto; background:white; padding:10mm; box-sizing:border-box;}
h2{margin:0 0 6px 0;}
.box{border:1px solid #ddd;border-radius:10px;padding:10px;margin:10px 0;}
li{margin-bottom:12px;}
.small{font-size:11px;color:#555;}
.tablewrap{overflow-x:hidden;}
table{border-collapse:collapse;width:100%; table-layout:fixed;}
th,td{border:1px solid #ccc;padding:6px;font-size:11.5px;vertical-align:top;}
th{background:#f0f0f0;}
.col-k{width:10%;}
.col-i{width:6%;}
.col-c{width:10%;}
.col-d{width:10%;}
.col-t{width:64%;}
</style>
"""


def _fallback_snippet_ko(title: str, snippet: str) -> str:
    sn = _clean_text(snippet)
    # 제목=스니펫 같으면 스니펫을 더 잘라서 2~3줄 흉내
    if not sn or sn.lower() == _clean_text(title).lower():
        if sn:
            return "<br/>".join([sn[:90], sn[90:180], sn[180:270]]).strip("<br/>")
        return "(요약 미확보: 원문 링크를 확인하세요)"
    # 문장 단위로 2~3줄
    parts = re.split(r"(?<=[\.!?。])\s+|\s*\n+\s*", sn)
    parts = [p.strip() for p in parts if p.strip()]
    if len(parts) >= 3:
        return "<br/>".join(parts[:3])
    return sn[:270]


def build_top3_blocks(df_top3: pd.DataFrame, allowed_domains: set) -> Tuple[str, str, str]:
    """TOP3 HTML(li), WHY(통합), CHECK(통합)"""
    items_html = ""
    why_lines = []
    check_lines = []

    for _, r in df_top3.iterrows():
        kw = str(r.get("제시어", ""))
        country = str(r.get("대상 국가", ""))
        score = int(r.get("점수", 0))
        imp = importance_label(score)
        title = str(r.get("헤드라인", ""))
        url = str(r.get("출처(URL)", ""))
        snippet = str(r.get("주요내용", ""))

        # 요약(한글) 강제
        summ = gemini_summarize_ko(title, snippet, url, max_lines=3)
        if not summ:
            summ = _fallback_snippet_ko(title, snippet)

        items_html += f"""
        <li>
          <b>[{kw}｜{country}｜{imp}｜점수 {score}]</b><br/>
          <a href=\"{html.escape(url)}\" target=\"_blank\">{html.escape(title)}</a><br/>
          <div class=\"small\">{summ}</div>
        </li>
        """

        # WHY (중복 최소화)
        why = f"[{kw}|{country}|{imp}] 관세/통상 정책 변화 가능성 → 수입원가·판매가·마진 및 리드타임 영향(생산/판매법인 검토 필요)"
        why_lines.append(why)

        # CHECK (당사 템플릿 강화)
        prod_hint = "모바일/가전/네트워크/의료"  # 기본
        if country in ["USA", "EU"]:
            prod_hint = "미국/유럽향 판매 + 글로벌 생산법인(베트남/인도/멕시코/폴란드/슬로바키아 등)" \
                        "의 원산지·HS·추가관세 영향"
        elif country in ["India", "Vietnam", "Mexico", "Brazil", "China"]:
            prod_hint = f"{country} 생산/판매법인" \
                        " 기준 수입부품 과세/통관/원산지 영향"

        chk = (
            f"[{kw}|{country}|{imp}] 우선 확인: 1) 대상국/대상품목(HS)·적용시점 2) "
            f"당사 생산/판매 매핑({prod_hint}) 3) 특혜(FTA) 적용/원산지 리스크 4) "
            "필요 시 HQ 대응(가격/소싱/선적/CO)"
        )
        check_lines.append(chk)

    # 중복 제거(문장 단위)
    def dedup(lines: List[str]) -> List[str]:
        seen = set()
        out = []
        for l in lines:
            k = re.sub(r"\s+", " ", l).strip()
            if k in seen:
                continue
            seen.add(k)
            out.append(l)
        return out

    why_html = "".join(f"<li>{html.escape(l)}</li>" for l in dedup(why_lines))
    check_html = "".join(f"<li>{html.escape(l)}</li>" for l in dedup(check_lines))

    return items_html, why_html, check_html


def build_table(df: pd.DataFrame) -> Tuple[str, str]:
    """실무자용 표: 제시어별 top N + 건수라인"""
    if df is None or df.empty:
        return "", ""

    tmp = df.copy()
    tmp["중요도"] = tmp["점수"].apply(importance_label)
    imp_rank = {"상": 1, "중": 2, "하": 3}
    tmp["_ir"] = tmp["중요도"].map(imp_rank).fillna(9).astype(int)

    # 제시어별 top N 제한(점수/연관성 기준)
    out_parts = []
    counts = tmp.groupby("제시어").size().to_dict()
    count_line = ", ".join([f"{k} {v}건" for k, v in sorted(counts.items(), key=lambda x: x[0])])

    for kw in sorted(tmp["제시어"].unique()):
        sub = tmp[tmp["제시어"] == kw].copy()
        sub = sub.sort_values(["_ir", "점수", "연관성"], ascending=[True, False, False]).head(MAX_PER_KEYWORD)

        # 행 구성
        rows = ""
        for _, r in sub.iterrows():
            title = str(r.get("헤드라인", ""))
            url = str(r.get("출처(URL)", ""))
            snippet = str(r.get("주요내용", ""))

            summ = gemini_summarize_ko(title, snippet, url, max_lines=3)
            if not summ:
                summ = _fallback_snippet_ko(title, snippet)

            cell = (
                f"<a href=\"{html.escape(url)}\" target=\"_blank\">{html.escape(title)}</a>"
                f"<br/><span class=\"small\">{summ}</span>"
            )

            rows += (
                "<tr>"
                f"<td>{html.escape(str(r.get('제시어','')))}</td>"
                f"<td>{html.escape(str(r.get('중요도','')))}</td>"
                f"<td>{html.escape(str(r.get('대상 국가','')))}</td>"
                f"<td>{html.escape(str(r.get('발표일','')))}</td>"
                f"<td>{cell}</td>"
                "</tr>"
            )

        table = f"""
        <div class="box">
          <div><b>④ 정책 이벤트 표</b> <span class="small">(제시어: {html.escape(kw)} / 표기: 중복제거 후 최대 {MAX_PER_KEYWORD}건)</span></div>
          <div class="small" style="margin:6px 0 8px 0;">제시어별 주요뉴스 건수: {html.escape(count_line)}</div>
          <div class="tablewrap">
            <table>
              <colgroup>
                <col class="col-k"/><col class="col-i"/><col class="col-c"/><col class="col-d"/><col class="col-t"/>
              </colgroup>
              <tr>
                <th>제시어</th>
                <th>중요도</th>
                <th>국가</th>
                <th>발표일</th>
                <th>헤드라인 / 요약</th>
              </tr>
              {rows}
            </table>
          </div>
        </div>
        """
        out_parts.append(table)

    return count_line, "\n".join(out_parts)


def build_html_practitioner(df: pd.DataFrame, top3: pd.DataFrame, allowed_domains: set) -> str:
    date = now_kst().strftime("%Y-%m-%d")

    items_html, why_html, check_html = build_top3_blocks(top3, allowed_domains)
    count_line, tables_html = build_table(df)

    return f"""
    <html>
    <head>{STYLE}</head>
    <body>
      <div class="page">
        <h2>관세·무역 뉴스 브리핑 ({date})</h2>

        <div class="box">
          <h3 style="margin:0 0 6px 0;">① 오늘의 핵심 정책 이벤트 TOP3</h3>
          <ul style="margin:0; padding-left:18px;">{items_html}</ul>
        </div>

        <div class="box">
          <h3 style="margin:0 0 6px 0;">② 왜 중요한가 (TOP3 기반)</h3>
          <ul style="margin:0; padding-left:18px;">{why_html}</ul>
        </div>

        <div class="box">
          <h3 style="margin:0 0 6px 0;">③ 당사 관점 체크포인트 (TOP3 기반)</h3>
          <ul style="margin:0; padding-left:18px;">{check_html}</ul>
        </div>

        {tables_html}

        <div class="small">* 요약: Gemini API 키가 있으면 한국어 요약을 우선 적용합니다. 키가 없으면 RSS 스니펫 기반 요약(최대 3줄)로 대체됩니다.</div>
      </div>
    </body>
    </html>
    """


def build_html_executive(top3: pd.DataFrame, allowed_domains: set) -> str:
    date = now_kst().strftime("%Y-%m-%d")
    items_html, why_html, check_html = build_top3_blocks(top3, allowed_domains)

    return f"""
    <html>
    <head>{STYLE}</head>
    <body>
      <div class="page">
        <h2>[Executive] 관세·통상 핵심 TOP3 ({date})</h2>

        <div class="box">
          <h3 style="margin:0 0 6px 0;">① 관세·통상 핵심 TOP3</h3>
          <ul style="margin:0; padding-left:18px;">{items_html}</ul>
        </div>

        <div class="box">
          <h3 style="margin:0 0 6px 0;">② 왜 중요한가 (TOP3 기반)</h3>
          <ul style="margin:0; padding-left:18px;">{why_html}</ul>
        </div>

        <div class="box">
          <h3 style="margin:0 0 6px 0;">③ 당사 관점 체크포인트 (TOP3 기반)</h3>
          <ul style="margin:0; padding-left:18px;">{check_html}</ul>
        </div>

        <div class="box">
          <b>Action (HQ 트리거)</b><br/>
          1) 대상국/품목(HS)·적용시점 확인 → 2) 생산/판매법인 영향(원가/마진/리드타임) 1차 산정 →
          3) 필요 시 가격/소싱/선적/FTA CO 대응 착수
        </div>

        <div class="small">* TOP3는 통상 키워드 + 당사 연관성(정부/공인 도메인 또는 제품/생산지 힌트) 기준으로 선별됩니다.</div>
      </div>
    </body>
    </html>
    """


# ===============================
# MAIN
# ===============================

def main() -> None:
    print("BASE_DIR =", BASE_DIR)
    print("CUSTOM_QUERIES_FILE =", CUSTOM_QUERIES_FILE)
    print("SITES_FILE =", SITES_FILE)
    print("GEMINI_ENABLED =", bool(GEMINI_API_KEY))

    # 0) 로더
    keywords = load_custom_queries(CUSTOM_QUERIES_FILE)
    domain_to_name, allowed_domains = load_sites_xlsx(SITES_FILE)

    # 1) 수집
    df = build_df(keywords, domain_to_name, allowed_domains)
    if df is None or df.empty:
        print("오늘 수집된 이벤트/뉴스 없음")
        return

    # 2) 중요도/정렬 보정
    df["중요도"] = df["점수"].apply(importance_label)

    # 3) TOP3
    top3 = pick_top3(df, allowed_domains)

    # 4) HTML 생성
    html_prac = build_html_practitioner(df, top3, allowed_domains)
    html_exec = build_html_executive(top3, allowed_domains)

    # 5) 저장
    write_outputs(df, html_prac, html_exec)

    # 6) 메일 발송
    today = now_kst().strftime("%Y-%m-%d")
    send_mail_to(RECIPIENTS, f"관세·무역 뉴스 브리핑 ({today})", html_prac)
    send_mail_to(RECIPIENTS_EXEC, f"[Executive] 관세·통상 핵심 TOP3 ({today})", html_exec)

    print("✅ STABLE 완료 (실무/임원 분리 발송 + out 저장)")
    print("OUT_FILES =", os.listdir(BASE_DIR))


if __name__ == "__main__":
    main()
