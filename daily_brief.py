# -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief (STABLE + Gemini Summary)

핵심 개선:
- TOP3 및 표 요약이 "제목=요약"으로 비는 문제 해결:
  - URL 본문을 (가능한 범위에서) 추출 → Gemini 요약 강제
  - Gemini 불가/실패 시: 본문(또는 RSS)에서 2~3줄 발췌 fallback
- Gemini는 GEMINI_ENABLED=1 & GEMINI_API_KEY 존재 시 사용
- 요약 호출/본문 fetch는 MAX_SUMMARY_CALLS(기본 20)로 제한
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

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_ENABLED = os.getenv("GEMINI_ENABLED", "0").strip() in ("1", "true", "True", "YES", "yes")

BASE_DIR = os.getenv("BASE_DIR", os.path.join(os.path.dirname(__file__), "out"))
os.makedirs(BASE_DIR, exist_ok=True)

CUSTOM_QUERIES_FILE = os.getenv("CUSTOM_QUERIES_FILE", os.path.join(os.path.dirname(__file__), "custom_queries.TXT"))
SITES_FILE = os.getenv("SITES_FILE", os.path.join(os.path.dirname(__file__), "sites.xlsx"))

WINDOW_START_HOUR_KST = int(os.getenv("WINDOW_START_HOUR_KST", "7"))  # 07~07
# 발송은 워크플로 스케줄로 08시 실행하도록 설정 (코드에서 강제 sleep 하지 않음)

# ===============================
# TIME
# ===============================
def now_kst():
    return dt.datetime.utcnow() + dt.timedelta(hours=9)

def today_kst_str():
    return now_kst().strftime("%Y-%m-%d")

# ===============================
# TEXT HELPERS
# ===============================
def strip_html(s: str) -> str:
    s = s or ""
    s = re.sub(r"(?is)<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_title(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w\s가-힣]", "", s)
    return s[:200]

def domain_of(url: str) -> str:
    try:
        from urllib.parse import urlparse
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""

def fallback_3lines(title: str, rss_summary: str) -> str:
    # RSS 요약이 너무 빈약/제목 반복이면 2~3줄 발췌로 대체
    t = strip_html(rss_summary)
    if not t or norm_title(t) == norm_title(title):
        return ""
    parts = re.split(r"(?<=[\.\!\?。！？])\s+|\s*\n\s*", t)
    parts = [p.strip() for p in parts if p.strip()]
    return "<br/>".join(parts[:3])[:900]

# ===============================
# RISK / SCORE (간단 버전)
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

TRADE_KEYWORDS = [
    "tariff", "duty", "customs", "hs", "section 232", "section 301", "ieepa",
    "export control", "sanction", "origin", "fta", "anti-dumping", "countervailing",
    "관세", "관세율", "통관", "원산지", "수출통제", "제재", "무역구제"
]

# TOP3 제외(비관련)
NON_RELEVANT = [
    "연예","스포츠","야구","축구","농구","테니스","골프","경기","우승",
    "사망","추모","결혼","이혼","드라마","영화","배우",
    "와인","wine","recipe","cook","cooking","맛집","여행","tour","festival"
]

def policy_score(title: str, summary: str, allowed_domains: set, link: str) -> int:
    t = f"{title} {summary}".lower()
    s = 1
    for kw, w in RISK_RULES:
        if kw in t:
            s += w
    # allowed domain bonus
    d = domain_of(link)
    if allowed_domains and d and d in allowed_domains:
        s += 2
    return min(s, 20)

# ===============================
# INPUT LOADER (TIGHT)
# ===============================
def load_custom_queries(path: str) -> list[str]:
    if not os.path.exists(path):
        return ["관세"]
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            x = line.strip()
            if not x:
                continue
            out.append(x)
    # 중복 제거(순서 유지)
    seen = set()
    uniq = []
    for q in out:
        k = q.lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(q)
    return uniq or ["관세"]

def _normalize_url(u: str) -> str:
    if u is None:
        return ""
    u = str(u).strip()
    if not u:
        return ""
    if not re.match(r"^https?://", u, flags=re.I):
        # excel에서 url이 텍스트(이름)로 들어온 경우가 있어 방어
        if " " in u and "." not in u:
            return ""
        u = "https://" + u
    return u

def load_sites_xlsx(path: str):
    """
    sites.xlsx expected:
      - sheet: 'SiteList' (권장)  (예외적으로 'sites'도 허용)
      - columns: name, url  (대소문자/공백 변형 허용)
    return:
      - domain_to_name: dict(domain -> name)
      - allowed_domains: set(domains)
    """
    if not os.path.exists(path):
        return {}, set()

    xls = pd.ExcelFile(path)
    sheet = "SiteList" if "SiteList" in xls.sheet_names else ("sites" if "sites" in xls.sheet_names else xls.sheet_names[0])
    df = pd.read_excel(path, sheet_name=sheet)

    # column normalize
    cols = {c: re.sub(r"\s+", "", str(c).strip().lower()) for c in df.columns}
    name_col = None
    url_col = None
    for c, cc in cols.items():
        if cc in ("name", "기관명", "사이트명"):
            name_col = c
        if cc in ("url", "link", "주소", "사이트url"):
            url_col = c
    if name_col is None or url_col is None:
        # fallback: first two columns
        name_col = df.columns[0]
        url_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]

    df = df[[name_col, url_col]].copy()
    df.columns = ["name", "url"]
    df["name"] = df["name"].astype(str).str.strip()
    df["url"] = df["url"].apply(_normalize_url)

    domain_to_name = {}
    for _, r in df.iterrows():
        u = (r.get("url") or "").strip()
        if not u:
            continue
        d = domain_of(u)
        if not d:
            continue
        domain_to_name[d] = str(r.get("name") or d).strip()

    allowed_domains = set(domain_to_name.keys())
    return domain_to_name, allowed_domains

# ===============================
# MULTI-LANG GOOGLE NEWS RSS
# ===============================
LANGS = [
    ("ko", "KR", "KR:ko"),
    ("en", "US", "US:en"),
    ("fr", "FR", "FR:fr"),
]

def build_google_rss_url(query: str, hl: str, gl: str, ceid: str) -> str:
    base = "https://news.google.com/rss/search?"
    return base + urlencode({"q": query, "hl": hl, "gl": gl, "ceid": ceid})

def fetch_google_news(query: str, limit: int = 30) -> list[dict]:
    rows = []
    for hl, gl, ceid in LANGS:
        rss = build_google_rss_url(query, hl, gl, ceid)
        feed = feedparser.parse(rss)
        for e in getattr(feed, "entries", [])[:limit]:
            title = getattr(e, "title", "").strip()
            link = getattr(e, "link", "").strip()
            published = getattr(e, "published", "") or getattr(e, "updated", "")
            summary = strip_html(getattr(e, "summary", "") or "")
            rows.append({
                "query": query,
                "lang": hl,
                "title": title,
                "summary": summary,
                "link": link,
                "published": published,
            })
    return rows

def dedup_rows(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["k_title"] = df["title"].apply(norm_title)
    df["k_link"] = df["link"].fillna("").astype(str).str.strip()
    # title+domain 기준으로도 중복 제거
    df["k_dom"] = df["link"].fillna("").astype(str).apply(domain_of)
    df = df.drop_duplicates(subset=["query", "k_link"], keep="first")
    df = df.drop_duplicates(subset=["query", "k_title", "k_dom"], keep="first")
    df = df.drop(columns=["k_title","k_link","k_dom"], errors="ignore")
    return df

# ===============================
# GEMINI + ARTICLE TEXT FETCH
# ===============================
def _http_get(url: str, timeout: int = 12) -> str:
    """Lightweight HTTP GET with UA. Returns text or empty."""
    if not url:
        return ""
    try:
        import requests  # type: ignore
        headers = {"User-Agent": "Mozilla/5.0 (compatible; customs-news-bot/1.0)"}
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        if r.status_code >= 400:
            return ""
        r.encoding = r.apparent_encoding or r.encoding
        return r.text or ""
    except Exception:
        try:
            import urllib.request
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; customs-news-bot/1.0)"},
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                b = resp.read()
            return b.decode("utf-8", errors="ignore")
        except Exception:
            return ""

def extract_article_text(html_text: str) -> str:
    """Extract readable text from html; best-effort."""
    if not html_text:
        return ""
    html_text = re.sub(r"(?is)<(script|style|noscript)[^>]*>.*?</\1>", " ", html_text)
    try:
        from bs4 import BeautifulSoup  # type: ignore
        soup = BeautifulSoup(html_text, "html.parser")
        main = soup.find("article") or soup.find("main") or soup.body
        text = (main.get_text(" ", strip=True) if main else soup.get_text(" ", strip=True))
    except Exception:
        text = re.sub(r"(?is)<[^>]+>", " ", html_text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:6000]

def fetch_article_text(url: str) -> str:
    raw = _http_get(url)
    return extract_article_text(raw)

def pick_fallback_lines(title: str, rss_summary: str, article_text: str) -> str:
    """Fallback when Gemini is unavailable: show 2~3 short lines."""
    src = (article_text or "").strip() or strip_html(rss_summary).strip()
    if not src:
        return fallback_3lines(title, rss_summary)
    src = re.sub(r"\s+", " ", src).strip()
    if norm_title(src[:200]) == norm_title(title):
        src = src[200:].strip()
    parts = re.split(r"(?<=[\.\!\?。！？])\s+|\s*\n\s*", src)
    parts = [p.strip() for p in parts if p.strip()]
    return "<br/>".join(parts[:3])[:900]

def gemini_client():
    import google.generativeai as genai  # type: ignore
    genai.configure(api_key=GEMINI_API_KEY)
    return genai.GenerativeModel("gemini-1.5-flash")

def ensure_korean_summaries(df: pd.DataFrame, allowed_domains: set | None = None) -> pd.DataFrame:
    """
    df['summary_ko']를 '의미있는 한국어 요약'으로 보강합니다.
    - GEMINI_ENABLED=1 & GEMINI_API_KEY 존재 시: Gemini로 강제 요약(제목+RSS요약+본문추출 기반)
    - 그 외: 본문(가능 시) 또는 RSS요약에서 2~3줄 발췌
    """
    df = df.copy()
    if "summary_ko" not in df.columns:
        df["summary_ko"] = ""

    allowed_domains = allowed_domains or set()
    max_calls = int(os.getenv("MAX_SUMMARY_CALLS", "20"))

    def needs(r) -> bool:
        cur = str(r.get("summary_ko","") or "").strip()
        title = str(r.get("title","") or "")
        return (not cur) or (norm_title(cur) == norm_title(title)) or (len(strip_html(cur)) < 40)

    mask = df.apply(needs, axis=1)
    if not mask.any():
        return df

    # 우선순위: 점수 높은 순
    if "score" not in df.columns:
        df["score"] = df.apply(lambda r: policy_score(str(r.get("title","")), str(r.get("summary","")), allowed_domains, str(r.get("link",""))), axis=1)

    idxs = df.loc[mask].sort_values("score", ascending=False).index.tolist()
    idxs_call = idxs[:max_calls]

    use_gemini = bool(GEMINI_ENABLED and GEMINI_API_KEY)
    model = None
    if use_gemini:
        try:
            model = gemini_client()
        except Exception:
            model = None
            use_gemini = False

    article_cache: dict[str, str] = {}

    def get_article(url: str) -> str:
        url = (url or "").strip()
        if not url:
            return ""
        if url in article_cache:
            return article_cache[url]
        if allowed_domains:
            d = domain_of(url)
            if d and d not in allowed_domains:
                article_cache[url] = ""
                return ""
        article_cache[url] = fetch_article_text(url)
        return article_cache[url]

    for i in idxs_call:
        r = df.loc[i]
        title = str(r.get("title","") or "")
        rss_sum = str(r.get("summary","") or "")
        url = str(r.get("link","") or "")
        article = get_article(url)

        if use_gemini and model is not None:
            prompt = (
                "당신은 삼성전자 관세/통상 담당자를 위한 요약 AI입니다.\n"
                "아래 뉴스 내용을 한국어로 2~3문장 요약하세요.\n"
                "규칙: (1) 관세/관세율/HS/232/301/IEEPA/수출통제/제재/원산지/FTA/통관 포인트가 있으면 포함\n"
                "      (2) 당사 사업(모바일/가전/네트워크/의료기기) 관점 영향 1문장 포함\n"
                "      (3) 제목을 그대로 반복하지 말 것\n"
                "      (4) 불릿/번호 없이 문장으로\n\n"
                f"[제목] {title}\n"
                f"[RSS요약] {strip_html(rss_sum)}\n"
                f"[본문발췌] {article[:2500]}\n"
                f"[URL] {url}\n"
            )
            try:
                resp = model.generate_content(prompt)
                out = (getattr(resp, "text", "") or "").strip()
                out = re.sub(r"\s+", " ", out).strip()
                if (not out) or (norm_title(out) == norm_title(title)) or (len(out) < 35):
                    out = pick_fallback_lines(title, rss_sum, article)
                df.at[i, "summary_ko"] = out[:1000]
                continue
            except Exception:
                pass

        df.at[i, "summary_ko"] = pick_fallback_lines(title, rss_sum, article)

    # 나머지는 RSS 기반 fallback
    rest = [i for i in idxs if i not in idxs_call]
    for i in rest:
        df.at[i, "summary_ko"] = fallback_3lines(str(df.at[i, "title"]), str(df.at[i, "summary"]))

    return df

# ===============================
# TOP3 PICK
# ===============================
def ok_row(r, allowed_domains: set) -> bool:
    title = str(r.get("title","") or "")
    summ = str(r.get("summary","") or "")
    blob = (title + " " + summ).lower()

    # non-relevant
    if any(x.lower() in blob for x in NON_RELEVANT):
        return False

    # must contain trade-ish keyword
    if not any(k in blob for k in [k.lower() for k in TRADE_KEYWORDS]):
        return False

    # if allowed domains list exists, prefer those (not hard block)
    return True

def pick_top3(df: pd.DataFrame, allowed_domains: set) -> pd.DataFrame:
    if df.empty:
        return df

    cand = df.copy()
    cand = cand[cand.apply(lambda r: ok_row(r, allowed_domains), axis=1)].copy()

    # 부족하면 점수순으로 채우기(단 non-relevant 제외)
    if len(cand) < 3:
        df2 = df.copy()
        df2 = df2[~df2.apply(lambda r: any(x.lower() in (str(r.get("title",""))+" "+str(r.get("summary",""))).lower() for x in NON_RELEVANT), axis=1)].copy()
        cand = pd.concat([cand, df2], ignore_index=True).drop_duplicates(subset=["link","title"], keep="first")

    cand = cand.sort_values("score", ascending=False).head(3)
    return cand

# ===============================
# OUTPUT TABLE (제시어별 10건 제한)
# ===============================
def build_table(df: pd.DataFrame) -> str:
    # 제시어별 10건 제한 + 정렬(제시어, 중요도/점수)
    df = df.copy()

    # 중요도 표시
    def importance(score: int) -> str:
        if score >= 10:
            return "상"
        if score >= 6:
            return "중"
        return "하"
    df["중요도"] = df["score"].apply(lambda x: importance(int(x)))

    # 제시어별 상위 10개
    df = df.sort_values(["query", "score"], ascending=[True, False])
    df = df.groupby("query", as_index=False).head(10).copy()

    # 제시어별 건수 라인
    counts = df.groupby("query").size().to_dict()
    counts_line = ", ".join([f"{k} {v}건" for k, v in counts.items()])

    rows = []
    for _, r in df.iterrows():
        title = html.escape(str(r.get("title","") or ""))
        url = str(r.get("link","") or "#")
        summ_ko = str(r.get("summary_ko","") or "").strip()
        if not summ_ko:
            summ_ko = fallback_3lines(str(r.get("title","")), str(r.get("summary","")))
        summ_ko = summ_ko.replace("\n","<br/>")
        pub = html.escape(str(r.get("published","") or ""))
        q = html.escape(str(r.get("query","") or ""))
        imp = html.escape(str(r.get("중요도","") or ""))

        rows.append(f"""
        <tr>
          <td style="width:88px;white-space:nowrap;">{q}<br/><span class="small">({imp})</span></td>
          <td>
            <a href="{url}" target="_blank">{title}</a><br/>
            <span class="small">{summ_ko}</span>
          </td>
          <td style="width:140px;">{pub}</td>
          <td style="width:70px;">점수 {int(r.get("score",0))}</td>
        </tr>
        """)

    table = f"""
    <div class="box">
      <h3>④ 정책 이벤트 표</h3>
      <div class="small" style="margin:6px 0 10px 0;">제시어별 주요뉴스 건수: {html.escape(counts_line)}</div>
      <table>
        <tr>
          <th style="width:88px;">제시어(중요도)</th>
          <th>헤드라인 / 주요내용</th>
          <th style="width:140px;">발표일</th>
          <th style="width:70px;">비고</th>
        </tr>
        {''.join(rows)}
      </table>
    </div>
    """
    return table

# ===============================
# HTML STYLE (A4 Landscape 느낌)
# ===============================
STYLE = """
<style>
body{font-family:Malgun Gothic,Arial; background:#f6f6f6;}
.page{max-width:1120px;margin:auto;background:white;padding:14px;}
h2{margin-bottom:6px;}
.box{border:1px solid #ddd;border-radius:8px;padding:12px;margin:12px 0;}
li{margin-bottom:12px;}
table{border-collapse:collapse;width:100%;}
th,td{border:1px solid #ccc;padding:7px;font-size:12px;vertical-align:top;}
th{background:#f0f0f0;}
.small{font-size:11px;color:#555;line-height:1.35;}
</style>
"""

def build_top3_blocks(top3: pd.DataFrame) -> tuple[str,str,str]:
    items = []
    why = []
    chk = []

    for _, r in top3.iterrows():
        title = html.escape(str(r.get("title","") or ""))
        url = str(r.get("link","") or "#")
        summ_ko = str(r.get("summary_ko","") or "").strip()
        if not summ_ko:
            summ_ko = fallback_3lines(str(r.get("title","")), str(r.get("summary","")))
        if not summ_ko:
            # 최후: 제목만 나오지 않게 방어
            summ_ko = "요약 생성 실패(본문 접근 제한). 원문 링크를 확인해 주세요."
        score = int(r.get("score", 0))
        q = str(r.get("query","") or "")

        items.append(f"""
        <li>
          <b>[{html.escape(q)} | 점수 {score}]</b><br/>
          <a href="{url}" target="_blank">{title}</a><br/>
          <div class="small">{summ_ko}</div>
        </li>
        """)

        # 중복 문구 방지: 이벤트별 1줄만
        why.append(f"<li><b>{html.escape(q)}</b>: 관세/통상 규제 변화 가능 → 원가/마진/리드타임/공급망 리스크로 연결될 수 있음</li>")
        chk.append(f"<li><b>{html.escape(q)}</b>: (1) 대상국/품목(HS) 확인 (2) 생산지/판매법인 영향(원가·마진·리드타임) 1차 산정 (3) 필요 시 HQ 대응 착수</li>")

    return "".join(items), "".join(why), "".join(chk)

def build_html_exec(df: pd.DataFrame, top3: pd.DataFrame) -> str:
    date = today_kst_str()
    items_html, why_html, chk_html = build_top3_blocks(top3)

    return f"""
    <html><head>{STYLE}</head>
    <body><div class="page">
      <h2>[Executive] 관세·통상 핵심 TOP3 ({date})</h2>

      <div class="box">
        <h3>① 관세·통상 핵심 TOP3</h3>
        <ul>{items_html}</ul>
      </div>

      <div class="box">
        <h3>② 왜 중요한가 (TOP3 이벤트 기반)</h3>
        <ul>{why_html}</ul>
      </div>

      <div class="box">
        <h3>③ 당사 관점 체크포인트 (TOP3 이벤트 기반)</h3>
        <ul>{chk_html}</ul>
      </div>

    </div></body></html>
    """

def build_html_practitioner(df: pd.DataFrame, top3: pd.DataFrame) -> str:
    date = today_kst_str()
    items_html, why_html, chk_html = build_top3_blocks(top3)
    table = build_table(df)

    return f"""
    <html><head>{STYLE}</head>
    <body><div class="page">
      <h2>관세·무역 뉴스 브리핑 ({date})</h2>

      <div class="box">
        <h3>① 오늘의 핵심 정책 이벤트 TOP3</h3>
        <ul>{items_html}</ul>
      </div>

      <div class="box">
        <h3>② 왜 중요한가 (TOP3 이벤트 기반)</h3>
        <ul>{why_html}</ul>
      </div>

      <div class="box">
        <h3>③ 당사 관점 체크포인트 (TOP3 이벤트 기반)</h3>
        <ul>{chk_html}</ul>
      </div>

      {table}

    </div></body></html>
    """

# ===============================
# OUTPUTS
# ===============================
def write_outputs(df: pd.DataFrame, html_body: str):
    today = today_kst_str()
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
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_EMAIL
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
        s.starttls()
        if SMTP_EMAIL and SMTP_PASSWORD:
            s.login(SMTP_EMAIL, SMTP_PASSWORD)
        s.sendmail(SMTP_EMAIL, recipients, msg.as_string())

# ===============================
# MAIN
# ===============================
def main():
    print("[INFO] BASE_DIR =", BASE_DIR)
    print("[INFO] CUSTOM_QUERIES_FILE =", CUSTOM_QUERIES_FILE)
    print("[INFO] SITES_FILE =", SITES_FILE)
    print("[INFO] GEMINI_ENABLED =", GEMINI_ENABLED, "| GEMINI_API_KEY set =", bool(GEMINI_API_KEY))

    domain_to_name, allowed_domains = load_sites_xlsx(SITES_FILE)
    print(f"[INFO] sites.xlsx loaded: domains={len(allowed_domains)}")

    queries = load_custom_queries(CUSTOM_QUERIES_FILE)
    print(f"[INFO] custom queries loaded: {len(queries)}")

    all_rows = []
    for q in queries:
        rows = fetch_google_news(q, limit=30)
        all_rows.extend(rows)

    df = pd.DataFrame(all_rows)
    if df.empty:
        print("오늘 수집된 이벤트/뉴스 없음")
        return

    df = dedup_rows(df)
    df["score"] = df.apply(lambda r: policy_score(str(r.get("title","")), str(r.get("summary","")), allowed_domains, str(r.get("link",""))), axis=1)

    # ✅ 요약 보강(Gemini 강제 + 실패 시 본문 2~3줄)
    df = ensure_korean_summaries(df, allowed_domains)

    # TOP3
    top3 = pick_top3(df, allowed_domains)

    # 메일 HTML
    html_exec = build_html_exec(df, top3)
    html_prac = build_html_practitioner(df, top3)

    # outputs는 실무자용(표 포함) 기준 저장
    write_outputs(df, html_prac)

    today = today_kst_str()
    send_mail_to(RECIPIENTS, f"관세·무역 뉴스 브리핑 ({today})", html_prac)
    send_mail_to(RECIPIENTS_EXEC, f"[Executive] 관세·통상 핵심 TOP3 ({today})", html_exec)

    print("✅ DONE: mail sent + outputs written")

if __name__ == "__main__":
    main()
