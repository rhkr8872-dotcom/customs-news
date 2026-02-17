# -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief
vCurrent STABLE (GitHub Actions friendly)

Goals (A: 안정 운영)
- custom_queries.TXT 로 '제시어' 로딩
- sites.xlsx 로 '정부/공인단체 사이트(허용 도메인)' 로딩 (시트명/컬럼명 변동에도 견고)
- Google News RSS로 멀티언어(한/영/불) 검색 → 중복 제거 → 점수화 → 표/메일(실무/임원) 생성
- Gemini API 키가 있으면 TOP3/요약 강제(없으면 규칙 기반 fallback)

ENV
- SMTP_SERVER, SMTP_PORT, SMTP_EMAIL, SMTP_PASSWORD
- RECIPIENTS (실무), RECIPIENTS_EXEC (임원)
- BASE_DIR (default: ./out)
- NEWS_WINDOW_HOURS (default: 24)  # 07~07은 24시간 윈도우로 처리 (메일 발송은 Actions cron에서 08시로)
- GEMINI_API_KEY (optional)
- GEMINI_MODEL (optional, default gemini-1.5-flash)
"""

from __future__ import annotations

import os, re, html, smtplib, json, hashlib
import datetime as dt
from typing import List, Dict, Tuple, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

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

CUSTOM_QUERIES_FILE = os.getenv("CUSTOM_QUERIES_FILE", os.path.join(os.path.dirname(__file__), "custom_queries.TXT"))
SITES_FILE = os.getenv("SITES_FILE", os.path.join(os.path.dirname(__file__), "sites.xlsx"))

NEWS_WINDOW_HOURS = int(os.getenv("NEWS_WINDOW_HOURS", "24"))

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")

# ===============================
# TIME
# ===============================
def now_kst() -> dt.datetime:
    return dt.datetime.utcnow() + dt.timedelta(hours=9)

def parse_dt_any(s: str) -> Optional[dt.datetime]:
    if not s:
        return None
    s = str(s).strip()
    # feedparser often gives RFC822; pandas can parse many forms
    try:
        return pd.to_datetime(s, utc=True).to_pydatetime()
    except Exception:
        return None

def within_window(published_str: str, hours: int) -> bool:
    d = parse_dt_any(published_str)
    if not d:
        return True  # 날짜가 없으면 일단 포함 (대신 점수/표기에서 빈칸)
    # convert to KST for window compare
    d_kst = d.astimezone(dt.timezone(dt.timedelta(hours=9)))
    return (now_kst() - d_kst.replace(tzinfo=None)) <= dt.timedelta(hours=hours)

# ===============================
# CONFIG: 당사 맥락(템플릿 강화용)
# ===============================
SAMSUNG_PRODUCTS = [
    "모바일(휴대폰/태블릿/워치/이어폰)",
    "생활가전(에어컨/오븐/냉장고/청소기/TV/모니터/사운드바)",
    "네트워크(5G 기지국/안테나)",
    "의료기기(X-ray 등)"
]
SAMSUNG_PRODUCTION = ["한국","중국","베트남","인도","인도네시아","터키","슬로바키아","폴란드","멕시코","브라질"]

# ===============================
# LOADER: custom_queries.TXT (제시어)
# ===============================
def load_custom_queries(path: str) -> List[str]:
    if not os.path.exists(path):
        # fallback minimal
        return ["관세", "세관", "FTA", "원산지", "수출통제", "AEO", "WCO"]
    out = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            t = line.strip()
            if not t or t.startswith("#"):
                continue
            out.append(t)
    # de-dup while preserving order
    seen = set()
    uniq = []
    for q in out:
        k = q.lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(q)
    return uniq

# ===============================
# LOADER: sites.xlsx (정부/공인단체 사이트)
# - 시트명: SiteList / sites / Site / sheet1 ... 어떤 것이든 수용
# - 컬럼: name, url (대소문자/공백/한글 '기관명','URL' 등 수용)
# ===============================
def _normalize_url(u) -> str:
    if u is None:
        return ""
    if isinstance(u, float):
        # NaN
        try:
            if pd.isna(u):
                return ""
        except Exception:
            pass
        u = ""
    u = str(u).strip()
    if not u:
        return ""
    if not re.match(r"^https?://", u, flags=re.I):
        u = "https://" + u
    return u

def _pick_sheet(xls: pd.ExcelFile) -> str:
    prefs = ["SiteList", "sites", "site", "Site", "SITES", "Sheet1", "sheet1"]
    for p in prefs:
        if p in xls.sheet_names:
            return p
    return xls.sheet_names[0]

def _norm_col(c: str) -> str:
    return re.sub(r"\s+", "", str(c).strip().lower())

def load_sites_xlsx(path: str) -> Tuple[Dict[str, str], set]:
    """
    returns:
      - domain_to_name: {domain: 기관명}
      - allowed_domains: set(domains)
    """
    domain_to_name: Dict[str, str] = {}
    allowed_domains = set()

    if not os.path.exists(path):
        return domain_to_name, allowed_domains

    xls = pd.ExcelFile(path)
    sheet = _pick_sheet(xls)
    df = xls.parse(sheet)

    # columns detection
    cols = {_norm_col(c): c for c in df.columns}
    name_col = None
    url_col = None
    for key in ["name", "기관명", "기관", "site", "sitename"]:
        nk = _norm_col(key)
        if nk in cols:
            name_col = cols[nk]
            break
    for key in ["url", "link", "주소", "원본링크"]:
        uk = _norm_col(key)
        if uk in cols:
            url_col = cols[uk]
            break

    # very common case from user screenshot: columns are 'name' and 'url'
    if name_col is None:
        for c in df.columns:
            if "name" in _norm_col(c) or "기관" in str(c):
                name_col = c
                break
    if url_col is None:
        for c in df.columns:
            if "url" in _norm_col(c) or "http" in _norm_col(c) or "링크" in str(c):
                url_col = c
                break

    if url_col is None:
        # no usable url column
        return domain_to_name, allowed_domains

    for _, row in df.iterrows():
        u = _normalize_url(row.get(url_col))
        if not u:
            continue
        parsed = urlparse(u)
        dom = (parsed.netloc or "").lower()
        dom = dom[4:] if dom.startswith("www.") else dom
        if not dom:
            continue
        nm = str(row.get(name_col, "")).strip() if name_col else ""
        domain_to_name[dom] = nm or domain_to_name.get(dom, "")
        allowed_domains.add(dom)

    return domain_to_name, allowed_domains

# ===============================
# MULTI-LANG QUERY EXPANSION (한/영/불)
# ===============================
QUERY_EXPAND = {
    "관세": ["tariff", "customs duty", "tarif douanier", "droit de douane"],
    "세관": ["customs", "douane"],
    "수출입": ["import export", "trade", "importation exportation"],
    "전략물자": ["export control", "dual-use", "contrôle des exportations", "biens à double usage"],
    "fta": ["free trade agreement", "accord de libre-échange"],
    "보세공장": ["bonded factory", "bonded warehouse", "entrepôt sous douane"],
    "외국환거래": ["foreign exchange", "FX regulation", "réglementation de change"],
    "aeo": ["AEO", "authorized economic operator", "OEA opérateur économique agréé"],
    "wco": ["WCO", "world customs organization", "OMD organisation mondiale des douanes"],
    "원산지": ["origin", "rules of origin", "origine", "règles d'origine"],
    "수출통제": ["export control", "sanctions", "contrôle des exportations", "sanctions"],
}

def build_query_variants(base_kw: str) -> List[str]:
    base = base_kw.strip()
    extras = QUERY_EXPAND.get(base.lower(), QUERY_EXPAND.get(base, []))
    # always include base itself
    variants = [base]
    for e in extras:
        if e and e.lower() not in {v.lower() for v in variants}:
            variants.append(e)
    # cap to 3 terms per keyword to avoid overly long RSS query
    return variants[:3]

def build_google_rss_url(terms: List[str]) -> str:
    # terms as OR query; quote multi-word
    parts = []
    for t in terms:
        t = t.strip()
        if " " in t:
            parts.append(f'"{t}"')
        else:
            parts.append(t)
    q = " OR ".join(parts)
    params = {"q": q, "hl": "ko", "gl": "KR", "ceid": "KR:ko"}
    return "https://news.google.com/rss/search?" + urlencode(params)

# ===============================
# POLICY SCORE
# ===============================
RISK_RULES = [
    ("section 301", 6), ("section 232", 6), ("ieepa", 6),
    ("export control", 6), ("수출통제", 6),
    ("sanction", 6), ("제재", 6),
    ("entity list", 5), ("anti-dumping", 5), ("countervailing", 5), ("safeguard", 5),
    ("tariff", 4), ("duty", 4), ("관세", 4), ("관세율", 4), ("추가관세", 4),
    ("hs code", 3), ("hs", 3), ("원산지", 3), ("fta", 3), ("customs", 3), ("통관", 3),
    ("개정", 2), ("고시", 2), ("시행", 2), ("규정", 2),
]

def calc_policy_score(title: str, summary: str) -> int:
    t = f"{title} {summary}".lower()
    score = 1
    for kw, w in RISK_RULES:
        if kw in t:
            score += w
    return min(score, 20)

# ===============================
# COUNTRY TAG (간단 규칙)
# ===============================
COUNTRY_KEYWORDS = {
    "USA": ["u.s.", "united states", "america", "section 301", "section 232", "u.s. trade"],
    "India": ["india"],
    "Türkiye": ["turkey", "türkiye"],
    "Vietnam": ["vietnam"],
    "Netherlands": ["netherlands", "dutch"],
    "EU": ["european union", "eu commission", "european commission"],
    "China": ["china"],
    "Mexico": ["mexico"],
    "Brazil": ["brazil"],
    "Poland": ["poland"],
    "Slovakia": ["slovakia"],
    "Indonesia": ["indonesia"],
    "Korea": ["korea", "south korea", "republic of korea"],
}

def detect_country(text: str) -> str:
    t = (text or "").lower()
    for country, keys in COUNTRY_KEYWORDS.items():
        if any(k in t for k in keys):
            return country
    return ""

# ===============================
# DEDUP
# ===============================
def norm_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w\s가-힣]", "", s)
    return s

def make_fingerprint(title: str, link: str) -> str:
    dom = urlparse(link or "").netloc.lower()
    dom = dom[4:] if dom.startswith("www.") else dom
    base = norm_text(title) + "|" + dom
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def dedup_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    df["fp"] = df.apply(lambda r: make_fingerprint(str(r.get("헤드라인","")), str(r.get("출처(URL)",""))), axis=1)
    # keep highest score for dup
    df = df.sort_values(["fp", "점수"], ascending=[True, False]).drop_duplicates("fp", keep="first")
    df = df.drop(columns=["fp"], errors="ignore")
    return df

# ===============================
# GEMINI (optional) - Korean summary + relevance/action
# ===============================
def gemini_generate(prompt: str) -> Optional[str]:
    if not GEMINI_API_KEY:
        return None
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.3, "maxOutputTokens": 500}
        }
        req = Request(url, data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        # extract
        cands = data.get("candidates") or []
        if not cands:
            return None
        parts = cands[0].get("content", {}).get("parts") or []
        if not parts:
            return None
        return str(parts[0].get("text","")).strip() or None
    except Exception:
        return None

def needs_better_summary(title: str, summary: str) -> bool:
    t = (title or "").strip()
    s = (summary or "").strip()
    if not s:
        return True
    # if summary essentially same as title
    nt, ns = norm_text(t), norm_text(s)
    if not ns:
        return True
    if nt and ns and (nt in ns or ns in nt):
        return True
    if len(ns) < 25:
        return True
    return False

def enrich_with_ai(df: pd.DataFrame, top_n: int = 3) -> pd.DataFrame:
    """
    Only enrich TOP N rows by score (cost control)
    Adds:
      - ai_요약 (Korean 2-3 lines)
      - ai_연관성 (당사 관점)
      - ai_Action (권고)
    """
    if df is None or df.empty:
        return df
    df = df.copy()
    df["ai_요약"] = ""
    df["ai_연관성"] = ""
    df["ai_Action"] = ""
    # pick top N for forced quality
    cand = df.sort_values("점수", ascending=False).head(top_n).index.tolist()
    for idx in cand:
        title = str(df.at[idx, "헤드라인"])
        summary = str(df.at[idx, "주요내용"])
        link = str(df.at[idx, "출처(URL)"])
        country = str(df.at[idx, "대상 국가"])
        kw = str(df.at[idx, "제시어"])

        if GEMINI_API_KEY:
            prompt = f"""너는 삼성전자 관세/통상 실무 지원 AI다.
아래 뉴스(제목/본문요약)를 읽고, 한국어로만 결과를 작성해라.

[제시어] {kw}
[대상국] {country}
[제목] {title}
[요약/스니펫] {summary}
[링크] {link}

출력 형식(각 항목은 2~3문장, 너무 길게 쓰지 말 것):
1) 요약:
2) 당사 연관성(제품/생산지 관점 포함):
3) 권고 Action(HS/대상국/법인 영향 산정 중심, 3개 이내):
"""
            out = gemini_generate(prompt)
            if out:
                # simple parse
                def pick(label):
                    m = re.search(rf"{label}\s*:\s*(.+?)(?=\n\d\)|\Z)", out, flags=re.S)
                    return m.group(1).strip() if m else ""
                df.at[idx, "ai_요약"] = pick("1\\) 요약") or pick("요약") or out.strip()
                df.at[idx, "ai_연관성"] = pick("2\\) 당사 연관성") or pick("당사 연관성") or ""
                df.at[idx, "ai_Action"] = pick("3\\) 권고 Action") or pick("권고 Action") or ""
                continue

        # fallback (no key or failed)
        if needs_better_summary(title, summary):
            # 규칙 기반: 요약이 빈약하면 제목 기반 짧은 설명
            df.at[idx, "ai_요약"] = f"기사 요약 정보가 제한적입니다. 제목 기준으로 관세/통상 이슈 여부를 우선 확인 필요: {title[:80]}"
        else:
            df.at[idx, "ai_요약"] = summary[:220]

        # relevance/action templates
        prod = ", ".join(SAMSUNG_PRODUCTS[:2]) + " 등"
        prod_cty = ", ".join(SAMSUNG_PRODUCTION)
        df.at[idx, "ai_연관성"] = (
            f"{country or '해당국'} 정책 변화가 관세/통관/수출통제에 연결될 경우, "
            f"{prod} 제품군의 원가·리드타임·판매가(마진) 영향 가능. "
            f"생산거점({prod_cty}) 연계 공급망을 우선 점검."
        )
        df.at[idx, "ai_Action"] = "HS/대상국/적용시점 확인 → 생산/판매법인 영향 1차 산정 → 필요 시 대응(가격/원산지/대체조달) 착수"

    return df

# ===============================
# SENSOR (RSS)
# ===============================
def run_sensor_build_df(keywords: List[str], allowed_domains: set, domain_to_name: Dict[str,str]) -> pd.DataFrame:
    rows = []
    for kw in keywords:
        terms = build_query_variants(kw)
        rss = build_google_rss_url(terms)
        feed = feedparser.parse(rss)

        for e in feed.entries[:40]:
            title = getattr(e, "title", "").strip()
            link = getattr(e, "link", "").strip()
            published = getattr(e, "published", "") or getattr(e, "updated", "")
            if not within_window(published, NEWS_WINDOW_HOURS):
                continue

            # snippet
            summary = getattr(e, "summary", "") or ""
            summary = re.sub(r"<[^>]+>", "", summary).strip()

            # filter: allow if contains tariff/customs/trade terms (avoid pure unrelated)
            blob = f"{title} {summary}".lower()
            if not any(x in blob for x in ["tariff", "customs", "duty", "trade", "관세", "세관", "통관", "fta", "원산지", "수출통제", "sanction", "제재", "section 301", "section 232", "ieepa"]):
                continue

            # domain allowlist (정부/공인단체만 강제하고 싶으면 strict=True로 바꿀 수 있음)
            dom = urlparse(link).netloc.lower()
            dom = dom[4:] if dom.startswith("www.") else dom
            agency = domain_to_name.get(dom, "")
            # 현재는 '허용도메인'이면 기관명 표기, 아니면 빈칸(출처 불명확 방지 차원)
            if allowed_domains and dom in allowed_domains and not agency:
                agency = dom

            country = detect_country(f"{title} {summary}")
            score = calc_policy_score(title, summary)

            rows.append({
                "제시어": kw,
                "헤드라인": title,
                "주요내용": summary[:600],
                "발표일": published,
                "대상 국가": country,
                "관련 기관": agency,
                "출처(URL)": link,
                "중요도": "",  # later
                "점수": score,
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # importance mapping from score
    def to_importance(s: int) -> str:
        if s >= 14:
            return "상"
        if s >= 8:
            return "중"
        return "하"
    df["중요도"] = df["점수"].apply(to_importance)

    return df

# ===============================
# POST-PROCESS: sort, limit per keyword, fill blanks
# ===============================
IMPORTANCE_RANK = {"상": 1, "중": 2, "하": 3}

def finalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()

    # sanitize
    for c in ["제시어","헤드라인","주요내용","발표일","대상 국가","관련 기관","출처(URL)","중요도"]:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].fillna("").astype(str)

    # dedup globally
    df = dedup_df(df)

    # limit per keyword (10)
    out = []
    for kw, g in df.groupby("제시어", dropna=False):
        g = g.sort_values(["점수"], ascending=False).head(10)
        out.append(g)
    df = pd.concat(out, ignore_index=True) if out else df

    # sort by 제시어 then 중요도(상→중→하) then 점수 desc
    df["_imp"] = df["중요도"].map(IMPORTANCE_RANK).fillna(9).astype(int)
    df = df.sort_values(["제시어","_imp","점수"], ascending=[True, True, False]).drop(columns=["_imp"])
    return df

# ===============================
# HTML STYLE (A4 landscape)
# ===============================
STYLE = """
<style>
@page { size: A4 landscape; margin: 10mm; }
body{font-family:Malgun Gothic,Arial; background:#f6f6f6;}
.page{max-width:297mm;margin:auto;background:white;padding:10px;}
h2{margin:0 0 6px 0;}
.box{border:1px solid #ddd;border-radius:8px;padding:10px;margin:10px 0;}
li{margin-bottom:10px;}
.small{font-size:11px;color:#555;}
.kpi{font-size:12px;color:#333;margin-top:4px;}
table{border-collapse:collapse;width:100%; table-layout:fixed;}
th,td{border:1px solid #ccc;padding:6px;font-size:11px;vertical-align:top; word-break:break-word;}
th{background:#f0f0f0;}
.col-kw{width:11%;}
.col-imp{width:6%;}
.col-date{width:12%;}
.col-cty{width:8%;}
.col-agy{width:12%;}
.col-news{width:auto;}
</style>
"""

def escape(s: str) -> str:
    return html.escape(s or "")

def link_html(url: str, text: str) -> str:
    u = url or "#"
    return f'<a href="{escape(u)}" target="_blank">{escape(text)}</a>'

# ===============================
# BUILD: 실무자 메일 (표 유지, 출처 칸 삭제, 헤드라인에 링크, 요약은 separate text)
# ===============================
def build_counts_line(df: pd.DataFrame) -> str:
    counts = df.groupby("제시어").size().to_dict()
    parts = [f"{k} {v}건" for k, v in counts.items()]
    return ", ".join(parts)

def build_top3_blocks(df_ai: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    cand = df_ai.sort_values("점수", ascending=False).head(3)
    html_items = ""
    for _, r in cand.iterrows():
        summary_ko = (r.get("ai_요약") or "").strip() or (r.get("주요내용") or "").strip()
        if needs_better_summary(r.get("헤드라인",""), summary_ko) and GEMINI_API_KEY:
            # should have been filled; but keep safe
            summary_ko = summary_ko or "요약 생성 실패(원문 확인 필요)"
        html_items += f"""
<li>
  <b>[{escape(r.get('제시어',''))}｜{escape(r.get('대상 국가',''))}｜{escape(r.get('중요도',''))}｜점수 {escape(str(r.get('점수','')))}]</b><br/>
  {link_html(r.get('출처(URL)',''), str(r.get('헤드라인','')))}<br/>
  <div class="small">{escape(summary_ko[:260])}</div>
</li>
"""
    return cand, html_items

def build_why_and_action(top3: pd.DataFrame) -> Tuple[str,str]:
    # 중복 제거: 동일 문구는 집계 후 1회만
    why_lines = []
    act_lines = []
    for _, r in top3.iterrows():
        kw = r.get("제시어","")
        cty = r.get("대상 국가","") or "해당국"
        rel = (r.get("ai_연관성") or "").strip()
        act = (r.get("ai_Action") or "").strip()

        if not rel:
            rel = f"{cty} 관세/통상 정책 변화 가능성 → 당사 주요 제품({', '.join(SAMSUNG_PRODUCTS)}) 원가·마진·리드타임 영향 가능"
        if not act:
            act = "HS/대상국/적용시점 확인 → 법인 영향 1차 산정 → 필요 시 HQ 대응 착수"

        why_lines.append(f"[{kw}] {rel}")
        act_lines.append(f"[{kw}] {act}")

    # de-dup exact
    why_uniq = []
    seen = set()
    for x in why_lines:
        k = norm_text(x)
        if k in seen: 
            continue
        seen.add(k); why_uniq.append(x)

    act_uniq = []
    seen = set()
    for x in act_lines:
        k = norm_text(x)
        if k in seen:
            continue
        seen.add(k); act_uniq.append(x)

    why_html = "<ul>" + "".join(f"<li>{escape(x)}</li>" for x in why_uniq) + "</ul>"
    act_html = "<ul>" + "".join(f"<li>{escape(x)}</li>" for x in act_uniq) + "</ul>"
    return why_html, act_html

def build_table_rows(df: pd.DataFrame) -> str:
    rows = ""
    for _, r in df.iterrows():
        headline = str(r.get("헤드라인",""))
        summary = str(r.get("주요내용",""))
        # table cell: headline(link) + summary
        cell = f"{link_html(r.get('출처(URL)',''), headline)}<br/>{escape(summary)}"
        rows += f"""
<tr>
  <td class="col-kw">{escape(r.get('제시어',''))}</td>
  <td class="col-imp">{escape(r.get('중요도',''))}</td>
  <td class="col-date">{escape(r.get('발표일',''))}</td>
  <td class="col-cty">{escape(r.get('대상 국가',''))}</td>
  <td class="col-agy">{escape(r.get('관련 기관',''))}</td>
  <td class="col-news">{cell}</td>
</tr>
"""
    return rows

def build_html_practitioner(df: pd.DataFrame, df_ai: pd.DataFrame) -> str:
    date = now_kst().strftime("%Y-%m-%d")
    counts_line = build_counts_line(df)

    top3_df, top3_html = build_top3_blocks(df_ai)
    why_html, act_html = build_why_and_action(top3_df)

    rows = build_table_rows(df)

    return f"""
<html><head>{STYLE}</head>
<body>
<div class="page">
  <h2>관세·통상 데일리 브리프 ({date})</h2>
  <div class="kpi"><b>제시어별 건수:</b> {escape(counts_line)}</div>

  <div class="box">
    <h3>① 관세·통상 핵심 TOP3</h3>
    <ul>{top3_html}</ul>
  </div>

  <div class="box">
    <h3>② 왜 중요한가 (TOP3 기반)</h3>
    {why_html}
  </div>

  <div class="box">
    <h3>③ 당사 관점 체크포인트 (TOP3 기반)</h3>
    {act_html}
  </div>

  <div class="box">
    <h3>④ 정책 이벤트 표</h3>
    <div class="small" style="margin-bottom:6px;">(표는 제시어별 중복 제거 후 최대 10건)</div>
    <table>
      <tr>
        <th class="col-kw">제시어</th>
        <th class="col-imp">중요도</th>
        <th class="col-date">발표일</th>
        <th class="col-cty">국가</th>
        <th class="col-agy">관련기관</th>
        <th class="col-news">헤드라인 / 주요내용</th>
      </tr>
      {rows}
    </table>
  </div>
</div>
</body></html>
"""

# ===============================
# BUILD: 임원용 (TOP3 + 요약/연관성/Action 강제)
# ===============================
def build_html_exec(df_ai: pd.DataFrame) -> str:
    date = now_kst().strftime("%Y-%m-%d")
    top3 = df_ai.sort_values("점수", ascending=False).head(3)

    items = ""
    for _, r in top3.iterrows():
        summary_ko = (r.get("ai_요약") or "").strip() or (r.get("주요내용") or "").strip()
        rel = (r.get("ai_연관성") or "").strip()
        act = (r.get("ai_Action") or "").strip()

        if not summary_ko:
            summary_ko = "요약 생성 실패(원문 확인 필요)"
        if not rel:
            rel = "당사 주요 생산/판매 법인 기준 원가·마진·리드타임 영향 가능"
        if not act:
            act = "HS/대상국 확인 → 영향 1차 산정 → 대응 착수"

        items += f"""
<li>
  <b>[{escape(r.get('대상 국가',''))}｜{escape(r.get('중요도',''))}｜점수 {escape(str(r.get('점수','')))}]</b><br/>
  {link_html(r.get('출처(URL)',''), str(r.get('헤드라인','')))}<br/>
  <div class="small"><b>요약:</b> {escape(summary_ko[:260])}</div>
  <div class="small"><b>당사 연관성:</b> {escape(rel[:260])}</div>
  <div class="small"><b>Action:</b> {escape(act[:260])}</div>
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
</div>
</body></html>
"""

# ===============================
# OUTPUTS
# ===============================
def write_outputs(df: pd.DataFrame, html_body: str):
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
def send_mail_to(recipients: List[str], subject: str, html_body: str):
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
    print("BASE_DIR =", BASE_DIR)
    print("CUSTOM_QUERIES_FILE =", CUSTOM_QUERIES_FILE)
    print("SITES_FILE =", SITES_FILE)

    keywords = load_custom_queries(CUSTOM_QUERIES_FILE)
    domain_to_name, allowed_domains = load_sites_xlsx(SITES_FILE)

    # sensor
    df_raw = run_sensor_build_df(keywords, allowed_domains, domain_to_name)
    if df_raw is None or df_raw.empty:
        print("오늘 수집된 이벤트/뉴스 없음")
        return

    df = finalize_df(df_raw)

    # enrich TOP3 only (forced quality)
    df_ai = enrich_with_ai(df, top_n=3)

    # build htmls
    today = now_kst().strftime("%Y-%m-%d")
    html_prac = build_html_practitioner(df, df_ai)
    write_outputs(df, html_prac)
    send_mail_to(RECIPIENTS, f"관세·통상 데일리 브리프 ({today})", html_prac)

    html_exec = build_html_exec(df_ai)
    send_mail_to(RECIPIENTS_EXEC, f"[Executive] 관세·통상 핵심 TOP3 ({today})", html_exec)

    print("✅ STABLE 완료 (실무/임원 발송)")

if __name__ == "__main__":
    main()
