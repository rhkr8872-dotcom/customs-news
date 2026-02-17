# -*- coding: utf-8 -*-
"""
Customs & Trade Daily Brief - v2 STRATEGIC
- v1 STABLE + (중요도/국가/기관/삼성연관성) 고도화
- 임원 인사이트/Action을 실무자 메일에도 동일 반영
- Gemini API(선택)로 요약/당사영향/Action 강화 (없으면 룰 기반)
"""

import os, re, html, smtplib, hashlib, json
import datetime as dt
from typing import List, Dict, Tuple, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from difflib import SequenceMatcher

import pandas as pd
import feedparser
import urllib.parse

try:
    from dateutil import parser as dtparser
except Exception:
    dtparser = None

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

NEWS_LIMIT_PER_FEED = int(os.getenv("NEWS_LIMIT_PER_FEED", "30"))
DEDUP_SIM_THRESHOLD = float(os.getenv("DEDUP_SIM_THRESHOLD", "0.92"))

# Gemini (선택)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash").strip()

# 다국어
LOCALES = [
    {"hl": "ko", "gl": "KR", "ceid": "KR:ko", "lang": "ko"},
    {"hl": "en", "gl": "US", "ceid": "US:en", "lang": "en"},
    {"hl": "fr", "gl": "FR", "ceid": "FR:fr", "lang": "fr"},
]

# ===============================
# TIME WINDOW (KST 07~07)
# ===============================
def now_kst() -> dt.datetime:
    return dt.datetime.utcnow() + dt.timedelta(hours=9)

def window_07_to_07_kst(ref: Optional[dt.datetime] = None) -> Tuple[dt.datetime, dt.datetime]:
    ref = ref or now_kst()
    end = ref.replace(hour=7, minute=0, second=0, microsecond=0)
    if ref < end:
        end = end - dt.timedelta(days=1)
    start = end - dt.timedelta(days=1)
    return start, end

def parse_published(published: str) -> Optional[dt.datetime]:
    if not published or not dtparser:
        return None
    try:
        d = dtparser.parse(published)
        if d.tzinfo is not None:
            d_utc = d.astimezone(dt.timezone.utc).replace(tzinfo=None)
            return d_utc + dt.timedelta(hours=9)
        return d
    except Exception:
        return None

# ===============================
# INPUT: KEYWORDS
# ===============================
def load_keywords() -> List[str]:
    """
    Tight loader for keywords (제시어)
    Priority:
      1) custom_queries.TXT
      2) sites.xlsx (strict sheet/column rules)
      3) fallback ["관세"]

    Excel rule (sites.xlsx):
      - Prefer specific sheets: sites/site/list/source/config (case-insensitive contains)
      - Keyword columns allowed: 제시어, 검색어, 키워드, query, keyword, term
      - Optional enable columns allowed: 사용, 활성, enable, enabled, active, use
      - If enable col exists => only truthy rows are used
      - Split cell values by [comma, semicolon, pipe, newline] into multiple keywords
    """
    import os
    import re
    import pandas as pd

    def _dedup(seq: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    def _clean(s: str) -> str:
        s = (s or "").strip()
        s = re.sub(r"\s+", " ", s)
        return s

    def _split_keywords(cell: str) -> List[str]:
        cell = _clean(cell)
        if not cell:
            return []
        # 관세,관세율; tariff | section 301  \n 등 분해
        parts = re.split(r"[,;|\n]+", cell)
        parts = [_clean(p) for p in parts]
        return [p for p in parts if p]

    def _is_truthy(v) -> bool:
        if v is None:
            return False
        if isinstance(v, (int, float)):
            return v == 1
        s = str(v).strip().lower()
        return s in {"1", "y", "yes", "true", "t", "on", "use", "enable", "enabled", "active"}

    # 1) TXT
    txt_path = os.path.join(os.path.dirname(__file__), "custom_queries.TXT")
    if os.path.exists(txt_path):
        kws = []
        with open(txt_path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                kws.extend(_split_keywords(s))
        kws = _dedup([k for k in kws if k])
        if kws:
            return kws

    # 2) XLSX (strict)
    xlsx_path = os.path.join(os.path.dirname(__file__), "sites.xlsx")
    if os.path.exists(xlsx_path):
        try:
            xl = pd.ExcelFile(xlsx_path)

            # ✅ 타이트: 시트 후보를 제한
            sheet_priority_keywords = ["sites", "site", "list", "source", "config", "setting", "master"]
            sheets = xl.sheet_names

            # 우선순위 시트 먼저
            preferred = []
            others = []
            for sh in sheets:
                key = sh.strip().lower()
                if any(k in key for k in sheet_priority_keywords):
                    preferred.append(sh)
                else:
                    others.append(sh)
            # preferred 먼저 보고, 없으면 others도 보되 "제시어 컬럼이 정확히 있는 시트만" 사용
            scan_order = preferred + others

            # ✅ 타이트: 컬럼 후보 제한 (여기 없는 컬럼은 제시어로 절대 안 봄)
            kw_cols = ["제시어", "검색어", "키워드", "query", "keyword", "term"]
            enable_cols = ["사용", "활성", "enable", "enabled", "active", "use"]

            all_kws = []
            for sh in scan_order:
                df = xl.parse(sh)

                # 컬럼명 trim
                df.columns = [str(c).strip() for c in df.columns]

                kw_col = next((c for c in kw_cols if c in df.columns), None)
                if not kw_col:
                    continue  # ✅ 타이트: 제시어 컬럼 없으면 해당 시트 무시

                en_col = next((c for c in enable_cols if c in df.columns), None)

                for _, row in df.iterrows():
                    if en_col is not None:
                        if not _is_truthy(row.get(en_col)):
                            continue
                    cell = row.get(kw_col)
                    if pd.isna(cell):
                        continue
                    all_kws.extend(_split_keywords(str(cell)))

                # ✅ 타이트: 첫 “유효 시트”에서 키워드를 찾으면 그 시트만 사용하고 종료
                # (여러 시트 섞이면 관리가 어려워지는 걸 방지)
                if all_kws:
                    break

            all_kws = _dedup([k for k in all_kws if k])
            if all_kws:
                return all_kws

        except Exception:
            pass

    return ["관세"]


# ===============================
# NORMALIZE + DEDUP
# ===============================
def normalize_title(t: str) -> str:
    t = (t or "").strip().lower()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"[\"'“”‘’]", "", t)
    t = re.sub(r"\s-\s.*$", "", t)
    t = re.sub(r"\|\s*.*$", "", t)
    return t.strip()

def normalize_url(u: str) -> str:
    u = (u or "").strip()
    try:
        if "news.google.com" in u and "url=" in u:
            qs = urllib.parse.urlparse(u).query
            params = urllib.parse.parse_qs(qs)
            if "url" in params:
                return params["url"][0]
    except Exception:
        pass
    return u

def hash_key(title: str, url: str) -> str:
    s = f"{normalize_title(title)}|{normalize_url(url)}"
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def dedup_rows(rows: List[Dict]) -> List[Dict]:
    seen = set()
    uniq = []
    for r in rows:
        k = hash_key(r.get("헤드라인",""), r.get("출처(URL)",""))
        if k in seen:
            continue
        seen.add(k)
        uniq.append(r)

    final = []
    titles = []
    for r in uniq:
        tn = normalize_title(r.get("헤드라인",""))
        dup = False
        for prev in titles:
            if tn and prev and SequenceMatcher(None, tn, prev).ratio() >= DEDUP_SIM_THRESHOLD:
                dup = True
                break
        if not dup:
            final.append(r)
            titles.append(tn)
    return final

def strip_html(s: str) -> str:
    s = s or ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# ===============================
# COUNTRY / AGENCY / IMPORTANCE / SAMSUNG RELEVANCE
# ===============================
SAMSUNG_PROD_COUNTRIES = ["Korea","China","Vietnam","India","Indonesia","Türkiye","Slovakia","Poland","Mexico","Brazil","Netherlands","USA","EU"]
PRODUCT_KEYWORDS = [
    "smartphone","mobile","tablet","smartwatch","earbuds",
    "tv","monitor","refrigerator","air conditioner","vacuum","soundbar","oven",
    "5g","base station","antenna","network equipment",
    "medical device","x-ray"
]
COMPETITORS = ["apple","lg","whirlpool","general electric","ge"]

COUNTRY_KEYWORDS = {
    "USA": ["u.s.", "united states", "america", "ustr", "cbp", "section 301", "section 232", "ieepa"],
    "EU": ["european union", "european commission", "eu commission", "dg trade"],
    "China": ["china", "prc", "moFCOM".lower()],
    "India": ["india"],
    "Vietnam": ["vietnam"],
    "Türkiye": ["turkey", "türkiye"],
    "Mexico": ["mexico"],
    "Brazil": ["brazil"],
    "Netherlands": ["netherlands", "dutch"],
}

AGENCY_PATTERNS = [
    ("USTR", ["ustr", "office of the u.s. trade representative"]),
    ("US CBP", ["cbp", "u.s. customs", "customs and border protection"]),
    ("US DOC", ["department of commerce", "commerce department", "bis"]),
    ("EU Commission", ["european commission", "eu commission"]),
    ("EU DG TRADE", ["dg trade"]),
    ("MOF", ["ministry of finance", "mof"]),
    ("Customs", ["customs", "세관", "관세청"]),
]

HIGH_TERMS = ["관세율","추가관세","tariff rate","tariff increase","duty rate","hs code","hs코드","section 301","section 232","ieepa","anti-dumping","countervailing","safeguard","무역구제"]
MID_TERMS  = ["export control","sanction","entity list","fta","origin","원산지","통관","customs","규정","개정","고시","시행"]

def detect_country(text: str) -> str:
    t = (text or "").lower()
    for c, keys in COUNTRY_KEYWORDS.items():
        if any(k in t for k in keys):
            return c
    return ""

def detect_agency(text: str) -> str:
    t = (text or "").lower()
    hits = []
    for name, keys in AGENCY_PATTERNS:
        if any(k in t for k in keys):
            hits.append(name)
    return ", ".join(sorted(set(hits)))

def detect_importance(title: str, summary: str) -> str:
    t = f"{title} {summary}".lower()
    if any(k.lower() in t for k in HIGH_TERMS):
        return "상"
    if any(k.lower() in t for k in MID_TERMS):
        return "중"
    return "하"

def samsung_relevance_score(title: str, summary: str, country: str) -> int:
    t = f"{title} {summary}".lower()
    score = 0
    # 국가
    if country and country in SAMSUNG_PROD_COUNTRIES:
        score += 4
    # 제품군
    if any(k in t for k in PRODUCT_KEYWORDS):
        score += 3
    # 경쟁사
    if any(k in t for k in COMPETITORS):
        score += 2
    # 관세/법령 강키워드
    strong = ["section 301","section 232","ieepa","tariff","duty","관세","관세율","hs"]
    if any(k in t for k in strong):
        score += 4
    return score

# ===============================
# GEMINI (선택 사용)
# - 키 없으면 룰 기반으로 대체
# ===============================
def gemini_generate_insight(items: List[Dict]) -> Optional[Dict]:
    """
    items: top3 dict list
    return: {"summary": "...", "actions": ["..",".."], "why": "..."} or None
    """
    if not GEMINI_API_KEY:
        return None

    # ⚠️ 실제 호출은 사내 환경/정책에 따라 막힐 수 있음.
    # 여기서는 "구조"만 제공합니다. (GitHub Actions에서 외부 호출 허용 시에만 동작)
    # 구현 원하면, 사용 중인 Gemini 호출 코드(현재 사용 방식)를 그대로 이 함수에 붙이면 됩니다.
    return None

def fallback_exec_insight(top3: List[Dict]) -> Dict:
    # 룰 기반 Executive Insight
    bullets = []
    for r in top3:
        bullets.append(f"- {r.get('대상 국가','')} / {r.get('중요도','')} : 관세/통상 변화 가능 → 원가·마진·리드타임 영향 검토")
    return {
        "why": "관세율/무역구제/수출통제 등은 수입원가·판매가·공급망 리드타임에 직접 영향. 생산/판매법인 기준 조기 스크리닝 필요.",
        "actions": [
            "1) 대상국/품목(HS) 및 적용 시점 확인",
            "2) 생산법인→판매법인 순으로 원가/마진/리드타임 1차 영향 산정",
            "3) 필요 시 대체 소싱/가격/FTA 적용/무역구제 대응 착수"
        ],
        "bullets": bullets
    }

# ===============================
# RSS FETCH
# ===============================
def build_rss_url(query: str, locale: Dict) -> str:
    return "https://news.google.com/rss/search?" + urllib.parse.urlencode({
        "q": query,
        "hl": locale["hl"],
        "gl": locale["gl"],
        "ceid": locale["ceid"],
    })

def fetch_for_keyword(query: str, locale: Dict, wstart: dt.datetime, wend: dt.datetime) -> List[Dict]:
    rss = build_rss_url(query, locale)
    feed = feedparser.parse(rss)

    rows = []
    for e in feed.entries[:NEWS_LIMIT_PER_FEED]:
        title = getattr(e, "title", "").strip()
        link = normalize_url(getattr(e, "link", "").strip())
        published = getattr(e, "published", "") or getattr(e, "updated", "")
        summary = strip_html(getattr(e, "summary", "") or getattr(e, "description", ""))

        pk = parse_published(published)
        if pk is not None and not (wstart <= pk < wend):
            continue

        country = detect_country(f"{title} {summary}")
        agency = detect_agency(f"{title} {summary}")
        importance = detect_importance(title, summary)
        rel = samsung_relevance_score(title, summary, country)

        rows.append({
            "제시어": query,
            "헤드라인": title,
            "주요내용": summary[:900],
            "발표일": (pk.strftime("%Y-%m-%d %H:%M") if pk else ""),
            "출처(URL)": link,
            "대상 국가": country,
            "관련 기관": agency,
            "중요도": importance,
            "비고": "",  # 실무자 메일 비고 내용은 비움(요구사항)
            "_lang": locale["lang"],
            "_rel": rel
        })
    return rows

def run_sensor_build_df() -> pd.DataFrame:
    kws = load_keywords()
    wstart, wend = window_07_to_07_kst()

    all_rows = []
    for kw in kws:
        for loc in LOCALES:
            all_rows.extend(fetch_for_keyword(kw, loc, wstart, wend))

    all_rows = dedup_rows(all_rows)
    df = pd.DataFrame(all_rows)
    if df.empty:
        return df

    # 삼성 연관성/중요도 우선 정렬
    df["_imp_rank"] = df["중요도"].map({"상":3, "중":2, "하":1}).fillna(0)
    df = df.sort_values(by=["_imp_rank","_rel","발표일"], ascending=False).drop(columns=["_imp_rank"], errors="ignore")
    return df

# ===============================
# HTML
# ===============================
STYLE = """
<style>
body{font-family:Malgun Gothic,Arial; background:#f6f6f6;}
.page{max-width:1120px;margin:auto;background:white;padding:14px;}
h2{margin-bottom:4px;}
.box{border:1px solid #ddd;border-radius:8px;padding:12px;margin:12px 0;}
table{border-collapse:collapse;width:100%;}
th,td{border:1px solid #ccc;padding:8px;font-size:12px;vertical-align:top;}
th{background:#f0f0f0;}
.small{font-size:11px;color:#555;}
</style>
"""

def build_table_html(df: pd.DataFrame, for_practitioner: bool = True) -> str:
    for c in ["제시어","헤드라인","주요내용","발표일","대상 국가","관련 기관","중요도","비고","출처(URL)"]:
        if c not in df.columns:
            df[c] = ""

    rows_html = ""
    for _, r in df.iterrows():
        link = r.get("출처(URL)", "") or "#"
        headline = html.escape(str(r.get("헤드라인","")))
        content = html.escape(str(r.get("주요내용","")))
        combo = f'<a href="{html.escape(link)}" target="_blank">{headline}</a><br/><div class="small">{content}</div>'

        note = "" if for_practitioner else html.escape(str(r.get("비고","")))

        rows_html += f"""
        <tr>
          <td>{html.escape(str(r.get("제시어","")))}</td>
          <td>{combo}</td>
          <td>{html.escape(str(r.get("발표일","")))}</td>
          <td>{html.escape(str(r.get("대상 국가","")))}</td>
          <td>{html.escape(str(r.get("관련 기관","")))}</td>
          <td>{html.escape(str(r.get("중요도","")))}</td>
          <td>{note}</td>
        </tr>
        """

    return f"""
    <table>
      <tr>
        <th>제시어</th>
        <th>헤드라인 / 주요내용</th>
        <th>발표일</th>
        <th>대상 국가</th>
        <th>관련 기관</th>
        <th>중요도</th>
        <th>비고</th>
      </tr>
      {rows_html}
    </table>
    """

def pick_top3(df: pd.DataFrame) -> pd.DataFrame:
    # 상/중 우선 + 삼성연관성(_rel) 우선
    tmp = df.copy()
    tmp["_imp_rank"] = tmp["중요도"].map({"상":3, "중":2, "하":1}).fillna(0)
    tmp["_rel2"] = tmp.get("_rel", 0)
    tmp = tmp.sort_values(by=["_imp_rank","_rel2","발표일"], ascending=False).head(3)
    return tmp.drop(columns=["_imp_rank","_rel2"], errors="ignore")

def build_exec_section(top3: pd.DataFrame) -> Tuple[str, Dict]:
    items = top3.to_dict(orient="records")
    insight = gemini_generate_insight(items) or fallback_exec_insight(items)

    li = ""
    for r in items:
        li += f"""
        <li>
          <b>[{html.escape(str(r.get("대상 국가","")))} | {html.escape(str(r.get("관련 기관","")))} | 중요도 {html.escape(str(r.get("중요도","")))}]</b><br/>
          <a href="{html.escape(str(r.get("출처(URL)","") or "#"))}" target="_blank">{html.escape(str(r.get("헤드라인","")))}</a><br/>
          <div class="small">{html.escape(str(r.get("주요내용",""))[:240])}</div>
        </li>
        """

    actions_html = "<br/>".join([html.escape(a) for a in insight["actions"]])

    exec_html = f"""
    <div class="box">
      <h3>① Executive Insight TOP3</h3>
      <ul>{li}</ul>
    </div>
    <div class="box">
      <h3>② 왜 중요한가</h3>
      <div class="small">{html.escape(insight["why"])}</div>
    </div>
    <div class="box">
      <h3>③ Action</h3>
      <div class="small">{actions_html}</div>
    </div>
    """
    return exec_html, insight

def build_html_practitioner(df: pd.DataFrame) -> str:
    date = now_kst().strftime("%Y-%m-%d")
    wstart, wend = window_07_to_07_kst()

    top3 = pick_top3(df)
    exec_block, _ = build_exec_section(top3)

    table = build_table_html(df, for_practitioner=True)

    return f"""
    <html><head>{STYLE}</head><body>
    <div class="page">
      <h2>관세·무역 뉴스 브리핑 (실무) ({date})</h2>
      <div class="small">수집창(KST): {wstart.strftime("%Y-%m-%d %H:%M")} ~ {wend.strftime("%Y-%m-%d %H:%M")}</div>

      {exec_block}

      <div class="box">
        <h3>④ 정책 이벤트 표</h3>
        {table}
      </div>
    </div>
    </body></html>
    """

def build_html_exec(df: pd.DataFrame) -> str:
    date = now_kst().strftime("%Y-%m-%d")
    top3 = pick_top3(df)
    exec_block, _ = build_exec_section(top3)

    # 임원용은 표는 최소(Top10)
    table = build_table_html(df.head(10), for_practitioner=False)

    return f"""
    <html><head>{STYLE}</head><body>
    <div class="page">
      <h2>[Executive] 관세·통상 핵심 브리프 ({date})</h2>
      {exec_block}
      <div class="box">
        <h3>④ 참고(Top 10)</h3>
        {table}
      </div>
    </div>
    </body></html>
    """

# ===============================
# OUTPUTS
# ===============================
def write_outputs(df: pd.DataFrame, html_body: str) -> Tuple[str,str,str]:
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
def send_mail_to(recipients: List[str], subject: str, html_body: str) -> None:
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
    df = run_sensor_build_df()
    if df is None or df.empty:
        print("수집 결과 없음")
        return

    today = now_kst().strftime("%Y-%m-%d")

    # 실무자(임원 인사이트 섹션 포함 + 전체 표 유지)
    html_prac = build_html_practitioner(df)
    write_outputs(df, html_prac)
    send_mail_to(RECIPIENTS, f"관세·무역 뉴스 브리핑 ({today})", html_prac)

    # 임원용
    html_exec = build_html_exec(df)
    send_mail_to(RECIPIENTS_EXEC, f"[Executive] 관세·통상 핵심 브리프 ({today})", html_exec)

    print("✅ v2 STRATEGIC 완료")
    print("BASE_DIR =", BASE_DIR)
    print("OUT_FILES =", os.listdir(BASE_DIR))

if __name__ == "__main__":
    main()
