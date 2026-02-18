# -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief (GitHub Actions STABLE)

Goals (STABLE):
- Read queries from custom_queries.TXT  (one per line)
- Read official / approved sites from sites.xlsx (sheet "SiteList", columns: name, url)
- Collect Google News RSS within time window (KST 07:00~07:00; send at 08:00 via workflow schedule)
- Deduplicate
- Executive mail: TOP3 + (② Why / ③ Checkpoint)  (same content as practitioner, WITHOUT the big table)
- Practitioner mail: same as executive + ④ Policy Event Table (tight A4 landscape)
- Gemini optional: Korean 2~3 sentence summary (if GEMINI_ENABLED=1 and GEMINI_API_KEY exists)
  Fallback: RSS snippet 3 lines
"""

import os, re, html, smtplib, traceback
import datetime as dt
from typing import List, Dict, Tuple, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urlparse, urlencode

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

BASE_DIR = os.getenv("BASE_DIR", os.path.join(os.path.dirname(__file__), "out"))
CUSTOM_QUERIES_FILE = os.getenv("CUSTOM_QUERIES_FILE", os.path.join(os.path.dirname(__file__), "custom_queries.TXT"))
SITES_FILE = os.getenv("SITES_FILE", os.path.join(os.path.dirname(__file__), "sites.xlsx"))

WINDOW_START_HOUR_KST = int(os.getenv("WINDOW_START_HOUR_KST", "7"))  # 07:00~07:00
MAX_ITEMS_PER_QUERY = int(os.getenv("MAX_ITEMS_PER_QUERY", "30"))
MAX_TABLE_PER_KEYWORD = int(os.getenv("MAX_TABLE_PER_KEYWORD", "10"))

GEMINI_ENABLED = os.getenv("GEMINI_ENABLED", "0").strip() in ("1", "true", "True", "YES", "yes")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")

os.makedirs(BASE_DIR, exist_ok=True)

# ===============================
# TIME
# ===============================
def now_kst() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).astimezone(dt.timezone(dt.timedelta(hours=9)))

def window_kst() -> Tuple[dt.datetime, dt.datetime]:
    """Return (start,end) KST for last window ending at today's WINDOW_START_HOUR_KST."""
    now = now_kst()
    end = now.replace(hour=WINDOW_START_HOUR_KST, minute=0, second=0, microsecond=0)
    if now < end:
        end -= dt.timedelta(days=1)
    start = end - dt.timedelta(days=1)
    return start, end

# ===============================
# LOADERS
# ===============================
def load_queries_txt(path: str) -> List[str]:
    if not os.path.exists(path):
        return ["관세"]
    out = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            q = line.strip()
            if not q:
                continue
            if q.startswith("#"):
                continue
            out.append(q)
    # de-dup preserve order
    seen=set(); uniq=[]
    for q in out:
        if q.lower() in seen: 
            continue
        seen.add(q.lower())
        uniq.append(q)
    return uniq or ["관세"]

def _normalize_url(u: str) -> str:
    if not u:
        return ""
    u = str(u).strip()
    if not u:
        return ""
    # sometimes excel has non-string -> str already
    if not re.match(r"^https?://", u, flags=re.I):
        u = "https://" + u
    return u

def _domain(u: str) -> str:
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ""

def load_sites_xlsx(path: str) -> Tuple[Dict[str,str], set]:
    """Return domain_to_name, allowed_domains."""
    if not os.path.exists(path):
        return {}, set()

    xls = pd.ExcelFile(path)
    # accept common sheet names
    sheet = None
    for cand in ["SiteList", "sites", "Sites", "Sheet1"]:
        if cand in xls.sheet_names:
            sheet = cand
            break
    if sheet is None:
        sheet = xls.sheet_names[0]

    df = pd.read_excel(path, sheet_name=sheet)
    df.columns = [str(c).strip().lower() for c in df.columns]

    # required columns: name, url (case-insensitive)
    if "name" not in df.columns:
        # try korean
        for c in df.columns:
            if "name" in c or "기관" in c or "사이트" in c:
                df.rename(columns={c: "name"}, inplace=True)
                break
    if "url" not in df.columns:
        for c in df.columns:
            if "url" in c or "link" in c:
                df.rename(columns={c: "url"}, inplace=True)
                break

    if "name" not in df.columns or "url" not in df.columns:
        raise ValueError(f"sites.xlsx must contain columns name,url (found: {list(df.columns)})")

    domain_to_name: Dict[str,str] = {}
    allowed_domains: set = set()
    for _, r in df.iterrows():
        name = str(r.get("name","") or "").strip()
        url = _normalize_url(r.get("url",""))
        dom = _domain(url)
        if not dom:
            continue
        if dom not in domain_to_name:
            domain_to_name[dom] = name or dom
        allowed_domains.add(dom)
    return domain_to_name, allowed_domains

# ===============================
# RSS FETCH
# ===============================
LANG_PACK = [
    # (lang_tag, extra_terms)
    ("ko", ""),   # Korean
    ("en", " tariff OR customs OR trade OR \"Section 232\" OR \"Section 301\" OR IEEPA OR \"Tariff Act\" OR \"trade expansion act\""),
    ("fr", " tarif OR douane OR commerce OR \"section 232\" OR \"section 301\" OR sanctions"),
    ("es", " arancel OR aduana OR comercio OR \"section 232\" OR \"section 301\" OR sanciones"),
]

def build_rss_url(query: str, lang: str) -> str:
    # Google News RSS search endpoint (language affects rendering)
    extra = ""
    for tag, terms in LANG_PACK:
        if tag == lang:
            extra = terms
            break
    q = query
    if extra:
        q = f'({query}) {extra}'
    params = {
        "q": q,
        "hl": f"{lang}",
        "gl": "KR" if lang=="ko" else "US",
        "ceid": f"KR:ko" if lang=="ko" else "US:en",
    }
    return "https://news.google.com/rss/search?" + urlencode(params)

def strip_html(s: str) -> str:
    s = s or ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_published(e) -> str:
    # keep original string; we will store as is (actions user asked to avoid wrong dates)
    p = getattr(e, "published", "") or getattr(e, "updated", "") or ""
    return str(p)

def fetch_news(queries: List[str]) -> pd.DataFrame:
    rows = []
    for q in queries:
        for lang in ["ko","en","fr","es"]:
            rss = build_rss_url(q, lang)
            feed = feedparser.parse(rss)
            for e in feed.entries[:MAX_ITEMS_PER_QUERY]:
                title = str(getattr(e,"title","") or "").strip()
                link = str(getattr(e,"link","") or "").strip()
                summ = strip_html(str(getattr(e,"summary","") or ""))
                published = parse_published(e)
                rows.append({
                    "keyword": q,
                    "lang": lang,
                    "title": title,
                    "link": link,
                    "summary": summ[:1200],
                    "published": published,
                })
    df = pd.DataFrame(rows)
    return df

# ===============================
# SCORING / FILTERING
# ===============================
# hard triggers -> always show
MUST_SHOW = [
    "tariff act", "trade expansion act", "international emergency economic powers act",
    "section 232", "section 301", "ieepa", "관세", "관세율"
]
TRADE_TERMS = [
    "tariff","duty","customs","trade","import","export","hs","harmonized","origin","fta",
    "sanction","export control","antidumping","anti-dumping","countervailing","safeguard",
    "관세","통관","수입","수출","hs","원산지","fta","제재","수출통제","반덤핑","세이프가드"
]
NON_RELEVANT = [
    # keep small but effective
    "wine","wines","vino","vin","whisky","soccer","football","baseball","nba","k-pop","entertainment","celebrity",
    "레시피","맛집","와인","축구","야구","연예"
]

PRODUCT_HINTS = [
    "smartphone","mobile","phone","tablet","tv","monitor","refrigerator","air conditioner","network","5g","medical","x-ray",
    "휴대폰","스마트폰","태블릿","tv","모니터","냉장고","에어컨","네트워크","5g","의료","엑스레이"
]

PROD_COUNTRIES = ["korea","china","vietnam","india","indonesia","turkey","türkiye","slovakia","poland","mexico","brazil",
                  "한국","중국","베트남","인도","인도네시아","터키","슬로바키아","폴란드","멕시코","브라질"]

def norm_title(t: str) -> str:
    t = (t or "").lower().strip()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"[\W_]+", " ", t)
    return t.strip()

def is_trade_related(title: str, summary: str) -> bool:
    blob = f"{title} {summary}".lower()
    if any(x in blob for x in MUST_SHOW):
        return True
    if any(x in blob for x in NON_RELEVANT):
        # allow if MUST_SHOW hit
        return False
    return any(x in blob for x in TRADE_TERMS)

def policy_score(title: str, summary: str, allowed_domains: set, link: str) -> int:
    blob = f"{title} {summary}".lower()
    score = 1
    # high-impact
    for kw,w in [
        ("section 301",7),("section 232",7),("ieepa",7),
        ("tariff act",7),("trade expansion act",7),("international emergency economic powers act",7),
        ("export control",6),("sanction",6),("entity list",5),
        ("anti-dumping",5),("antidumping",5),("countervailing",5),("safeguard",5),
        ("관세율",6),("추가관세",6),("관세",5),("tariff",5),("duty",4),
        ("hs",3),("harmonized",3),("원산지",3),("fta",3),("통관",3),("customs",3),
    ]:
        if kw in blob:
            score += w
    # product/country hints
    if any(k in blob for k in PRODUCT_HINTS):
        score += 2
    if any(c in blob for c in PROD_COUNTRIES):
        score += 1
    # preferred sources bonus (official/approved list)
    dom = _domain(link)
    if dom and dom in allowed_domains:
        score += 3
    return min(score, 20)

def dedup(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["t_norm"] = df["title"].map(norm_title)
    df["dom"] = df["link"].map(_domain)
    # key: title norm + domain
    df["dedup_key"] = df["t_norm"] + "|" + df["dom"]
    df = df.drop_duplicates(subset=["dedup_key"], keep="first")
    return df

# ===============================
# GEMINI SUMMARY (OPTIONAL)
# ===============================
def fallback_3lines(title: str, summary: str) -> str:
    s = strip_html(summary or "")
    if not s or norm_title(s) == norm_title(title):
        s = strip_html(title or "")
    if not s:
        return ""
    parts = re.split(r"(?<=[\.\!\?。！？])\s+|\s*\n\s*", s)
    parts = [p.strip() for p in parts if p.strip()]
    return "<br/>".join(parts[:3])[:900]

def gemini_client():
    import google.generativeai as genai  # type: ignore
    genai.configure(api_key=GEMINI_API_KEY)
    return genai.GenerativeModel(GEMINI_MODEL)

def ensure_korean_summaries(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "summary_ko" not in df.columns:
        df["summary_ko"] = ""

    needs = []
    for _, r in df.iterrows():
        cur = str(r.get("summary_ko","") or "").strip()
        title = str(r.get("title","") or "")
        if (not cur) or (norm_title(cur) == norm_title(title)) or (len(cur) < 40):
            needs.append(True)
        else:
            needs.append(False)

    use_gemini = bool(GEMINI_ENABLED and GEMINI_API_KEY)
    if not use_gemini:
        df.loc[needs, "summary_ko"] = [
            fallback_3lines(str(r.get("title","")), str(r.get("summary","")))
            for _, r in df.loc[needs].iterrows()
        ]
        return df

    # Gemini
    try:
        model = gemini_client()
    except Exception:
        df.loc[needs, "summary_ko"] = [
            fallback_3lines(str(r.get("title","")), str(r.get("summary","")))
            for _, r in df.loc[needs].iterrows()
        ]
        return df

    out = []
    for _, r in df.iterrows():
        title = str(r.get("title","") or "")
        summ = str(r.get("summary","") or "")
        url = str(r.get("link","") or "")
        if not ((not r.get("summary_ko","")) or (norm_title(str(r.get("summary_ko",""))) == norm_title(title)) or (len(str(r.get("summary_ko",""))) < 40)):
            out.append(str(r.get("summary_ko","")))
            continue
        prompt = (
            "아래 뉴스의 핵심 내용을 한국어로 2~3문장으로 요약해 주세요.\n"
            "- 관세/관세율/HS/232/301/IEEPA/수출통제/제재/원산지/FTA/통관 관련 포인트가 있으면 반드시 포함\n"
            "- 기업 관점에서 당사 영향(원가/마진/리드타임/공급망/수출입 리스크)을 1문장 포함\n"
            "- 불릿/번호 없이 문장으로\n\n"
            f"[제목] {title}\n"
            f"[RSS요약] {summ}\n"
            f"[URL] {url}\n"
        )
        try:
            resp = model.generate_content(prompt)
            text = (getattr(resp, "text", "") or "").strip()
            text = re.sub(r"\s+", " ", text)
            if not text:
                text = fallback_3lines(title, summ)
            out.append(html.escape(text).replace("\n","<br/>")[:1000])
        except Exception:
            out.append(fallback_3lines(title, summ))
    df["summary_ko"] = out
    return df

# ===============================
# TOP3 PICK + WHY/CHECKPOINT
# ===============================
def pick_top3(df: pd.DataFrame, allowed_domains: set) -> pd.DataFrame:
    df = df.copy()
    df = df[df.apply(lambda r: is_trade_related(str(r.get("title","")), str(r.get("summary",""))), axis=1)].copy()
    if df.empty:
        return df
    df["score"] = df.apply(lambda r: policy_score(str(r["title"]), str(r["summary"]), allowed_domains, str(r["link"])), axis=1)
    # exclude obvious non-relevant (e.g. wine) unless MUST_SHOW
    def _keep(r):
        blob = f"{r['title']} {r['summary']}".lower()
        if any(x in blob for x in NON_RELEVANT) and not any(x in blob for x in MUST_SHOW):
            return False
        return True
    df = df[df.apply(_keep, axis=1)].copy()
    df = df.sort_values(["score"], ascending=False).head(3)
    return df

def build_why_and_checkpoint(top3: pd.DataFrame) -> Tuple[str,str]:
    # de-dup by same country/product hint pattern
    seen=set()
    why_lines=[]
    chk_lines=[]
    for _, r in top3.iterrows():
        title = str(r.get("title",""))
        summ = strip_html(str(r.get("summary_ko","")) or str(r.get("summary","")))
        blob = f"{title} {summ}".lower()
        # infer impact tags
        impact=[]
        if any(k in blob for k in ["tariff","관세","관세율","duty","section 301","section 232","ieepa"]):
            impact.append("원가/마진")
        if any(k in blob for k in ["export control","수출통제","sanction","제재","entity list"]):
            impact.append("수출입 제한/거래차질")
        if any(k in blob for k in ["hs","harmonized","classification","품목분류"]):
            impact.append("HS/분류 리스크")
        if any(k in blob for k in ["origin","원산지","fta"]):
            impact.append("FTA/원산지 리스크")
        if not impact:
            impact=["리스크 모니터링"]

        key = "|".join(impact)
        if key in seen:
            continue
        seen.add(key)

        why_lines.append(f"• {', '.join(impact)} 관점에서 영향 가능 — 정책/집행 변화 시 비용·리드타임·컴플라이언스 리스크 확대")
        chk_lines.append(
            "• Action: ① 대상국/품목(HS)·적용시점 확인 → ② 생산/판매법인 영향(원가·마진·리드타임) 1차 산정 → ③ 필요 시 HQ 대응(관세전략/FTA/제재·수출통제) 착수"
        )
    return "<br/>".join(why_lines) or "• (TOP3가 충분히 산출되지 않아 금일은 모니터링만 수행)", "<br/>".join(chk_lines) or "• (TOP3가 충분히 산출되지 않아 금일은 모니터링만 수행)"

# ===============================
# TABLE BUILD (A4 Landscape)
# ===============================
STYLE = """
<style>
@page { size: A4 landscape; margin: 10mm; }
body{font-family:Malgun Gothic,Arial; background:#f6f6f6; margin:0;}
.page{max-width:297mm;margin:auto;background:white;padding:10mm;}
h2{margin:0 0 6px 0;}
.box{border:1px solid #ddd;border-radius:10px;padding:10px;margin:10px 0;}
.small{font-size:11px;color:#555;}
table{border-collapse:collapse;width:100%; table-layout:fixed;}
th,td{border:1px solid #ccc;padding:6px;font-size:11px;vertical-align:top;word-wrap:break-word;}
th{background:#f0f0f0;}
.col-k{width:10%;}
.col-c{width:8%;}
.col-d{width:14%;}
.col-a{width:12%;}
.col-s{width:56%;}
</style>
"""

def org_from_domain(domain_to_name: Dict[str,str], link: str) -> str:
    dom = _domain(link)
    return domain_to_name.get(dom, "")

def keyword_counts_line(df: pd.DataFrame) -> str:
    vc = df["keyword"].value_counts()
    parts = [f"{k} {int(v)}건" for k,v in vc.items()]
    return ", ".join(parts)

def build_table(df: pd.DataFrame, domain_to_name: Dict[str,str]) -> str:
    # sort: keyword asc, importance asc (상->중->하) so map
    imp_rank = {"상":0, "중":1, "하":2}
    df = df.copy()
    df["importance"] = df.get("importance", "중")
    df["imp_r"] = df["importance"].map(imp_rank).fillna(1).astype(int)

    # per keyword keep top N after dedup
    out_rows=[]
    for kw, g in df.groupby("keyword", sort=True):
        g = g.sort_values(["score","published"], ascending=[False, False]).head(MAX_TABLE_PER_KEYWORD)
        out_rows.append(g)
    df2 = pd.concat(out_rows, ignore_index=True) if out_rows else df.head(0)

    df2 = df2.sort_values(["keyword","imp_r","score"], ascending=[True,True,False])

    rows_html=""
    for _, r in df2.iterrows():
        kw = html.escape(str(r.get("keyword","")))
        imp = html.escape(str(r.get("importance","중")))
        date = html.escape(str(r.get("published","")))
        org = html.escape(org_from_domain(domain_to_name, str(r.get("link",""))))
        title = html.escape(str(r.get("title","")))
        summ = str(r.get("summary_ko","") or "")
        if not summ:
            summ = fallback_3lines(str(r.get("title","")), str(r.get("summary","")))
        link = html.escape(str(r.get("link","")))
        cell = f'<a href="{link}" target="_blank">{title}</a><br/><span class="small">{summ}</span>'
        rows_html += f"""
        <tr>
          <td class="col-k">{kw}</td>
          <td class="col-c">{imp}</td>
          <td class="col-d">{date}</td>
          <td class="col-a">{org}</td>
          <td class="col-s">{cell}</td>
        </tr>
        """
    return f"""
    <div class="box">
      <h3>④ 정책 이벤트 표</h3>
      <div class="small">{html.escape(keyword_counts_line(df2) if not df2.empty else "표시할 항목이 없습니다")}</div>
      <table>
        <tr>
          <th class="col-k">제시어</th>
          <th class="col-c">중요도</th>
          <th class="col-d">발표일</th>
          <th class="col-a">관련기관</th>
          <th class="col-s">헤드라인 / 주요내용</th>
        </tr>
        {rows_html}
      </table>
    </div>
    """

# ===============================
# HTML BUILD (shared blocks)
# ===============================
def build_top3_html(top3: pd.DataFrame) -> str:
    if top3.empty:
        return "<div class='small'>TOP3 산출 없음 (금일 수집 결과가 조건에 부합하지 않음)</div>"
    items=""
    for _, r in top3.iterrows():
        title = html.escape(str(r.get("title","")))
        link = html.escape(str(r.get("link","")))
        summ = str(r.get("summary_ko","") or "")
        if not summ:
            summ = fallback_3lines(str(r.get("title","")), str(r.get("summary","")))
        items += f"""
        <li>
          <a href="{link}" target="_blank"><b>{title}</b></a><br/>
          <span class="small">{summ}</span>
        </li>
        """
    return f"<ul>{items}</ul>"

def build_html_common(top3: pd.DataFrame, why_html: str, chk_html: str, title_prefix: str) -> str:
    date = now_kst().strftime("%Y-%m-%d")
    start, end = window_kst()
    win = f"{start.strftime('%m/%d %H:%M')}~{end.strftime('%m/%d %H:%M')} (KST)"
    return f"""
    <html><head>{STYLE}</head>
    <body><div class="page">
      <h2>{title_prefix} 관세·통상 데일리 브리프 ({date})</h2>
      <div class="small">수집 구간: {win} / 제시어: {html.escape(', '.join(top3['keyword'].unique().tolist()) if not top3.empty else '')}</div>

      <div class="box">
        <h3>① 관세·통상 핵심 TOP3</h3>
        {build_top3_html(top3)}
      </div>

      <div class="box">
        <h3>② 왜 중요한가 (TOP3 기반)</h3>
        <div class="small">{why_html}</div>
      </div>

      <div class="box">
        <h3>③ 당사 관점 체크포인트 (TOP3 기반)</h3>
        <div class="small">{chk_html}</div>
      </div>
    </div></body></html>
    """

def build_html_exec(top3: pd.DataFrame, why_html: str, chk_html: str) -> str:
    return build_html_common(top3, why_html, chk_html, "[Executive]")

def build_html_practitioner(top3: pd.DataFrame, why_html: str, chk_html: str, table_html: str) -> str:
    base = build_html_common(top3, why_html, chk_html, "")
    # insert table before closing
    return base.replace("</div></body></html>", table_html + "</div></body></html>")

# ===============================
# OUTPUTS + MAIL
# ===============================
def write_outputs(df: pd.DataFrame, html_body: str) -> Tuple[str,str,str]:
    today = now_kst().strftime("%Y-%m-%d")
    csv_path  = os.path.join(BASE_DIR, f"policy_events_{today}.csv")
    xlsx_path = os.path.join(BASE_DIR, f"policy_events_{today}.xlsx")
    html_path = os.path.join(BASE_DIR, f"policy_events_{today}.html")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_body)
    return csv_path, xlsx_path, html_path

def send_mail_to(recipients: List[str], subject: str, html_body: str) -> None:
    if not recipients:
        print("[WARN] recipients empty -> skip sending:", subject)
        return
    if not (SMTP_SERVER and SMTP_EMAIL and SMTP_PASSWORD):
        print("[WARN] SMTP env missing -> skip sending")
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
    print("BASE_DIR =", BASE_DIR)
    print("CUSTOM_QUERIES_FILE =", CUSTOM_QUERIES_FILE)
    print("SITES_FILE =", SITES_FILE)
    print("GEMINI_ENABLED =", GEMINI_ENABLED)
    start, end = window_kst()
    print("WINDOW_KST =", start, "~", end)

    queries = load_queries_txt(CUSTOM_QUERIES_FILE)
    domain_to_name, allowed_domains = load_sites_xlsx(SITES_FILE)
    print(f"[INFO] sites.xlsx loaded: domains={len(allowed_domains)}")

    df = fetch_news(queries)
    if df.empty:
        print("No RSS entries.")
        return

    # trade filter first
    df = df[df.apply(lambda r: is_trade_related(str(r["title"]), str(r["summary"])), axis=1)].copy()
    if df.empty:
        print("No trade-related entries after filter.")
        return

    df = dedup(df)
    df["score"] = df.apply(lambda r: policy_score(str(r["title"]), str(r["summary"]), allowed_domains, str(r["link"])), axis=1)

    # importance mapping
    df["importance"] = df["score"].apply(lambda s: "상" if s>=15 else ("중" if s>=8 else "하"))

    # summaries
    df = ensure_korean_summaries(df)

    # top3
    top3 = pick_top3(df, allowed_domains)
    why_html, chk_html = build_why_and_checkpoint(top3)

    # emails
    today = now_kst().strftime("%Y-%m-%d")
    exec_html = build_html_exec(top3, why_html, chk_html)

    table_html = build_table(df, domain_to_name)
    prac_html = build_html_practitioner(top3, why_html, chk_html, table_html)

    # outputs: store practitioner HTML as file
    write_outputs(df, prac_html)

    # send (exec/prac content same except table)
    send_mail_to(RECIPIENTS_EXEC, f"[Executive] 관세·통상 데일리 브리프 ({today})", exec_html)
    send_mail_to(RECIPIENTS,      f"관세·통상 데일리 브리프 ({today})", prac_html)

    print("✅ DONE")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        raise
