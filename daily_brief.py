# -*- coding: utf-8 -*-
"""
Samsung Electronics | Customs & Trade Daily Brief
vNext FINAL (E2E: Sensor + Outputs + Mail)

핵심 개선
1) Executive 메일 미발송 방지: RECIPIENTS_EXEC 누락 시 경고 + (옵션) fallback
2) Google RSS '헤드라인=요약' 문제 완화: 정제 + (가능 시) og:description 보강
3) 정책 이벤트 표 개편:
   - '출처(URL)' 칼럼 삭제
   - '헤드라인'에 링크 부여
   - '헤드라인+주요내용' 한 칸에 표기
4) 임원용 Insight(Trigger/Exposure/Action)을 실무자 메일에도 동일 표기
5) 수집 범위: 전일 07:00 ~ 당일 07:00 (KST)
   발송 시간(08:00 KST)은 GitHub Actions cron에서 설정

ENV (GitHub Secrets 권장)
- SMTP_SERVER, SMTP_PORT, SMTP_EMAIL, SMTP_PASSWORD
- RECIPIENTS          : 실무자 수신자(콤마)
- RECIPIENTS_EXEC     : 임원 수신자(콤마)
- EXEC_FALLBACK=1     : RECIPIENTS_EXEC 비어있을 때 RECIPIENTS로 임원메일도 발송(옵션)
- NEWS_QUERY          : 기본 "관세"
- BASE_DIR            : 기본 ./out
"""

import os, re, html, smtplib
import datetime as dt
from typing import Optional, Tuple, List
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import parsedate_to_datetime

import pandas as pd
import feedparser
import urllib.parse

# 선택: 요약 보강용(가능하면 더 좋아짐, 없으면 graceful fallback)
try:
    import requests
    from bs4 import BeautifulSoup
    HAS_WEB = True
except Exception:
    HAS_WEB = False

# ===============================
# ENV
# ===============================
SMTP_SERVER   = os.getenv("SMTP_SERVER")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_EMAIL    = os.getenv("SMTP_EMAIL")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

RECIPIENTS = [x.strip() for x in os.getenv("RECIPIENTS", "").split(",") if x.strip()]
RECIPIENTS_EXEC = [x.strip() for x in os.getenv("RECIPIENTS_EXEC", "").split(",") if x.strip()]
EXEC_FALLBACK = os.getenv("EXEC_FALLBACK", "0") == "1"

BASE_DIR = os.getenv("BASE_DIR", os.path.join(os.path.dirname(__file__), "out"))
os.makedirs(BASE_DIR, exist_ok=True)

NEWS_QUERY = os.getenv("NEWS_QUERY", "관세")

# ===============================
# TIME
# ===============================
def now_kst() -> dt.datetime:
    return dt.datetime.utcnow() + dt.timedelta(hours=9)

def window_07_to_07_kst(ref: Optional[dt.datetime] = None) -> Tuple[dt.datetime, dt.datetime]:
    """
    전일 07:00 ~ 당일 07:00 (KST)
    - 08시에 실행된다고 가정하면 end=오늘07:00이 맞음
    - 07시 이전에 실행되면 end를 하루 전으로 보정
    """
    now = ref or now_kst()
    end = now.replace(hour=7, minute=0, second=0, microsecond=0)
    if now < end:
        end = end - dt.timedelta(days=1)
    start = end - dt.timedelta(days=1)
    return start, end

def parse_pub_kst_naive(pub_str: str) -> Optional[dt.datetime]:
    if not pub_str:
        return None
    try:
        dtu = parsedate_to_datetime(pub_str)
        if dtu.tzinfo is None:
            dtu = dtu.replace(tzinfo=dt.timezone.utc)
        kst = dt.timezone(dt.timedelta(hours=9))
        return dtu.astimezone(kst).replace(tzinfo=None)
    except Exception:
        return None

def in_window(pub_str: str) -> bool:
    d = parse_pub_kst_naive(pub_str)
    if d is None:
        return True  # 날짜 없으면 포함(표에 날짜는 빈칸/원문 값 그대로)
    s, e = window_07_to_07_kst()
    return s <= d < e

# ===============================
# SUMMARY CLEAN & ENRICH
# ===============================
def strip_html(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def looks_like_title(title: str, summary: str) -> bool:
    t = (title or "").strip()
    s = (summary or "").strip()
    if not s:
        return True
    if s == t:
        return True
    if len(s) < 40:
        return True
    if t and s.startswith(t[: min(50, len(t))]):
        return True
    return False

def fetch_meta_desc(url: str, timeout: int = 8) -> str:
    if not (HAS_WEB and url and url.startswith("http")):
        return ""
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        og = soup.find("meta", attrs={"property": "og:description"})
        if og and og.get("content"):
            return strip_html(og.get("content"))
        md = soup.find("meta", attrs={"name": "description"})
        if md and md.get("content"):
            return strip_html(md.get("content"))
    except Exception:
        return ""
    return ""

def clean_summary(title: str, raw_summary: str, url: str = "") -> str:
    t = (title or "").strip()
    s = strip_html(raw_summary or "")

    # 제목 중복 제거 시도
    if t and s:
        if s == t:
            s = ""
        else:
            s2 = s.replace(t, "").strip(" -–—:|")
            if len(s2) >= 60:
                s = s2

    # 아직도 제목 같거나 빈약하면 meta description 보강 시도
    if looks_like_title(t, s):
        meta = fetch_meta_desc(url)
        if meta and not looks_like_title(t, meta):
            s = meta

    # 그래도 빈약하면 "거짓 요약" 대신 안내 문구(요구사항: 부정확 정보 금지)
    if len(s) < 40:
        s = "요약 정보가 제한적입니다. 원문 링크에서 세부 내용을 확인하세요."
    return s

# ===============================
# POLICY / RELEVANCE
# ===============================
# 반드시 보여줄 키워드(요구사항)
MUST_SHOW = [
    "tariff act", "trade expansion act", "international emergency economic powers act",
    "ieepa", "section 232", "section 301",
    "관세", "관세율", "관세법", "무역확장법", "국제비상경제권한법"
]

ALLOW = [
    "관세","tariff","관세율","hs","section 232","section 301","ieepa",
    "fta","원산지","무역구제","수출통제","export control","sanction","통관","customs",
    "tariff act","trade expansion act","international emergency economic powers act"
]
BLOCK = ["시위","protest","체포","arrest","충돌","violent","immigration","ice raid","연방정부","주정부"]

RISK_RULES = [
    ("tariff act", 8),
    ("trade expansion act", 8),
    ("international emergency economic powers act", 8),
    ("ieepa", 8),
    ("section 232", 7),
    ("section 301", 7),

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

def is_relevant(title: str, summary: str) -> bool:
    blob = f"{title} {summary}".lower()
    if any(b in blob for b in BLOCK):
        return False
    return any(a in blob for a in ALLOW)

def calc_score(title: str, summary: str) -> int:
    blob = f"{title} {summary}".lower()
    score = 1
    for kw, w in RISK_RULES:
        if kw in blob:
            score += w
    return min(score, 20)

def importance(score: int, title: str, summary: str) -> str:
    blob = f"{title} {summary}".lower()
    if any(k in blob for k in MUST_SHOW) or score >= 14:
        return "상"
    if score >= 7:
        return "중"
    return "하"

# ===============================
# COUNTRY / AGENCY (간단 휴리스틱)
# ===============================
COUNTRY_KEYWORDS = {
    "USA": ["u.s.", "united states", "america", "ustr", "section 301", "section 232", "ieepa"],
    "India": ["india"],
    "Türkiye": ["turkey", "türkiye"],
    "Vietnam": ["vietnam"],
    "Netherlands": ["netherlands", "dutch"],
    "EU": ["european union", "eu commission", "european commission"],
    "China": ["china"],
    "Mexico": ["mexico"],
    "Brazil": ["brazil"],
    "Indonesia": ["indonesia"],
    "Poland": ["poland"],
    "Slovakia": ["slovakia"],
    "Korea": ["korea", "south korea", "korean"],
}

AGENCY_KEYWORDS = {
    "USTR(미 무역대표부)": ["ustr", "u.s. trade representative"],
    "미 상무부(DoC)": ["department of commerce", "commerce department"],
    "미 CBP(관세국경보호청)": ["cbp", "customs and border protection"],
    "EU 집행위(Trade)": ["european commission", "eu commission"],
    "WTO": ["wto", "world trade organization"],
    "WCO": ["wco", "world customs organization"],
}

def detect_country(text: str) -> str:
    t = (text or "").lower()
    for c, keys in COUNTRY_KEYWORDS.items():
        if any(k in t for k in keys):
            return c
    return ""

def detect_agency(text: str) -> str:
    t = (text or "").lower()
    hits = []
    for a, keys in AGENCY_KEYWORDS.items():
        if any(k in t for k in keys):
            hits.append(a)
    return ", ".join(hits)

# ===============================
# SENSOR
# ===============================
def run_sensor() -> pd.DataFrame:
    rss = "https://news.google.com/rss/search?" + urllib.parse.urlencode({
        "q": NEWS_QUERY,
        "hl": "ko",
        "gl": "KR",
        "ceid": "KR:ko"
    })
    feed = feedparser.parse(rss)

    rows = []
    for e in feed.entries[:70]:
        title = (getattr(e, "title", "") or "").strip()
        link  = (getattr(e, "link", "") or "").strip()
        pub   = (getattr(e, "published", "") or "").strip()
        raw_summary = getattr(e, "summary", "") or ""

        # 07~07 필터
        if not in_window(pub):
            continue

        # 요약 정제 + 보강(가능할 때만)
        summary = clean_summary(title, raw_summary, link)

        # 주제 필터(관련성 낮은 건 제외)
        if not is_relevant(title, summary):
            continue

        sc = calc_score(title, summary)
        imp = importance(sc, title, summary)
        country = detect_country(f"{title} {summary}")
        agency  = detect_agency(f"{title} {summary}")

        rows.append({
            "제시어": NEWS_QUERY,
            "헤드라인": title,
            "주요내용": summary[:600],
            "발표일": pub,             # 요구: 최초 게시일/공표일. RSS published를 그대로 표기(추정 금지)
            "대상 국가": country,
            "관련 기관": agency,
            "중요도": imp,
            "점수": sc,
            "출처(URL)": link,
            "비고": "",
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["헤드라인", "출처(URL)"]).reset_index(drop=True)
    return df

# ===============================
# EXEC INSIGHT (사업연관성/Action 강화)
# ===============================
PROD_HINTS = [
    ("mobile", ["smartphone", "phone", "galaxy", "mobile", "tablet", "smartwatch", "earbuds", "반도체", "스마트폰", "휴대폰", "태블릿"]),
    ("ce", ["tv", "monitor", "refrigerator", "air conditioner", "appliance", "가전", "에어컨", "냉장고", "tv", "모니터"]),
    ("network", ["5g", "base station", "antenna", "network", "기지국", "안테나", "네트워크"]),
    ("medical", ["x-ray", "medical", "의료", "엑스레이"]),
]

def infer_exposure(blob: str) -> str:
    b = blob.lower()
    touched = []
    for name, keys in PROD_HINTS:
        if any(k.lower() in b for k in keys):
            touched.append(name)
    if not touched:
        return "전사 영향(원가·마진·리드타임/특혜관세/제재준수) 가능성 점검"
    if "mobile" in touched:
        return "모바일/부품(스마트폰·태블릿·웨어러블) 영향 가능성 점검"
    if "ce" in touched:
        return "생활가전/TV·모니터 영향 가능성 점검"
    if "network" in touched:
        return "네트워크 장비(5G 등) 프로젝트/납품 영향 점검"
    if "medical" in touched:
        return "의료기기(X-ray 등) 인증/수출입 영향 점검"
    return "전사 영향 점검"

def build_exec_pack(top3: pd.DataFrame) -> Tuple[str, str, str]:
    """
    Trigger/Exposure/Action 을 TOP3 기반으로 생성
    """
    triggers: List[str] = []
    exposures: List[str] = []
    actions: List[str] = []

    for _, r in top3.iterrows():
        title = str(r.get("헤드라인", ""))
        summ  = str(r.get("주요내용", ""))
        blob  = f"{title} {summ}".lower()
        ctry  = str(r.get("대상 국가", "") or "-")
        score = str(r.get("점수", ""))
        imp   = str(r.get("중요도", ""))

        # Trigger
        if any(k in blob for k in ["관세율", "tariff rate", "추가관세", "section 301", "section 232", "ieepa"]):
            trig = "관세율/추가관세/긴급권한(301·232·IEEPA) 관련 가능성"
        elif any(k in blob for k in ["hs", "hs code"]):
            trig = "HS 분류/세율 적용 리스크 가능성"
        elif any(k in blob for k in ["fta", "원산지"]):
            trig = "FTA/원산지 이슈(특혜관세 적용/소명) 가능성"
        elif any(k in blob for k in ["export control", "sanction", "entity list"]):
            trig = "수출통제/제재 준수 리스크 가능성"
        else:
            trig = "통상 정책 변화 가능성"

        triggers.append(f"[{ctry} | {imp} | {score}] {trig}")

        # Exposure
        exp = infer_exposure(blob)
        exposures.append(f"[{ctry}] {exp}")

        # Action (삼성 실무 플로우에 맞춰 구체화)
        act = (
            f"[{ctry}] 1) 적용시점/대상국/대상품목(HS) 확인 → "
            f"2) 생산법인 우선 스크리닝 후 판매법인 영향(원가·마진·리드타임) 1차 산정 → "
            f"3) 필요 시 HS/원산지/제재(수출통제) 체크리스트 업데이트 및 HQ 대응 착수"
        )
        actions.append(act)

    return "<br/>".join([html.escape(x) for x in triggers]), \
           "<br/>".join([html.escape(x) for x in exposures]), \
           "<br/>".join([html.escape(x) for x in actions])

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
.badge{display:inline-block;padding:2px 7px;border:1px solid #aaa;border-radius:10px;font-size:11px;margin-right:6px;}
</style>
"""

def get_link(r) -> str:
    u = r.get("출처(URL)", "")
    return u if u else "#"

# ===============================
# HTML (Practitioner) - 임원 인사이트 포함 + 표 유지(요구 반영)
# ===============================
def build_html_practitioner(df: pd.DataFrame) -> str:
    date = now_kst().strftime("%Y-%m-%d")
    s, e = window_07_to_07_kst()
    period = f"{s.strftime('%Y-%m-%d %H:%M')} ~ {e.strftime('%Y-%m-%d %H:%M')} (KST)"

    cand = df.copy()
    top3 = cand.sort_values("점수", ascending=False).head(3)
    trigger, exposure, action = build_exec_pack(top3)

    top3_html = ""
    for _, r in top3.iterrows():
        title = str(r.get("헤드라인", ""))
        summ  = str(r.get("주요내용", ""))
        link  = get_link(r)
        top3_html += f"""
        <li>
          <span class="badge">{html.escape(str(r.get('중요도','')))}</span>
          <b>[{html.escape(str(r.get('대상 국가','') or '-'))} | 점수 {html.escape(str(r.get('점수','')))}]</b><br/>
          <a href="{html.escape(link)}" target="_blank">{html.escape(title)}</a><br/>
          <div class="small">{html.escape(summ[:260])}</div>
        </li>
        """

    # 표: '출처' 칼럼 삭제 + 헤드라인 링크 + 헤드라인/주요내용 1칸
    rows = ""
    for _, r in df.sort_values("점수", ascending=False).iterrows():
        title = str(r.get("헤드라인", ""))
        summ  = str(r.get("주요내용", ""))
        link  = get_link(r)

        cell = f'<a href="{html.escape(link)}" target="_blank">{html.escape(title)}</a>' \
               f'<br/><span class="small">{html.escape(summ)}</span>'

        rows += f"""
        <tr>
          <td>{html.escape(str(r.get('발표일','') or ''))}</td>
          <td>{html.escape(str(r.get('대상 국가','') or ''))}</td>
          <td>{html.escape(str(r.get('관련 기관','') or ''))}</td>
          <td>{html.escape(str(r.get('중요도','') or ''))}</td>
          <td>{cell}</td>
          <td>{html.escape(str(r.get('비고','') or ''))}</td>
        </tr>
        """

    return f"""
    <html><head>{STYLE}</head>
    <body>
      <div class="page">
        <h2>관세·통상 정책 센서 ({date})</h2>
        <div class="small">수집기간: {html.escape(period)}</div>

        <div class="box">
          <h3>① 오늘의 핵심 TOP3</h3>
          <ul>{top3_html}</ul>
        </div>

        <div class="box">
          <h3>② Executive Insight (실무자 메일에도 동일 표기)</h3>
          <div class="small"><b>Trigger</b><br/>{trigger}</div><br/>
          <div class="small"><b>Exposure (사업 연관성)</b><br/>{exposure}</div><br/>
          <div class="small"><b>Action</b><br/>{action}</div>
        </div>

        <div class="box">
          <h3>③ 정책 이벤트 표</h3>
          <table>
            <tr>
              <th>발표일</th>
              <th>대상 국가</th>
              <th>관련 기관</th>
              <th>중요도</th>
              <th>헤드라인 / 주요내용</th>
              <th>비고</th>
            </tr>
            {rows}
          </table>
        </div>
      </div>
    </body></html>
    """

# ===============================
# HTML (Executive)
# ===============================
def build_html_exec(df: pd.DataFrame) -> str:
    date = now_kst().strftime("%Y-%m-%d")
    s, e = window_07_to_07_kst()
    period = f"{s.strftime('%Y-%m-%d %H:%M')} ~ {e.strftime('%Y-%m-%d %H:%M')} (KST)"

    top3 = df.sort_values("점수", ascending=False).head(3)
    trigger, exposure, action = build_exec_pack(top3)

    items = ""
    for _, r in top3.iterrows():
        title = str(r.get("헤드라인", ""))
        summ  = str(r.get("주요내용", ""))
        link  = get_link(r)
        items += f"""
        <li>
          <b>[{html.escape(str(r.get('대상 국가','') or '-'))} | {html.escape(str(r.get('중요도','')))} | 점수 {html.escape(str(r.get('점수','')))}]</b><br/>
          <a href="{html.escape(link)}" target="_blank">{html.escape(title)}</a><br/>
          <div class="small">{html.escape(summ[:220])}</div>
        </li>
        """

    return f"""
    <html><head>{STYLE}</head>
    <body>
      <div class="page">
        <h2>[Executive] 관세·통상 핵심 TOP3 ({date})</h2>
        <div class="small">수집기간: {html.escape(period)}</div>
        <div class="box"><ul>{items}</ul></div>
        <div class="box">
          <div class="small"><b>Trigger</b><br/>{trigger}</div><br/>
          <div class="small"><b>Exposure</b><br/>{exposure}</div><br/>
          <div class="small"><b>Action</b><br/>{action}</div>
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
def send_mail_to(recipients: List[str], subject: str, html_body: str):
    if not recipients:
        print(f"[WARN] recipients empty -> skip: {subject}")
        return

    if not SMTP_SERVER or not SMTP_EMAIL or not SMTP_PASSWORD:
        raise RuntimeError("SMTP env missing (SMTP_SERVER/SMTP_EMAIL/SMTP_PASSWORD)")

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
    print("RECIPIENTS count =", len(RECIPIENTS))
    print("RECIPIENTS_EXEC count =", len(RECIPIENTS_EXEC))
    print("HAS_WEB(meta enrich) =", HAS_WEB)

    df = run_sensor()
    if df is None or df.empty:
        print("오늘 수집된 이벤트/뉴스 없음")
        return

    today = now_kst().strftime("%Y-%m-%d")

    # 실무자 메일(임원 인사이트 포함) + 출력 저장
    html_prac = build_html_practitioner(df)
    write_outputs(df, html_prac)
    send_mail_to(RECIPIENTS, f"관세·통상 정책 센서 ({today})", html_prac)

    # 임원 메일
    exec_targets = RECIPIENTS_EXEC[:]
    if not exec_targets and EXEC_FALLBACK:
        exec_targets = RECIPIENTS[:]
        print("[WARN] RECIPIENTS_EXEC empty -> fallback to RECIPIENTS (EXEC_FALLBACK=1)")

    if not exec_targets:
        print("[WARN] RECIPIENTS_EXEC empty -> exec mail NOT sent")
    else:
        html_exec = build_html_exec(df)
        send_mail_to(exec_targets, f"[Executive] 관세·통상 핵심 TOP3 ({today})", html_exec)

    print("✅ vNext FINAL 완료")

if __name__ == "__main__":
    main()
