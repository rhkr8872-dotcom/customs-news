# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import re
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse, urlunparse

import pandas as pd

from gti_report_window import kst_now

BASE_DIR = Path(os.getenv('GTI_BASE_DIR', r'C:\Temp'))
INPUT_FILE = BASE_DIR / '1-1.regulation_raw.xlsx'
KEYWORD_FILE = BASE_DIR / 'keyword.xlsx'
OUT_SUMMARY = BASE_DIR / '3-1.regulation_summary.xlsx'
OUT_ARTICLE = BASE_DIR / '3-1.regulation_article_summary.xlsx'
OUT_CUMULATIVE = BASE_DIR / '3-1.regulation_cumulative.xlsx'
OUT_EXCLUDED = BASE_DIR / '3-1.regulation_excluded.xlsx'
OUT_AUDIT = BASE_DIR / '3-1.regulation_audit.xlsx'

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '').strip()
GEMINI_MODEL = os.getenv('GTI_GEMINI_MODEL', os.getenv('GEMINI_MODEL', 'gemini-2.5-flash-lite'))
AI_MIN_SCORE = int(os.getenv('GTI_REG_AI_RELEVANCE_MIN', '60'))
SAME_DAY_SIMILARITY = float(os.getenv('GTI_REG_SAME_DAY_SIMILARITY', '0.90'))

client = None
if GEMINI_API_KEY:
    try:
        from google import genai
        client = genai.Client(api_key=GEMINI_API_KEY)
    except Exception:
        client = None

CORE_TERMS = [
    '관세','관세율','통관','세관','수입신고','수출신고','보세','관세환급','품목분류','hs code','hs코드',
    '원산지','fta','cepa','epa','rcep','협정세율','특혜관세','반덤핑','덤핑방지관세','상계관세','세이프가드',
    '수출통제','전략물자','제재','cbam','탄소국경','section 301','section 232',
    'customs','tariff','duty','import duty','anti-dumping','antidumping','countervailing','rules of origin',
    'export control','sanctions','customs valuation','de minimis','trade remedy',
    # Customs-adjacent rules that directly change import/export payment or
    # declaration operations.  Keep these as exact concepts, not broad
    # "외환"/"전자" matches, to avoid unrelated finance/IT notices.
    '외국환거래규정','외국환 신고','수출입대금','지급 및 영수',
    '납세신고 정정','수입신고 정정','전자문서 변경','전자신고 변경','5fe','5fk'
]

# HQ + overseas subsidiary regulation scope. General customs procedures in
# these jurisdictions affect the local Samsung entity even without a product.
SAMSUNG_ENTITY_COUNTRIES = {
    '대한민국','한국','korea','united states','usa','미국','china','중국','vietnam','베트남',
    'india','인도','mexico','멕시코','brazil','브라질','poland','폴란드','hungary','헝가리',
    'slovakia','슬로바키아','malaysia','말레이시아','indonesia','인도네시아','thailand','태국',
    'philippines','필리핀','canada','캐나다','united kingdom','uk','영국','germany','독일',
    'france','프랑스','spain','스페인','italy','이탈리아','netherlands','네덜란드',
    'european union','eu','유럽연합','turkiye','turkey','튀르키예','칠레','chile',
}
GENERAL_CUSTOMS_PROCEDURE_TERMS = [
    '통관절차','수입신고','수출신고','전자신고','세관신고','신고서','제출서류','증빙서류',
    '과세가격','관세평가','관세납부','납부기한','보세','특송','de minimis','사후심사','세관조사',
    '심판청구','심사청구','이의신청','행정심판','불복절차','사전심사','사전판정','관세환급',
    'customs procedure','customs declaration','import declaration','export declaration',
    'customs valuation','customs audit','administrative appeal','appeal procedure','advance ruling',
    'binding ruling','duty drawback','customs refund','bonded warehouse','record keeping',
    '납세신고 정정','수입신고 정정','신고 정정','전자문서 변경','전자신고 변경',
    '5fe','5fk','외국환거래규정','외국환 신고','수출입대금','지급 및 영수',
]
ITEM_SPECIFIC_TERMS = [
    'hs code','hs코드','품목분류','tariff classification','반덤핑','anti-dumping','antidumping',
    '덤핑방지관세','덤핑방지','anti dumping',
    '상계관세','countervailing','세이프가드','safeguard','쿼터','quota','대상품목','특정품목',
    '덤핑사실','국내산업피해','산업피해구제','불공정무역행위','조사개시결정','무역위원회공고',
    '수출통제','export control','entity list','전략물자','cbam','탄소국경',
]

def regulation_mapping_type(row: pd.Series, title: str) -> tuple[str, str, str]:
    text = norm(' '.join([title, clean(row.get('Country','')), clean(row.get('Agency','')), clean(row.get('Source',''))]))
    country = clean(row.get('Country',''))
    entity_country = any(term in text for term in SAMSUNG_ENTITY_COUNTRIES)
    procedure = any(term in text for term in GENERAL_CUSTOMS_PROCEDURE_TERMS)
    item_specific = any(term in text for term in ITEM_SPECIFIC_TERMS)
    samsung_named = any(term in text for term in ['삼성전자', 'samsung electronics', '삼성디스플레이', 'samsung display'])
    if samsung_named:
        return 'ENTITY_DIRECT', 'ENTITY_CONFIRMED', 'Y'
    if item_specific:
        return 'PRODUCT_1TO1', 'MAPPING_REQUIRED', 'N'
    if procedure and entity_country:
        return 'POLICY_GENERAL', 'GENERAL_APPLICABILITY', 'N'
    if procedure:
        return 'POLICY_GENERAL', 'COUNTRY_CONFIRM', 'N'
    return 'POLICY_GENERAL', 'POLICY_REVIEW', 'N'

NOISE_TERMS = [
    '채용','합격자','인사','승진','교육','세미나','웨비나','행사','입찰','공모','마약','밀수','범죄',
    'recruitment','webinar','seminar','conference','tender','drug seizure','smuggling','ceremony',
    # Vietnam Customs news/statistics/enforcement pages are official posts but
    # not new or amended customs regulations.
    'thúc đẩy hợp tác hải quan','đối thoại hải quan','phiên đối thoại',
    'sơ bộ tình hình xuất nhập khẩu','tình hình xuất nhập khẩu',
    'bắt giữ','hàng giả','giả nhãn hiệu','lịch bảo trì hệ thống'
]

def clean(v) -> str:
    if v is None:
        return ''
    try:
        if pd.isna(v):
            return ''
    except Exception:
        pass
    return re.sub(r'\s+', ' ', str(v)).strip()

def norm(v) -> str:
    return clean(v).lower()

def norm_title(v) -> str:
    s = norm(v)
    s = re.sub(r'\[[^\]]*\]|\([^\)]*\)', ' ', s)
    s = re.sub(r'[^0-9a-z가-힣一-龥ぁ-ゔァ-ヴー\s]', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()

def norm_url(v) -> str:
    u = clean(v)
    if not u.startswith(('http://','https://')):
        return ''
    try:
        p = urlparse(u)
        keep = []
        for k, val in parse_qsl(p.query, keep_blank_values=True):
            if k.lower() not in {'utm_source','utm_medium','utm_campaign','utm_term','utm_content','gclid','fbclid'}:
                keep.append((k,val))
        # Normalize path spaces, %20 and mixed percent-encoding to one form.
        # Without this, the same DGFT PDF can be emitted again as a new item.
        path = quote(unquote(p.path), safe='/@:~!$&\'()*+,;=-._')
        query = urlencode(sorted(keep), doseq=True)
        return urlunparse((p.scheme.lower(), p.netloc.lower(), path.rstrip('/'), '', query, ''))
    except Exception:
        return u.lower()


def national_assembly_bill_no(value) -> str:
    """Extract the stable bill id from a National Assembly detail URL."""
    url = clean(value)
    match = re.search(r"/gcom/nsmLmSts/out/(\d+)/detailRP(?:$|[/?#])", url, re.I)
    return match.group(1) if match else ""


def dgft_document_identity(row: pd.Series) -> str:
    """Stable DGFT identity independent of spaces, %20 and mirror URLs.

    DGFT often publishes the same PDF with a literal-space URL on one run and
    a percent-encoded URL on another.  Prefer the trade-notice/public-notice-
    notice number; fall back to the notice number plus publication year.
    """
    text = " ".join([
        clean(row.get("Headline", "")), clean(row.get("URL", "")),
        clean(row.get("Agency", "")), clean(row.get("Source", "")),
    ])
    low = unquote(text).lower()
    if "dgft" not in low and "directorate general of foreign trade" not in low:
        return ""
    full = re.search(r"\b(\d{1,4})\s*/\s*(20\d{2})\s*[-–]\s*(\d{1,4})\b", low)
    if full:
        return f"DGFT:NOTICE:{full.group(1)}/{full.group(2)}-{full.group(3)}"
    notice = re.search(
        r"\b(?:tn|trade\s*notice|pn|public\s*notice|notification)\s*(?:no\.?\s*)?(\d{1,4})\b",
        low,
    )
    year = re.search(r"\b(20\d{2})\b", low)
    if notice and year:
        return f"DGFT:NOTICE:{notice.group(1)}:{year.group(1)}"
    return ""


def document_identity(row: pd.Series) -> str:
    """Return a document-level id; equal bill titles are not equal bills."""
    bill_no = national_assembly_bill_no(row.get("URL", ""))
    if bill_no:
        return f"BILL:{bill_no}"
    dgft_id = dgft_document_identity(row)
    if dgft_id:
        return dgft_id
    number = regulation_number(row.get("Headline", ""))
    if number:
        return f"REGNO:{number}"
    url = norm_url(row.get("URL", ""))
    if url:
        return f"URL:{url}"
    return f"TITLE:{norm_title(row.get('Headline', ''))}|{norm(row.get('Agency', ''))}"

def first_existing(df: pd.DataFrame, names: list[str]):
    lower = {str(c).strip().lower(): c for c in df.columns}
    for name in names:
        if name.lower() in lower:
            return lower[name.lower()]
    return None

def standardize(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    mapping = {
        'Date':['Date','date','published','posted_at'],
        'CollectedAt':['CollectedAt','collected_at','last_checked'],
        'Headline':['Headline','headline','title','제목'],
        'URL':['URL','url','link'],
        'Source':['Source','source'],
        'Agency':['Agency','agency','site_name','publisher'],
        'Country':['Country','country'],
        'site_type':['site_type','SiteType'],
        'date_status':['date_status','DateStatus'],
        'date_source':['date_source','DateSource','PublicationDateSource'],
        'date_confidence':['date_confidence','DateConfidence','PublicationDateConfidence'],
    }
    for target, names in mapping.items():
        c = first_existing(df, names)
        out[target] = df[c] if c else ''
    out['site_type'] = 'regulation'
    out['Date'] = pd.to_datetime(out['Date'], errors='coerce')
    return out

def load_keywords() -> list[str]:
    if not KEYWORD_FILE.exists():
        return []
    df = pd.read_excel(KEYWORD_FILE)
    if df.empty:
        return []
    c = first_existing(df, ['Keyword','keyword','키워드']) or df.columns[0]
    vals = []
    for v in df[c].dropna().tolist():
        t = clean(v)
        if len(t) >= 2:
            vals.append(t)
    return list(dict.fromkeys(vals))

def keyword_hits(title: str, keywords: list[str]) -> list[str]:
    """
    keyword.xlsx title matching:
    - Korean/CJK or longer terms: substring match
    - short Latin terms (2~4 chars): whole-word match only
    """
    t = norm(title)
    hits = []
    for k in keywords:
        nk = norm(k)
        if not nk:
            continue
        if re.fullmatch(r'[a-z0-9]{2,4}', nk):
            if re.search(rf'(?<![a-z0-9]){re.escape(nk)}(?![a-z0-9])', t):
                hits.append(k)
        elif nk == '세관':
            if re.search(r'(?<!과)세관(?!청)', t):
                hits.append(k)
        elif nk in t:
            hits.append(k)
    return hits



NEGATIVE_GUARD_TERMS = [
    # 기관명/조직·내부행정: 관세/통상 문자열이 있어도 삼성전자 관세업무 법규가 아님
    '직제 시행규칙','직제 일부개정','조직개편','기구 개편','정원','인사발령',
    '그 소속기관 직제','소속기관 직제','직제 (행정관련','직제(행정관련',
    '철도교통관제센터','선박안전법','한국수출입은행법','인터넷 통관포털(uni-pass) 이용약관',
    '공익신고','신고자 보호','행정처분 및 과태료의 가중 처분',
    '도시정책','도시 계획','도시계획','주택정책','공익신고','신고자 보호',
    '채용','인사발령','조직개편','복무','청렴','윤리','개인정보 보호',
    'urban policy','urban planning','whistleblower protection','recruitment',
    'personnel appointment','privacy policy'
    ,'공휴일법','hari kelepasan','holiday act','국세기본법','전체 관세청 유관기관'
    ,'방송통신기자재등 시험기관','자원순환에 관한 법률','수출검역요령','토마토 생과실'
    ,'thúc đẩy hợp tác hải quan','đối thoại hải quan','phiên đối thoại'
    ,'sơ bộ tình hình xuất nhập khẩu','tình hình xuất nhập khẩu'
    ,'bắt giữ','hàng giả','giả nhãn hiệu','lịch bảo trì hệ thống'
    ,'와인제품','포도주','denominação de origem','denomination of origin','geographical indication'
]

OFFICIAL_TRADE_POLICY_AGENCIES = [
    'dgft', 'cbic', 'customs', '관세청', 'ustr', 'usitc', 'mofcom',
    'gacc', 'taxud', 'department of commerce', 'ministry of trade',
]
OFFICIAL_TRADE_POLICY_ACTIONS = [
    'amendment in the export policy', 'amendment in export policy',
    'amendment in the import policy', 'amendment in import policy',
    'export policy of', 'import policy of', 'trade notice',
    '수출정책 개정', '수입정책 개정', '수출입정책 개정',
]

def official_trade_policy_rule(title: str, agency: str = '', source: str = '') -> tuple[bool, list[str]]:
    """Require both an official trade agency and a concrete policy action."""
    title_n = norm(title)
    owner_n = norm(f'{agency} {source}')
    agency_hits = [x for x in OFFICIAL_TRADE_POLICY_AGENCIES if x in owner_n]
    action_hits = [x for x in OFFICIAL_TRADE_POLICY_ACTIONS if x in title_n]
    return bool(agency_hits and action_hits), action_hits

def obvious_non_customs_guard(title: str) -> bool:
    t = norm(title)
    return any(x in t for x in NEGATIVE_GUARD_TERMS)

def keyword_hits_effective(title: str, keywords: list[str]) -> list[str]:
    """keyword.xlsx 제목 매칭 후 기관명 내부의 가짜 매칭을 제거한다."""
    hits = keyword_hits(title, keywords)
    t = norm(title)
    cleaned = []
    for h in hits:
        nh = norm(h)
        customs_context = any(x in t for x in [
            '관세','통관','세관','수입신고','수출신고','보세','환급','tariff','customs','duty','clearance'
        ])
        origin_context = any(x in t for x in [
            'fta','cepa','epa','rcep','협정세율','특혜관세','원산지증명','rules of origin','certificate of origin'
        ])
        sanctions_context = any(x in t for x in [
            '수출통제','전략물자','경제제재','금융제재','제재대상','entity list','export control','economic sanctions','sanctions list'
        ])
        if nh in {'수입','수출','import','export'} and not customs_context:
            continue
        if nh in {'원산지','origin'} and not (customs_context or origin_context):
            continue
        if nh in {'제재','sanctions','sanction'} and not sanctions_context:
            continue
        # '통상'이 '산업통상부' 기관명에만 존재하는 경우는 키워드 적중으로 보지 않음
        if nh == '통상':
            residual = t.replace('산업통상부', ' ').replace('산업통상자원부', ' ')
            if '통상' not in residual:
                continue
        # '관세'가 '관세청' 기관명에만 존재하는 조직/직제 문서는 제외
        if nh == '관세' and ('직제 시행규칙' in t or '조직개편' in t):
            residual = t.replace('관세청', ' ')
            if '관세' not in residual:
                continue
        cleaned.append(h)
    return cleaned


STRONG_CUSTOMS_TERMS = [
    '관세','관세율','추가관세','상호관세','수입관세','수출관세',
    '통관','세관','수입신고','수출신고','관세평가','과세가격','보세','관세환급','특송물품',
    'customs','customs clearance','customs valuation','customs declaration',
    'tariff','tariffs','customs duty','import duty','export duty','de minimis',
    '품목분류','hs code','hs코드','tariff classification','harmonized system',
    'fta','cepa','epa','rcep','원산지','원산지증명','협정세율','특혜관세',
    'rules of origin','certificate of origin','preferential tariff',
    '반덤핑','덤핑방지관세','덤핑사실','국내산업피해','산업피해구제','불공정무역행위','조사개시결정',
    '상계관세','세이프가드','무역구제',
    'anti-dumping','anti dumping','antidumping','countervailing','countervailing duty','safeguard',
    '수출통제','전략물자','제재','entity list','export control','export controls','sanctions','uflpa',
    'cbam','탄소국경','carbon border adjustment',
    'section 232','section 301','232조','301조',
    # HQ customs operations: trade-payment compliance and declaration EDI.
    '외국환거래규정','외국환 신고','수출입대금','지급 및 영수',
    '납세신고 정정','수입신고 정정','전자문서 변경','전자신고 변경','5fe','5fk',
]

def strong_customs_rule(title: str) -> tuple[bool, list[str]]:
    t = norm(title)
    hits = []
    for term in STRONG_CUSTOMS_TERMS:
        if term == '세관':
            if re.search(r'(?<!과)세관(?!청)', t):
                hits.append(term)
        elif re.fullmatch(r'[a-z0-9]{2,4}', term):
            if re.search(rf'(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])', t):
                hits.append(term)
        elif term in t:
            hits.append(term)
    customs_context = any(x in t for x in [
        '관세','통관','세관','수입신고','수출신고','보세','환급','tariff','customs','duty','clearance'
    ])
    origin_context = any(x in t for x in [
        'fta','cepa','epa','rcep','협정세율','특혜관세','원산지증명','rules of origin','certificate of origin'
    ])
    sanctions_context = any(x in t for x in [
        '수출통제','전략물자','경제제재','금융제재','제재대상','entity list','export control','economic sanctions','sanctions list'
    ])
    hits = [h for h in hits if not (
        (h in {'수입','수출','import','export'} and not customs_context)
        or (h in {'원산지','origin'} and not (customs_context or origin_context))
        or (h in {'제재','sanctions','sanction'} and not sanctions_context)
    )]
    # 관세청 조직/직제 개정은 customs compliance 법규가 아니므로 strong hard-keep 금지
    if ('직제 시행규칙' in t or '조직개편' in t) and '관세청' in t:
        substantive = [h for h in hits if h not in {'관세'}]
        hits = substantive
    return bool(hits), hits


def ai_judge(title: str, agency: str) -> tuple[bool,int,str]:
    if re.fullmatch(r'\s*(?:download(?:\s*\(type\s*:\s*pdf\))?|pdf|view|notification|public notice)\s*', title, re.I):
        return False, 0, "GENERIC_DOCUMENT_TITLE_REQUIRES_BODY"
    if client is None:
        return False, 0, "AI_OFF"

    prompt = (
        "You are screening an OFFICIAL government/IGO post for Samsung Electronics customs compliance.\n"
        "Return JSON only: {\"relevant\": true/false, \"score\": 0-100, \"reason\": \"...\"}.\n"
        "Relevant means customs, tariff, clearance, HS classification, origin/FTA, AD/CVD/safeguard, "
        "export control/sanctions, CBAM or another trade-compliance obligation.\n"
        "Do NOT mark generic economy, industry, competition-law, recruitment, events, crime or statistics as relevant.\n"
        "The agency name alone is never enough. A generic Download/PDF title must be false until the document body is read.\n"
        f"Title: {title}\nAgency: {agency}"
    )

    models = []
    for m in [
        GEMINI_MODEL,
        os.getenv("GTI_GEMINI_FALLBACK_MODEL", "").strip(),
    ]:
        if m and m not in models:
            models.append(m)

    last_error = ""
    for model_name in models:
        try:
            r = client.models.generate_content(model=model_name, contents=prompt)
            raw = clean(getattr(r, "text", ""))
            a, b = raw.find("{"), raw.rfind("}")
            if a >= 0 and b > a:
                d = json.loads(raw[a:b+1])
                score = int(float(d.get("score", 0) or 0))
                relevant = bool(d.get("relevant")) and score >= AI_MIN_SCORE
                return relevant, score, f"{model_name}: {clean(d.get('reason'))}"
            last_error = f"{model_name}:AI_NO_JSON"
        except Exception as e:
            last_error = f"{model_name}:{type(e).__name__}:{clean(e)[:180]}"

    return False, 0, f"AI_ERROR:{last_error or 'UNKNOWN'}"


def canonical_regulation_title(v) -> str:
    title = clean(v).lower()

    title = re.sub(r'^\s*\[[^\]]+\]\s*', '', title)
    title = re.sub(r'\(\s*제출기한\s*:[^)]*\)', ' ', title)
    title = re.sub(r'\(\s*의견제출[^)]*\)', ' ', title)
    title = re.sub(r'\(\s*[가-힣a-z0-9·ㆍ\s]+과\s*\)\s*$', ' ', title)
    title = re.sub(r'제출기한\s*[:：]?\s*20\d{2}[./-]\s*\d{1,2}[./-]\s*\d{1,2}[^\s)]*', ' ', title)

    title = title.replace('일부개정(안)', '일부 개정안')
    title = title.replace('일부 개정(안)', '일부 개정안')
    title = title.replace('개정(안)', '개정안')

    title = re.sub(r'\s*및\s*의견조회.*$', '', title)
    title = re.sub(r'\s*행정예고\s*및\s*의견조회.*$', ' 행정예고', title)

    title = re.sub(r'[「」『』“”"\'`]', '', title)
    title = re.sub(r'[^0-9a-z가-힣一-龥ぁ-ゔァ-ヴー\s]', ' ', title)
    title = re.sub(r'\s+', ' ', title).strip()
    return title


def regulation_number(v) -> str:
    """관세청고시제2026-50호와 같은 법규번호를 안정적으로 추출한다."""
    raw = clean(v)
    # Match a numbered enactment at the beginning of a title or inside an
    # official parenthetical citation.  The boundary prevents the preceding
    # regulation title from being swallowed as part of the ministry name.
    m = re.search(
        r"(?:^|[\s(（])([가-힣]{2,20}(?:부령|총리령|대통령령|법률))\s*[,，]?\s*제?\s*(\d+)호",
        raw,
    )
    if m:
        return f"{m.group(1)}제{m.group(2)}호".lower()

    t = re.sub(r"[\s,，()（）·ㆍ]+", "", raw)
    m = re.search(
        r"(관세청(?:고시|공고|훈령|예규)제?20\d{2}[-–]\d+호)",
        t,
    )
    if m:
        return re.sub(r"[-–]", "-", m.group(1)).lower()

    # Ministry ordinances and other numbered enactments often appear in two
    # official forms: "재정경제부령제49호" and "(재정경제부령, 제49호)".
    # Treat both as the same legal document across Gazette and law.go.kr.
    m = re.search(r"^([가-힣]{2,20}(?:부령|총리령|대통령령|법률))제?(\d+)호", t)
    if m:
        return f"{m.group(1)}제{m.group(2)}호".lower()

    m = re.search(
        r"([가-힣]{2,20}(?:고시|공고|훈령|예규)제?20\d{2}[-–]\d+호)",
        t,
    )
    return re.sub(r"[-–]", "-", m.group(1)).lower() if m else ""


def clean_gazette_headline(v) -> str:
    """한 관보 제목에 붙은 다른 기관 고시를 제거한다."""
    title = clean(v)
    if "관세청" not in title:
        return title

    m = re.search(
        r"(관세청(?:고시|공고|훈령|예규)제?\s*20\d{2}[-–]\d+호"
        r"\s*\([^)]{3,300}\))",
        title,
    )
    if m:
        return m.group(1)
    return title

def legal_fingerprint(row: pd.Series) -> str:
    # 기관이나 URL이 달라도 동일 법규번호는 하나의 법규로 처리한다.
    bill_no = national_assembly_bill_no(row.get('URL', ''))
    if bill_no:
        return f'bill:{bill_no}'
    dgft_id = dgft_document_identity(row)
    if dgft_id:
        return dgft_id.lower()
    return (
        regulation_number(row.get('Headline', ''))
        or canonical_regulation_title(row.get('Headline', ''))
    )



REG_EVENT_PATTERNS = [
    ("PROVISIONAL_DUTY", [
        "provisional anti-dumping", "provisional antidumping",
        "잠정 반덤핑", "잠정관세", "잠정 관세",
    ]),
    ("FINAL_DUTY", [
        "definitive anti-dumping", "final anti-dumping",
        "확정 반덤핑", "최종 반덤핑", "확정관세", "최종 관세",
    ]),
    ("ADMIN_NOTICE", [
        "행정예고", "administrative notice", "notice and comment",
    ]),
    ("LEGISLATIVE_NOTICE", [
        "입법예고", "proposed rule", "proposed regulation",
    ]),
    ("AMENDMENT", [
        "일부개정", "일부 개정", "개정안", "개정", "amendment", "amending",
    ]),
    ("ENACTMENT", [
        "공포", "promulgation", "enacted", "adopted",
    ]),
    ("EFFECTIVE", [
        "시행", "effective date", "enters into force", "entry into force",
    ]),
    ("INVESTIGATION", [
        "조사개시", "investigation initiated", "initiation of investigation",
    ]),
    ("REVIEW", [
        "재심", "review", "sunset review",
    ]),
]

def regulation_event_type(title: str) -> str:
    t = norm(title)
    for label, terms in REG_EVENT_PATTERNS:
        if any(term in t for term in terms):
            return label
    return "OTHER"

def regulation_event_key(row: pd.Series) -> str:
    """
    Event-level duplicate key.
    Same regulation title on a NEW publication date/event is reportable again.
    Reposts of the same event on the same date collapse to one item.
    """
    bill_no = national_assembly_bill_no(row.get("URL", ""))
    if bill_no:
        return f"bill:{bill_no}"
    dgft_id = dgft_document_identity(row)
    if dgft_id:
        return dgft_id.lower()
    number = regulation_number(row.get("Headline", ""))
    if number:
        return f"regno:{number}"
    title_key = canonical_regulation_title(row.get("Headline", ""))
    event_type = regulation_event_type(row.get("Headline", ""))
    dt = pd.to_datetime(row.get("Date", ""), errors="coerce")
    day_key = dt.strftime("%Y-%m-%d") if pd.notna(dt) else ""
    return f"{title_key}|{event_type}|{day_key}"


def cross_source_policy_identity(row: pd.Series) -> str:
    """Stable identity for the same regulation reposted by different official sites."""
    dgft_id = dgft_document_identity(row)
    if dgft_id:
        return dgft_id.lower()
    number = regulation_number(row.get('Headline', ''))
    if number:
        return f'regno:{number}'
    title = norm(clean(row.get('Headline', '')))
    if '전략물자수출입고시' in title:
        return 'kr strategic goods export import notice'
    if '품목분류 적용기준에 관한 고시' in title:
        return 'kr customs classification application standard notice'
    return ''


def same_day_dedup(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    def source_rank(row):
        text = (clean(row.get('URL', '')) + ' ' + clean(row.get('Source', ''))).lower()
        if 'law.go.kr' in text:
            return 100
        if 'gwanbo.go.kr' in text:
            return 90
        if any(x in text for x in ['customs.go.kr', 'motie.go.kr', 'mofe.go.kr', 'moleg.go.kr']):
            return 80
        if 'clhs.co.kr' in text:
            return 40
        return 60
    work['_source_rank'] = work.apply(source_rank, axis=1)
    work = work.sort_values(['Date','_source_rank','Headline'], ascending=[False,False,True]).copy()
    work['Headline'] = work['Headline'].apply(clean_gazette_headline)
    work['AlternateURLs'] = work.get('AlternateURLs', pd.Series('', index=work.index)).fillna('').astype(str)
    work['AlternateSources'] = work.get('AlternateSources', pd.Series('', index=work.index)).fillna('').astype(str)
    source_count = work['SourceCount'] if 'SourceCount' in work.columns else pd.Series(1, index=work.index)
    work['SourceCount'] = pd.to_numeric(source_count, errors='coerce').fillna(1).astype(int)
    keep = []
    for idx, row in work.iterrows():
        day = row['Date'].date() if pd.notna(row['Date']) else None
        title = norm_title(row.get('Headline',''))
        dup = False
        for kidx in keep:
            kr = work.loc[kidx]
            bill_a = national_assembly_bill_no(row.get('URL', ''))
            bill_b = national_assembly_bill_no(kr.get('URL', ''))
            if bill_a or bill_b:
                if bill_a and bill_a == bill_b:
                    dup = True
                    break
                # Different bill ids (or bill vs non-bill) are separate legal
                # documents even when title, agency and publication day match.
                continue
            kday = kr['Date'].date() if pd.notna(kr['Date']) else None
            identity_a = cross_source_policy_identity(row)
            identity_b = cross_source_policy_identity(kr)
            near_day_same_policy = bool(
                identity_a and identity_a == identity_b and day and kday
                and abs((day - kday).days) <= 2
                and regulation_event_type(row.get('Headline', '')) == 'OTHER'
                and regulation_event_type(kr.get('Headline', '')) == 'OTHER'
            )
            # A unique legal instrument number is stable across official
            # publication sites and does not require identical title wording.
            number_a = regulation_number(row.get('Headline', ''))
            number_b = regulation_number(kr.get('Headline', ''))
            same_number = bool(number_a and number_a == number_b)
            if day != kday and not near_day_same_policy and not same_number:
                continue
            if near_day_same_policy:
                dup = True
                break
            other = norm_title(kr.get('Headline',''))
            event_a = regulation_event_key(row)
            event_b = regulation_event_key(kr)
            if event_a and event_a == event_b:
                dup = True
                break

            canonical_a = canonical_regulation_title(row.get('Headline',''))
            canonical_b = canonical_regulation_title(kr.get('Headline',''))
            if number_a and number_a == number_b:
                dup = True
                break
            if canonical_a and canonical_a == canonical_b:
                dup = True
                break
            if title and other and SequenceMatcher(None,title,other).ratio() >= SAME_DAY_SIMILARITY:
                dup = True
                break
        if dup:
            kept_idx = kidx
            alt_urls = [x for x in [clean(work.at[kept_idx, 'AlternateURLs']), clean(row.get('URL', ''))] if x]
            alt_sources = [x for x in [clean(work.at[kept_idx, 'AlternateSources']), clean(row.get('Source', ''))] if x]
            work.at[kept_idx, 'AlternateURLs'] = ' | '.join(dict.fromkeys(' | '.join(alt_urls).split(' | ')))
            work.at[kept_idx, 'AlternateSources'] = ' | '.join(dict.fromkeys(' | '.join(alt_sources).split(' | ')))
            work.at[kept_idx, 'SourceCount'] = int(work.at[kept_idx, 'SourceCount']) + 1
        else:
            keep.append(idx)
    return work.loc[keep].drop(columns=['_source_rank'], errors='ignore').copy()


def cumulative_row_is_valid(row: pd.Series, keywords: list[str]) -> tuple[bool, str]:
    """Re-validate legacy cumulative rows using current deterministic rules."""
    title = clean(row.get('Headline', '') or row.get('title', ''))
    if not title:
        return False, 'EMPTY_TITLE'

    low = norm(title)
    if low in {'feedback', 'directorates', 'helpdesk', 'website policy'}:
        return False, 'LEGACY_MENU_TITLE'
    if re.fullmatch(r'법률\s*제?\s*\d+호', title):
        return False, 'UNIDENTIFIABLE_LEGAL_TITLE'
    notice_markers = re.findall(
        r'(?:법률|대통령령|총리령)제\s*\d+호|'
        r'[가-힣]{2,30}(?:부령|고시|공고|훈령|예규)제?\s*\d{4}(?:[-–]\d+)?호',
        title,
    )
    if len(notice_markers) >= 2:
        return False, 'COMPOUND_GAZETTE_TITLE'

    noise = any(x in norm(title) for x in NOISE_TERMS)
    negative_guard = obvious_non_customs_guard(title)
    strong_rel, _ = strong_customs_rule(title)
    official_policy, _ = official_trade_policy_rule(
        title, clean(row.get('Agency', '')), clean(row.get('Source', ''))
    )
    kw_hits = keyword_hits_effective(title, keywords)

    # Preserve previously confirmed AI rows only when they are not now blocked.
    ai_rel = clean(row.get('AIRelevant', '')).upper() == 'Y'
    selection_rule = clean(row.get('SelectionRule', '')).upper()
    ai_confirmed = ai_rel or selection_rule == 'AI_CUSTOMS_YES'

    if noise:
        return False, 'NOISE'
    if negative_guard:
        return False, 'OBVIOUS_NON_CUSTOMS'
    if official_policy:
        return True, 'OFFICIAL_TRADE_POLICY'
    if strong_rel:
        return True, 'STRONG_CUSTOMS_RULE'
    if kw_hits:
        return True, 'TITLE_KEYWORD'
    # Old AI-only decisions were produced before the current context guards.
    # They must not survive cumulative cleanup without a current deterministic
    # customs/trade signal.
    if ai_confirmed:
        return False, 'LEGACY_AI_ONLY_RECHECK_REQUIRED'
    return False, 'NO_CUSTOMS_TRADE_SIGNAL'


def clean_cumulative(old_raw: pd.DataFrame, keywords: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean legacy cumulative and deduplicate by regulation event identity."""
    if old_raw is None or old_raw.empty:
        return pd.DataFrame(), pd.DataFrame()

    work = old_raw.copy()
    std = standardize(work)

    # Bring normalized fields back for robust validation.
    for c in ['Date','Headline','URL','Source','Agency']:
        work[c] = std[c].values

    decisions = work.apply(lambda r: cumulative_row_is_valid(r, keywords), axis=1)
    work['_keep'] = [x[0] for x in decisions]
    work['CumulativeValidationReason'] = [x[1] for x in decisions]

    removed = work[~work['_keep']].copy()
    kept = work[work['_keep']].copy()

    if not kept.empty:
        mapping = kept.apply(
            lambda r: regulation_mapping_type(r, clean(r.get('Headline', ''))), axis=1
        )
        kept['RegulationMappingType'] = [x[0] for x in mapping]
        kept['MappingStatus'] = [x[1] for x in mapping]
        kept['EntityDirectFlag'] = [x[2] for x in mapping]
        kept['RequiredMappingKeys'] = [
            'SamsungEntity; Transaction' if x[0] == 'ENTITY_DIRECT'
            else 'Product; HSCode; OriginCountry; Supplier; SamsungEntity; ImportHistory' if x[0] == 'PRODUCT_1TO1'
            else 'Country; SamsungEntity'
            for x in mapping
        ]
        kept['EventType'] = kept['Headline'].apply(regulation_event_type)
        kept = same_day_dedup(kept)
        kept['EventKey'] = kept.apply(regulation_event_key, axis=1)
        kept['_date_sort'] = pd.to_datetime(kept['Date'], errors='coerce')
        kept = kept.sort_values('_date_sort', ascending=False, kind='stable')
        kept = kept.drop_duplicates('EventKey', keep='first')
        kept = kept.drop(columns=['_date_sort'], errors='ignore')

    kept = kept.drop(columns=['_keep'], errors='ignore').reset_index(drop=True)
    removed = removed.drop(columns=['_keep'], errors='ignore').reset_index(drop=True)
    return kept, removed


def historical_keys(report_day, cumulative_df: pd.DataFrame | None = None):
    """
    Prior-day event duplicate protection.
    Same-day reruns stay visible in today's summary.
    Only prior publication dates count as historical duplicates.
    """
    if cumulative_df is None:
        if not OUT_CUMULATIVE.exists():
            return set(), set(), set()
        try:
            cumulative_df = pd.read_excel(OUT_CUMULATIVE)
        except Exception:
            return set(), set(), set()

    if cumulative_df is None or cumulative_df.empty:
        return set(), set(), set()

    old = standardize(cumulative_df)
    old_dates = pd.to_datetime(old['Date'], errors='coerce')
    prior = old.loc[old_dates.dt.date < report_day].copy()

    urls = {norm_url(x) for x in prior['URL'] if norm_url(x)}
    event_keys = {
        regulation_event_key(r)
        for _, r in prior.iterrows()
        if regulation_event_key(r)
    }
    fingerprints = {
        legal_fingerprint(r)
        for _, r in prior.iterrows()
        if legal_fingerprint(r)
    }
    return urls, event_keys, fingerprints


def safe_write(path: Path, df: pd.DataFrame):
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_excel(path, index=False)
    except PermissionError:
        alt = path.with_name(f'{path.stem}_{datetime.now():%Y%m%d_%H%M%S}{path.suffix}')
        df.to_excel(alt, index=False)
        print(f'[WARN] locked: {path.name} -> {alt.name}')

def main():
    print('GTI v6.2 STEP3-1 KST-ZERO-ROW-SAFE IDENTITY START')
    if not INPUT_FILE.exists():
        raise FileNotFoundError(INPUT_FILE)

    raw = standardize(pd.read_excel(INPUT_FILE)).dropna(how='all')
    raw = raw[raw['Headline'].astype(str).str.strip().ne('') & raw['URL'].astype(str).str.strip().ne('')].copy()
    raw['_url'] = raw['URL'].map(norm_url)
    raw['_title'] = raw['Headline'].map(norm_title)
    raw['BillNo'] = raw['URL'].map(national_assembly_bill_no)
    raw['DocumentIdentity'] = raw.apply(document_identity, axis=1)
    raw['_row_order'] = range(len(raw))
    law_source = (
        raw['URL'].fillna('').astype(str).str.contains('law.go.kr', case=False, regex=False)
        | raw['Source'].fillna('').astype(str).str.contains('law.go.kr', case=False, regex=False)
    )
    law_decision = raw['URL'].fillna('').astype(str).str.contains(
        r'alldeccsc|deccinfo|행정심판', case=False, regex=True
    )
    legal_date = raw['date_confidence'].fillna('').astype(str).str.upper().eq('LEGAL_DATE')
    untrusted_law_date = law_source & ~law_decision & ~legal_date
    if untrusted_law_date.any():
        raw.loc[untrusted_law_date, 'Date'] = pd.NaT
        raw.loc[untrusted_law_date, 'date_status'] = 'unverified_query_or_update_date'
        raw.loc[untrusted_law_date, 'date_source'] = '원문 게시일 확인 필요'
    bill_rows = raw[raw['BillNo'].ne('')].drop_duplicates('DocumentIdentity', keep='first')
    other_rows = raw[raw['BillNo'].eq('')].drop_duplicates(subset=['_url'], keep='first')
    other_rows = other_rows.drop_duplicates(subset=['_title','Agency'], keep='first')
    raw = pd.concat([bill_rows, other_rows], ignore_index=False, sort=False)
    raw = raw.sort_values('_row_order', kind='stable').drop(columns='_row_order')

    keywords = load_keywords()
    selected, excluded, audit = [], [], []

    for _, row in raw.iterrows():
        r = row.copy()
        title = clean(r['Headline'])
        hits = keyword_hits_effective(title, keywords)
        keyword_rel = bool(hits)
        strong_rel, strong_hits = strong_customs_rule(title)
        official_policy, official_policy_hits = official_trade_policy_rule(
            title, clean(r['Agency']), clean(r['Source'])
        )
        noise = any(x in norm(title) for x in NOISE_TERMS)
        negative_guard = obvious_non_customs_guard(title)

        ai_rel = False
        ai_score = 0
        ai_reason = ''

        if not strong_rel and not keyword_rel and not official_policy and not noise and not negative_guard:
            ai_rel, ai_score, ai_reason = ai_judge(title, clean(r['Agency']))
            rescue_terms = [
                '대외경제','수출입','무역','통상','외환','trade','import','export',
                'customs','tariff','duty','origin','sanction','export control',
            ]
            concrete_title = any(x in norm(title) for x in rescue_terms)
            ai_rel = bool(ai_rel and ai_score >= 80 and concrete_title)

        # Strong customs terms are hard-keep; negative guard applies only to
        # keyword/AI rescue cases.
        keep = (
            (strong_rel and not negative_guard)
            or (official_policy and not negative_guard)
            or ((keyword_rel or ai_rel) and not negative_guard)
        ) and not noise
        r['KeywordMatches'] = '; '.join(hits)
        r['StrongRuleMatches'] = '; '.join(strong_hits)
        r['StrongRuleFlag'] = 'Y' if strong_rel else 'N'
        r['OfficialPolicyMatches'] = '; '.join(official_policy_hits)
        r['OfficialPolicyFlag'] = 'Y' if official_policy else 'N'
        r['TitleKeywordFlag'] = 'Y' if hits else 'N'
        r['AIRelevant'] = 'Y' if ai_rel else 'N'
        r['AIRelevantScore'] = ai_score
        r['AIRelevantReason'] = ai_reason
        r['NegativeGuard'] = 'Y' if negative_guard else 'N'
        r['CanonicalTitle'] = canonical_regulation_title(title)
        r['BillNo'] = national_assembly_bill_no(r.get('URL', ''))
        r['DocumentIdentity'] = document_identity(r)
        r['SelectionRule'] = (
            'REJECT_NEGATIVE_GUARD' if negative_guard
            else ('OFFICIAL_TRADE_POLICY' if official_policy
                  else ('STRONG_CUSTOMS_RULE' if strong_rel
                        else ('TITLE_KEYWORD' if hits else ('AI_CUSTOMS_YES' if ai_rel else 'REJECT'))))
        )
        r['CheckedAt'] = kst_now().strftime('%Y-%m-%d %H:%M:%S')
        r['OriginalDateStatus'] = (
            'VERIFIED_LEGAL_DATE'
            if clean(r.get('date_confidence', '')).upper() == 'LEGAL_DATE'
            else ('UNVERIFIED' if pd.isna(r.get('Date')) else 'SOURCE_PUBLISHED_DATE')
        )
        mapping_type, mapping_status, entity_direct = regulation_mapping_type(r, title)
        r['RegulationMappingType'] = mapping_type
        r['MappingStatus'] = mapping_status
        r['EntityDirectFlag'] = entity_direct
        r['RequiredMappingKeys'] = (
            'SamsungEntity; Transaction' if mapping_type == 'ENTITY_DIRECT'
            else 'Product; HSCode; OriginCountry; Supplier; SamsungEntity; ImportHistory' if mapping_type == 'PRODUCT_1TO1'
            else 'Country; SamsungEntity'
        )
        audit.append(r)

        if keep:
            selected.append(r)
        else:
            r['RejectReason'] = (
                'NOISE' if noise
                else ('OBVIOUS_NON_CUSTOMS' if negative_guard else 'NO_CUSTOMS_TRADE_SIGNAL')
            )
            excluded.append(r)

    sel = pd.DataFrame(selected)
    exc = pd.DataFrame(excluded)
    aud = pd.DataFrame(audit)

    # Load and clean legacy cumulative once, before historical comparison.
    if OUT_CUMULATIVE.exists():
        try:
            legacy_cumulative = pd.read_excel(OUT_CUMULATIVE)
        except Exception:
            legacy_cumulative = pd.DataFrame()
    else:
        legacy_cumulative = pd.DataFrame()

    clean_old, cumulative_removed = clean_cumulative(legacy_cumulative, keywords)
    if len(legacy_cumulative) != len(clean_old):
        total_delta = max(0, len(legacy_cumulative) - len(clean_old))
        invalid_removed = len(cumulative_removed)
        identity_merged = max(0, total_delta - invalid_removed)
        print(
            f'[CUMULATIVE CLEAN] {len(legacy_cumulative)} -> {len(clean_old)} '
            f'/ invalid_removed={invalid_removed} / identity_merged={identity_merged}'
        )

    if not sel.empty:
        sel = same_day_dedup(sel)
        report_day = kst_now().date()
        old_urls, old_event_keys, old_fingerprints = historical_keys(report_day, clean_old)
        sel['EventType'] = sel['Headline'].apply(regulation_event_type)
        sel['EventKey'] = sel.apply(regulation_event_key, axis=1)
        sel['HistoricalDuplicateReason'] = sel.apply(
            lambda r: (
                'PRIOR_URL'
                if norm_url(r.get('URL','')) in old_urls
                else ('PRIOR_EVENT' if regulation_event_key(r) in old_event_keys
                      else ('PRIOR_FINGERPRINT' if legal_fingerprint(r) in old_fingerprints else ''))
            ),
            axis=1,
        )
        sel['HistoricalDuplicate'] = sel['HistoricalDuplicateReason'].apply(
            lambda x: 'Y' if clean(x) else 'N'
        )
        today = sel[sel['HistoricalDuplicate'].eq('N')].copy()
    else:
        today = sel.copy()

    if not today.empty:
        if 'EventType' not in today.columns:
            today['EventType'] = today['Headline'].apply(regulation_event_type)
        if 'EventKey' not in today.columns:
            today['EventKey'] = today.apply(regulation_event_key, axis=1)

        today['original_url'] = today['URL']
        today['article_body'] = ''
        today['article_extract_status'] = 'PENDING_STEP4_RECHECK'

    old_raw = clean_old.copy()
    combined = pd.concat([old_raw, today], ignore_index=True, sort=False)
    if not combined.empty:
        std = standardize(combined)
        combined['_url_key'] = std['URL'].map(norm_url).values
        combined['_event_key'] = [regulation_event_key(r) for _, r in std.iterrows()]

        # Prefer event identity over URL: the same event reposted by another
        # official site is one cumulative record; a new date/event survives.
        combined['_date_sort'] = pd.to_datetime(std['Date'], errors='coerce').values
        combined = combined.sort_values('_date_sort', ascending=False, kind='stable')
        combined = combined.drop_duplicates('_event_key', keep='first')
        combined = combined.drop(columns=['_url_key','_event_key','_date_sort'], errors='ignore')

    # A day with zero new regulations is a valid business result.  Preserve
    # the workbook schema so the pipeline can distinguish ZERO_NEW from a
    # corrupt/blank Excel file and STEP4 can safely read it.
    internal_cols = {'_url', '_title'}
    base_schema = [c for c in aud.columns if c not in internal_cols]
    for c in [
        'EventType', 'EventKey', 'HistoricalDuplicateReason',
        'HistoricalDuplicate', 'original_url', 'article_body',
        'article_extract_status',
    ]:
        if c not in base_schema:
            base_schema.append(c)

    def schema_safe(frame: pd.DataFrame) -> pd.DataFrame:
        out = frame.drop(columns=list(internal_cols), errors='ignore').copy()
        if out.empty:
            return out.reindex(columns=base_schema)
        for col in base_schema:
            if col not in out.columns:
                out[col] = ''
        return out

    safe_write(OUT_SUMMARY, schema_safe(today))
    safe_write(OUT_ARTICLE, schema_safe(today))
    safe_write(OUT_CUMULATIVE, schema_safe(combined))
    safe_write(OUT_EXCLUDED, exc.drop(columns=['_url','_title'], errors='ignore'))
    safe_write(OUT_AUDIT, aud.drop(columns=['_url','_title'], errors='ignore'))
    if not cumulative_removed.empty:
        cumulative_removed_path = BASE_DIR / '3-1.regulation_cumulative_removed.xlsx'
        safe_write(cumulative_removed_path, cumulative_removed)

    print(f'[STEP3-1] raw={len(raw)} selected={len(sel)} new={len(today)} excluded={len(exc)} cumulative={len(combined)}')
    print('GTI v6.2 STEP3-1 KST-ZERO-ROW-SAFE IDENTITY DONE')

if __name__ == '__main__':
    main()
