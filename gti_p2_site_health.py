# -*- coding: utf-8 -*-
"""P2 site-health contract for GTI STEP1.

The contract intentionally separates collection health from content novelty.
NO_NEW is allowed only when a reachable listing exposes at least one dated,
real-post candidate and every observed date is outside the collection window.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from html.parser import HTMLParser
from typing import Iterable
from urllib.parse import urljoin, urlparse


P2_CONTRACT_VERSION = "2.1.0"

OK_NEW = "OK_NEW"
NO_NEW = "NO_NEW"
BLOCKED = "BLOCKED"
PARSE_ZERO = "PARSE_ZERO"
FAIL = "FAIL"
CONFIG_ERROR = "CONFIG_ERROR"

BLOCK_HTTP_CODES = {401, 403, 407, 409, 423, 429, 451}
BLOCK_MARKERS = (
    "access denied", "request blocked", "temporarily blocked", "captcha",
    "verify you are human", "are you a human", "cloudflare ray id",
    "cf-chl-", "security check", "bot detection", "robot verification",
    "접근이 차단", "접근 권한이 없", "자동입력 방지", "보안 확인",
)

DATE_PATTERNS = (
    re.compile(r"(?<!\d)(20\d{2})[-./](\d{1,2})[-./](\d{1,2})(?!\d)"),
    re.compile(r"(?<!\d)(\d{1,2})[-./](\d{1,2})[-./](20\d{2})(?!\d)"),
)

FEDERAL_REGISTER_DOCUMENT_DATE = re.compile(
    r"https?://(?:www\.)?federalregister\.gov/documents/"
    r"(20\d{2})/(\d{2})/(\d{2})/",
    re.I,
)

NON_POST_TITLES = {
    "", "home", "menu", "news", "notice", "notices", "more", "view",
    "download", "search", "contact", "about", "공지", "공고", "목록",
    "뉴스", "검색", "더보기", "상세보기",
}


class _AnchorReader(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.items: list[dict[str, str]] = []
        self.current: dict[str, str] | None = None

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a" or self.current is not None:
            return
        values = dict(attrs)
        if values.get("href"):
            self.current = {
                "href": values.get("href", ""),
                "title": values.get("title", ""),
                "text": "",
            }

    def handle_data(self, data):
        if self.current is not None:
            self.current["text"] += " " + data

    def handle_endtag(self, tag):
        if tag.lower() == "a" and self.current is not None:
            self.items.append(self.current)
            self.current = None


@dataclass
class FetchObservation:
    url: str
    method: str = "requests"
    status_code: int | None = None
    final_url: str = ""
    content_type: str = ""
    content_length: int = 0
    elapsed_ms: int | None = None
    body_sample: str = ""
    error_type: str = ""
    error_message: str = ""
    blocked_marker: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status_code is not None and 200 <= self.status_code < 400

    @property
    def blocked(self) -> bool:
        return self.status_code in BLOCK_HTTP_CODES or bool(self.blocked_marker)


@dataclass
class ListingEvidence:
    candidate_count: int = 0
    dated_candidate_count: int = 0
    recent_candidate_count: int = 0
    old_candidate_count: int = 0
    latest_observed_date: datetime | None = None
    sample_titles: list[str] = field(default_factory=list)


@dataclass
class HealthDecision:
    status: str
    reason_code: str
    detail: str
    http_status: str = ""
    final_url: str = ""
    content_type: str = ""
    content_length: int = 0
    candidate_count: int = 0
    dated_candidate_count: int = 0
    recent_candidate_count: int = 0
    latest_observed_date: str = ""
    blocked_marker: str = ""
    error_type: str = ""
    error_message: str = ""

    def as_dict(self) -> dict:
        return self.__dict__.copy()


def detect_block_marker(text: str) -> str:
    lowered = (text or "").lower()
    for marker in BLOCK_MARKERS:
        if marker.lower() in lowered:
            return marker
    return ""


def extract_official_url_date(url: str) -> datetime | None:
    """Recover dates only from official URL schemes that encode publication date."""
    match = FEDERAL_REGISTER_DOCUMENT_DATE.search(str(url or ""))
    if not match:
        return None
    try:
        return datetime(*map(int, match.groups()))
    except ValueError:
        return None


def _parse_dates(text: str) -> list[datetime]:
    found: list[datetime] = []
    for pattern_index, pattern in enumerate(DATE_PATTERNS):
        for match in pattern.finditer(text or ""):
            try:
                if pattern_index == 0:
                    year, month, day = map(int, match.groups())
                else:
                    month, day, year = map(int, match.groups())
                found.append(datetime(year, month, day))
            except ValueError:
                continue
    return found


def _looks_like_post(title: str, href: str, source_url: str) -> bool:
    clean = re.sub(r"\s+", " ", title or "").strip()
    if len(clean) < 8 or clean.lower() in NON_POST_TITLES:
        return False
    if not href or href.lower().startswith(("#", "javascript:", "mailto:", "tel:")):
        return False
    target = urljoin(source_url, href)
    if not target.startswith(("http://", "https://")):
        return False
    if urlparse(target).netloc and urlparse(source_url).netloc:
        return True
    return False


def inspect_listing_html(
    html: str,
    source_url: str,
    now: datetime,
    hours_back: int,
    max_candidates: int = 100,
) -> ListingEvidence:
    evidence = ListingEvidence()
    if not html:
        return evidence
    reader = _AnchorReader()
    try:
        reader.feed(html)
    except Exception:
        return evidence
    cutoff = now - timedelta(hours=hours_back)
    seen: set[tuple[str, str]] = set()
    for anchor in reader.items:
        title = re.sub(r"\s+", " ", anchor.get("text", "") or anchor.get("title", "")).strip()
        href = str(anchor.get("href", "")).strip()
        if not _looks_like_post(title, href, source_url):
            continue
        key = (title.lower(), urljoin(source_url, href))
        if key in seen:
            continue
        seen.add(key)
        evidence.candidate_count += 1
        if len(evidence.sample_titles) < 3:
            evidence.sample_titles.append(title[:160])
        context = " ".join(x for x in [title, href] if x)
        dates = _parse_dates(context)
        if dates:
            observed = max(dates)
            evidence.dated_candidate_count += 1
            if evidence.latest_observed_date is None or observed > evidence.latest_observed_date:
                evidence.latest_observed_date = observed
            if observed >= cutoff:
                evidence.recent_candidate_count += 1
            else:
                evidence.old_candidate_count += 1
        if evidence.candidate_count >= max_candidates:
            break
    return evidence


def classify_site_health(
    *,
    source_url: str,
    collected_count: int,
    parsed_candidate_count: int,
    parsed_old_count: int,
    parsed_no_date_count: int,
    observations: Iterable[FetchObservation],
    listing: ListingEvidence | None,
    parser_error: str = "",
) -> HealthDecision:
    observations = list(observations)
    listing = listing or ListingEvidence()
    root = next((o for o in reversed(observations) if o.url.rstrip("/") == source_url.rstrip("/")), None)
    representative = root or (observations[-1] if observations else None)

    common = {}
    if representative:
        common = {
            "http_status": str(representative.status_code or ""),
            "final_url": representative.final_url,
            "content_type": representative.content_type,
            "content_length": representative.content_length,
            "blocked_marker": representative.blocked_marker,
            "error_type": representative.error_type,
            "error_message": representative.error_message,
        }
    common.update({
        "candidate_count": listing.candidate_count,
        "dated_candidate_count": listing.dated_candidate_count,
        "recent_candidate_count": listing.recent_candidate_count,
        "latest_observed_date": (
            listing.latest_observed_date.strftime("%Y-%m-%d")
            if listing.latest_observed_date else ""
        ),
    })

    if collected_count > 0:
        return HealthDecision(OK_NEW, "POSTS_COLLECTED", f"수집 {collected_count}건", **common)

    blocked = next((o for o in observations if o.blocked), None)
    # A parser that already extracted rows is stronger evidence than a hidden
    # CAPTCHA widget in otherwise usable HTML. Apply BLOCKED only when no rows
    # were parsed at all.
    if blocked and parsed_candidate_count == 0:
        common.update({
            "http_status": str(blocked.status_code or ""),
            "final_url": blocked.final_url,
            "blocked_marker": blocked.blocked_marker,
        })
        return HealthDecision(BLOCKED, "ACCESS_BLOCKED", "접근 차단 또는 자동화 방지 응답", **common)

    if listing.recent_candidate_count > 0:
        return HealthDecision(
            PARSE_ZERO, "RECENT_POST_NOT_PARSED",
            "최근 게시물 후보가 보이지만 수집 결과가 0건", **common,
        )
    if parsed_no_date_count > 0:
        return HealthDecision(
            PARSE_ZERO, "PARSED_POST_DATE_MISSING",
            f"게시물 {parsed_candidate_count}건을 읽었으나 게시일 미확인 {parsed_no_date_count}건",
            **common,
        )
    if (
        parsed_candidate_count > 0
        and parsed_old_count == parsed_candidate_count
    ):
        return HealthDecision(
            NO_NEW, "PARSED_ONLY_OLD_POSTS",
            f"목록 해석 성공, 확인된 {parsed_old_count}건이 모두 수집기간 이전",
            **common,
        )

    # Some dedicated parsers use urllib/Selenium/API calls rather than the
    # shared requests probe. Only declare transport failure after considering
    # the parser's own row/date evidence above.
    successful = [o for o in observations if o.succeeded]
    if not successful:
        reason = "PARSER_EXCEPTION" if parser_error else "FETCH_UNREACHABLE"
        detail = parser_error or "성공한 HTTP 응답 및 파서 행 증거 없음"
        return HealthDecision(FAIL, reason, detail, **common)
    if listing.dated_candidate_count > 0 and listing.old_candidate_count == listing.dated_candidate_count:
        return HealthDecision(
            NO_NEW, "PARSED_ONLY_OLD_POSTS",
            "목록 해석 성공, 관측된 게시물이 수집기간 이전", **common,
        )
    if listing.candidate_count > 0:
        return HealthDecision(
            PARSE_ZERO, "POST_DATE_NOT_PARSED",
            "게시물 후보는 있으나 게시일을 검증하지 못함", **common,
        )
    return HealthDecision(
        PARSE_ZERO, "LISTING_STRUCTURE_NOT_PARSED",
        "페이지 응답은 성공했으나 실제 게시물 구조를 확인하지 못함", **common,
    )


def final_content_status(health_status: str, valid_count: int, raw_count: int) -> str:
    if health_status in {BLOCKED, PARSE_ZERO, FAIL, CONFIG_ERROR}:
        return health_status
    if valid_count > 0:
        return "VALID_REGULATION"
    if raw_count > 0:
        return "ALL_FILTERED"
    return NO_NEW
