def run_sensor_build_df() -> pd.DataFrame:
    """
    Google News RSS 기반 '관세' 관련 뉴스 수집 → DF 생성
    """
    import feedparser
    import urllib.parse

    query = os.getenv("NEWS_QUERY", "관세")

    rss = "https://news.google.com/rss/search?" + urllib.parse.urlencode({
        "q": query,
        "hl": "ko",
        "gl": "KR",
        "ceid": "KR:ko"
    })

    feed = feedparser.parse(rss)

    rows = []
    for e in feed.entries[:30]:
        title = getattr(e, "title", "").strip()
        link = getattr(e, "link", "").strip()
        published = getattr(e, "published", "")

        summary = getattr(e, "summary", "")
        summary = re.sub(r"<[^>]+>", "", summary).strip()

        rows.append({
            "제시어": query,
            "헤드라인": title,
            "주요내용": summary[:500],
            "대상 국가": "",
            "중요도": "중",
            "발표일": published,
            "출처(URL)": link,
            "근거건수": 1
        })

    return pd.DataFrame(rows)
