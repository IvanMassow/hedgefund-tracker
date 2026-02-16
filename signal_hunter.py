"""
Hedge Fund Edge Tracker - Signal Hunter
Active signal propagation detection. Searches for evidence that a position's
thesis catalyst is gaining traction in news and media.

Uses two free data sources:
1. Alpha Vantage News Sentiment API (already have the key, ticker-filtered)
2. Google News RSS (free, no key, query-filtered)

The system knows WHAT stock to watch and WHAT signal to watch for.
This module detects WHEN that signal starts to bite.
"""
import json
import logging
import time
import urllib.parse
from datetime import datetime, timezone, timedelta

import requests
from xml.etree import ElementTree

from db import get_conn
from config import ALPHA_VANTAGE_KEY, ALPHA_VANTAGE_BASE, AV_RATE_LIMIT, OPENAI_API_KEY

logger = logging.getLogger("hedgefund.signal_hunter")

# Major wire services / mainstream outlets — signal has gone mainstream when these cover it
MAJOR_SOURCES = {
    "reuters", "bloomberg", "associated press", "ap news", "financial times",
    "wall street journal", "wsj", "cnbc", "bbc", "new york times", "nyt",
    "marketwatch", "barrons", "the economist", "fortune", "forbes",
    "washington post", "the guardian", "abc news", "cbs news", "nbc news",
}

# Rate limit tracking for Alpha Vantage News (25 free requests/day)
_av_news_calls_today = 0
_av_news_date = None
AV_NEWS_DAILY_LIMIT = 22  # Leave a few in reserve


def _reset_av_counter():
    """Reset AV news call counter at start of new day."""
    global _av_news_calls_today, _av_news_date
    today = datetime.now(timezone.utc).date()
    if _av_news_date != today:
        _av_news_calls_today = 0
        _av_news_date = today


def _av_quota_available():
    """Check if we have AV news calls left today."""
    _reset_av_counter()
    return _av_news_calls_today < AV_NEWS_DAILY_LIMIT


def get_scannable_positions():
    """Get all WATCH, ACTIVE, and PUBLISH positions eligible for signal scanning.

    WATCH positions are the most important to scan — we need to detect
    when their signal starts propagating so the bot can decide to ENTER.
    """
    conn = get_conn()
    rows = conn.execute("""
        SELECT c.*, r.published_date
        FROM candidates c
        JOIN reports r ON c.report_id = r.report_id
        WHERE c.state IN ('WATCH', 'ACTIVE', 'PUBLISH')
            AND c.is_active = 1
            AND c.primary_ticker IS NOT NULL
            AND c.primary_ticker != ''
        ORDER BY c.confidence_pct DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def generate_signal_query(candidate):
    """Use GPT-4o-mini to generate a focused search query from the thesis.

    Called once per position, result stored on the candidate for reuse.
    Returns a 3-5 word search query string.
    """
    api_key = OPENAI_API_KEY or ""
    if not api_key:
        # Fallback: build a basic query from ticker + first few words of mechanism
        mechanism = (candidate.get("mechanism") or candidate.get("headline") or "")
        ticker = candidate.get("primary_ticker", "")
        words = mechanism.split()[:4]
        return "{} {}".format(ticker, " ".join(words))

    mechanism = candidate.get("mechanism") or "N/A"
    tripwire = candidate.get("tripwire") or "N/A"
    headline = candidate.get("headline") or "N/A"
    ticker = candidate.get("primary_ticker", "?")

    prompt = (
        "Generate a focused 3-5 word Google News search query to find articles "
        "about this specific signal.\n\n"
        "Ticker: {ticker}\n"
        "Headline: {headline}\n"
        "Mechanism: {mechanism}\n"
        "Tripwire/Catalyst: {tripwire}\n\n"
        "The query should find articles confirming or denying this SPECIFIC catalyst, "
        "not general stock news. Return JSON: {{\"query\": \"...\"}}"
    ).format(ticker=ticker, headline=headline[:100],
             mechanism=mechanism[:200], tripwire=tripwire[:150])

    try:
        import requests as http_requests
        resp = http_requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": "Bearer {}".format(api_key),
                "Content-Type": "application/json",
            },
            json={
                "model": "gpt-4o-mini",
                "max_tokens": 100,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": "You generate concise search queries. Return JSON only."},
                    {"role": "user", "content": prompt}
                ]
            },
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        result = json.loads(content)
        query = result.get("query", "")
        if query:
            logger.info("Generated signal query for {} ({}): '{}'".format(
                candidate["asset_theme"][:30], ticker, query))
            return query
    except Exception as e:
        logger.warning("Signal query generation failed for {}: {}".format(ticker, e))

    # Fallback
    mechanism = (candidate.get("mechanism") or candidate.get("headline") or "")
    words = mechanism.split()[:4]
    return "{} {}".format(ticker, " ".join(words))


def fetch_av_news(ticker, time_from=None):
    """Fetch news articles from Alpha Vantage News Sentiment API.

    Returns list of article dicts: {title, url, source, published_at, sentiment, relevance}
    """
    global _av_news_calls_today

    if not ALPHA_VANTAGE_KEY:
        return []

    if not _av_quota_available():
        logger.debug("AV News daily quota exhausted, skipping {}".format(ticker))
        return []

    params = {
        "function": "NEWS_SENTIMENT",
        "tickers": ticker,
        "sort": "LATEST",
        "limit": 10,
        "apikey": ALPHA_VANTAGE_KEY,
    }

    if time_from:
        # Format: YYYYMMDDTHHMM
        params["time_from"] = time_from.strftime("%Y%m%dT%H%M")

    try:
        resp = requests.get(ALPHA_VANTAGE_BASE, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        _av_news_calls_today += 1

        # Check for rate limit / error messages
        if "Note" in data or "Information" in data:
            logger.warning("AV News rate limit for {}: {}".format(
                ticker, data.get("Note", data.get("Information", ""))[:100]))
            return []

        articles = []
        for item in data.get("feed", []):
            # Find ticker-specific sentiment
            ticker_sentiment = None
            relevance = 0
            for ts in item.get("ticker_sentiment", []):
                if ts.get("ticker", "").upper() == ticker.upper():
                    ticker_sentiment = float(ts.get("ticker_sentiment_score", 0))
                    relevance = float(ts.get("relevance_score", 0))
                    break

            articles.append({
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "source": item.get("source", ""),
                "published_at": item.get("time_published", ""),
                "sentiment": ticker_sentiment or float(item.get("overall_sentiment_score", 0)),
                "relevance": relevance,
                "summary": (item.get("summary") or "")[:200],
            })

        logger.info("AV News for {}: {} articles (quota: {}/{})".format(
            ticker, len(articles), _av_news_calls_today, AV_NEWS_DAILY_LIMIT))
        return articles

    except Exception as e:
        logger.warning("AV News fetch failed for {}: {}".format(ticker, e))
        return []


def fetch_google_news(query, max_results=15):
    """Fetch news articles from Google News RSS.

    Free, no API key. Returns list of article dicts: {title, url, source, published_at}
    """
    encoded = urllib.parse.quote(query)
    url = "https://news.google.com/rss/search?q={}&hl=en-US&gl=US&ceid=US:en".format(encoded)

    try:
        resp = requests.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (compatible; HedgeFundTracker/1.0)"
        })
        resp.raise_for_status()

        root = ElementTree.fromstring(resp.content)
        articles = []

        for item in root.findall(".//item"):
            title = item.findtext("title", "")
            link = item.findtext("link", "")
            pub_date = item.findtext("pubDate", "")
            source_elem = item.find("source")
            source = source_elem.text if source_elem is not None else ""

            if title:
                articles.append({
                    "title": title,
                    "url": link,
                    "source": source,
                    "published_at": pub_date,
                    "sentiment": None,
                    "relevance": None,
                })

            if len(articles) >= max_results:
                break

        logger.info("Google News for '{}': {} articles".format(query[:40], len(articles)))
        return articles

    except Exception as e:
        logger.warning("Google News fetch failed for '{}': {}".format(query[:40], e))
        return []


def _is_major_source(source_name):
    """Check if a source is a major wire service / mainstream outlet."""
    if not source_name:
        return False
    lower = source_name.lower()
    for ms in MAJOR_SOURCES:
        if ms in lower:
            return True
    return False


def _dedupe_articles(articles):
    """Remove duplicate articles by URL."""
    seen = set()
    unique = []
    for a in articles:
        url = a.get("url", "")
        if url and url not in seen:
            seen.add(url)
            unique.append(a)
        elif not url:
            unique.append(a)
    return unique


def store_scan_results(candidate_id, articles, source_label):
    """Store scan results in signal_scans table. Skips duplicates by URL."""
    if not articles:
        return 0

    conn = get_conn()
    stored = 0
    for a in articles:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO signal_scans
                (candidate_id, source, article_title, article_url,
                 article_source, published_at, sentiment_score, relevance_score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                candidate_id, source_label,
                a.get("title", "")[:300],
                a.get("url", ""),
                a.get("source", "")[:100],
                a.get("published_at", ""),
                a.get("sentiment"),
                a.get("relevance"),
            ))
            stored += 1
        except Exception as e:
            logger.debug("Skip duplicate or error storing article: {}".format(e))

    conn.commit()
    conn.close()
    return stored


def compute_velocity(candidate_id):
    """Compute signal propagation velocity from recent scan results.

    Looks at articles from the last 24 hours.
    Returns (velocity_label, hit_count, has_major_source).

    Velocity levels:
    - quiet: 0 relevant hits
    - stirring: 1-2 hits from niche sources
    - propagating: 3-5 hits, or any from major wire services
    - mainstream: 6+ hits, or 2+ from major outlets
    """
    conn = get_conn()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    articles = conn.execute("""
        SELECT article_title, article_source, published_at, sentiment_score
        FROM signal_scans
        WHERE candidate_id = ? AND timestamp > ?
        ORDER BY timestamp DESC
    """, (candidate_id, cutoff)).fetchall()
    conn.close()

    if not articles:
        return "quiet", 0, False

    total = len(articles)
    major_count = sum(1 for a in articles if _is_major_source(a["article_source"]))

    if total >= 6 or major_count >= 2:
        return "mainstream", total, True
    elif total >= 3 or major_count >= 1:
        return "propagating", total, True if major_count > 0 else False
    elif total >= 1:
        return "stirring", total, False
    else:
        return "quiet", 0, False


def update_candidate_velocity(candidate_id, velocity, hits):
    """Update the candidate's signal velocity and hit count."""
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        UPDATE candidates SET
            signal_velocity = ?,
            signal_hits_24h = ?,
            last_signal_scan_at = ?
        WHERE id = ?
    """, (velocity, hits, now, candidate_id))
    conn.commit()
    conn.close()


def build_signal_context(candidate_id):
    """Build a text summary of recent signal scan findings for GPT context.

    Returns a string to be inserted into the position monitor prompt.
    """
    conn = get_conn()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    articles = conn.execute("""
        SELECT article_title, article_source, published_at, sentiment_score,
               source as scan_source
        FROM signal_scans
        WHERE candidate_id = ? AND timestamp > ?
        ORDER BY timestamp DESC
        LIMIT 8
    """, (candidate_id, cutoff)).fetchall()

    # Get velocity
    candidate = conn.execute(
        "SELECT signal_velocity, signal_hits_24h FROM candidates WHERE id = ?",
        (candidate_id,)
    ).fetchone()
    conn.close()

    velocity = candidate["signal_velocity"] if candidate else "quiet"
    hits = candidate["signal_hits_24h"] if candidate else 0

    if not articles and velocity == "quiet":
        return "Signal velocity: QUIET (0 articles in 24h). No mainstream coverage detected. Your edge appears intact."

    parts = []
    parts.append("Signal velocity: {} ({} articles in 24h)".format(
        velocity.upper(), hits))

    # Velocity interpretation
    if velocity == "quiet":
        parts.append("Interpretation: The market has not caught on to this signal. Edge is intact.")
    elif velocity == "stirring":
        parts.append("Interpretation: Early signs of awareness. Niche sources covering this. Edge still live but clock is ticking.")
    elif velocity == "propagating":
        parts.append("Interpretation: Signal is spreading to wider audience. Edge window is closing. Consider TAKE_PROFIT if profitable.")
    elif velocity == "mainstream":
        parts.append("Interpretation: Major outlets covering this. The edge is likely captured. Strongly consider TAKE_PROFIT.")

    if articles:
        parts.append("")
        parts.append("Recent articles:")
        for i, a in enumerate(articles[:6]):
            a = dict(a)
            source = a.get("article_source") or a.get("scan_source") or "Unknown"
            major_tag = " [MAJOR]" if _is_major_source(source) else ""
            sentiment = a.get("sentiment_score")
            sent_str = ""
            if sentiment is not None:
                sent_str = " (sentiment: {:.2f})".format(sentiment)
            parts.append("  {}. \"{}\" - {}{}{}".format(
                i + 1,
                (a.get("article_title") or "?")[:80],
                source,
                major_tag,
                sent_str
            ))

    return "\n".join(parts)


def scan_position(candidate):
    """Run a full signal scan for one position.

    Steps:
    1. Ensure signal_query exists (generate if needed)
    2. Fetch Alpha Vantage News (ticker-based)
    3. Fetch Google News RSS (query-based)
    4. Dedupe and store results
    5. Compute and update velocity
    """
    cid = candidate["id"]
    ticker = candidate["primary_ticker"]
    signal_query = candidate.get("signal_query")

    # 1. Generate query if needed
    if not signal_query:
        signal_query = generate_signal_query(candidate)
        conn = get_conn()
        conn.execute("UPDATE candidates SET signal_query = ? WHERE id = ?",
                     (signal_query, cid))
        conn.commit()
        conn.close()

    all_articles = []

    # 2. Alpha Vantage News (ticker-filtered, rate-limited)
    if _av_quota_available():
        last_scan = candidate.get("last_signal_scan_at")
        time_from = None
        if last_scan:
            try:
                time_from = datetime.fromisoformat(last_scan)
                if time_from.tzinfo is None:
                    time_from = time_from.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                pass

        av_articles = fetch_av_news(ticker, time_from)
        for a in av_articles:
            a["scan_source"] = "alpha_vantage"
        all_articles.extend(av_articles)
        time.sleep(AV_RATE_LIMIT)  # Respect rate limit between calls

    # 3. Google News RSS (query-based)
    gn_articles = fetch_google_news(signal_query)
    for a in gn_articles:
        a["scan_source"] = "google_news"
    all_articles.extend(gn_articles)

    # 4. Dedupe and store
    unique = _dedupe_articles(all_articles)
    for a in unique:
        store_scan_results(cid, [a], a.get("scan_source", "unknown"))

    # 5. Compute velocity
    velocity, hits, has_major = compute_velocity(cid)
    update_candidate_velocity(cid, velocity, hits)

    logger.info("Signal scan {} ({}): {} articles, velocity={}, hits_24h={}".format(
        candidate["asset_theme"][:30], ticker, len(unique), velocity, hits))

    return velocity, hits


def run_signal_scan():
    """Top-level function: scan all eligible positions for signal propagation.

    Called from runner.py on SIGNAL_SCAN_INTERVAL (1 hour).
    Returns the number of positions scanned.
    """
    positions = get_scannable_positions()
    if not positions:
        logger.info("No active positions to signal-scan")
        return 0

    logger.info("Signal hunting: {} eligible positions".format(len(positions)))
    scanned = 0

    for pos in positions:
        try:
            velocity, hits = scan_position(pos)
            scanned += 1

            if velocity in ("propagating", "mainstream"):
                logger.warning("SIGNAL ALERT: {} ({}) velocity={} hits={}".format(
                    pos["asset_theme"][:30], pos["primary_ticker"], velocity, hits))

        except Exception as e:
            logger.error("Signal scan failed for {} ({}): {}".format(
                pos["asset_theme"][:30], pos["primary_ticker"], e
            ), exc_info=True)

    logger.info("Signal hunting complete: {}/{} scanned".format(scanned, len(positions)))
    return scanned


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    from db import init_db

    init_db()

    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        # Test with a specific ticker
        ticker = sys.argv[2] if len(sys.argv) > 2 else "AAPL"
        print("Testing signal hunt for {}...".format(ticker))

        # Test Google News
        articles = fetch_google_news("{} litigation".format(ticker))
        print("Google News: {} articles".format(len(articles)))
        for a in articles[:3]:
            print("  - {} ({})".format(a["title"][:60], a["source"]))

        # Test AV News
        if ALPHA_VANTAGE_KEY:
            av = fetch_av_news(ticker)
            print("AV News: {} articles".format(len(av)))
            for a in av[:3]:
                print("  - {} ({}, sentiment={})".format(
                    a["title"][:60], a["source"], a.get("sentiment")))
    else:
        n = run_signal_scan()
        print("Scanned {} positions".format(n))
