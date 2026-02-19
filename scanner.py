"""
Hedge Fund Edge Tracker - Report Scanner
Polls the RSS feed for new pharma risk reports, parses candidates,
and stores everything in the database.
"""
import re
import json
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from html.parser import HTMLParser

import requests

from db import get_conn, init_db
from config import RSS_URL, REPORT_TITLE_PREFIX, BANDS, TRACKING_WINDOW_HOURS
try:
    from config import RSS_FEEDS
except ImportError:
    RSS_FEEDS = [RSS_URL]

logger = logging.getLogger("hedgefund.scanner")


class HTMLStripper(HTMLParser):
    """Strip HTML tags and return plain text."""
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)

    def get_text(self):
        return "".join(self.parts)


def strip_html(html_str):
    s = HTMLStripper()
    s.feed(html_str or "")
    return s.get_text().strip()


def assign_band(confidence_pct):
    """Assign A-E band based on confidence percentage."""
    if confidence_pct is None:
        return "E", BANDS["E"]["label"]
    for band_key in ["A", "B", "C", "D", "E"]:
        b = BANDS[band_key]
        if b["min"] <= confidence_pct <= b["max"]:
            return band_key, b["label"]
    return "E", BANDS["E"]["label"]


def fetch_rss(feed_url=None):
    """Fetch and parse a single RSS feed. Returns list of items."""
    url = feed_url or RSS_URL
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    items = []
    for item in root.findall(".//item"):
        title = item.findtext("title", "")
        # Only process Information Asymmetry reports
        if not title.startswith(REPORT_TITLE_PREFIX):
            continue
        items.append({
            "title": title,
            "link": item.findtext("link", ""),
            "description": item.findtext("description", ""),
            "guid": item.findtext("guid", ""),
            "pubDate": item.findtext("pubDate", ""),
            "_feed_url": url,
        })
    return items


def fetch_all_rss():
    """Fetch and parse ALL configured RSS feeds. Returns combined list of items."""
    all_items = []
    for feed_url in RSS_FEEDS:
        try:
            items = fetch_rss(feed_url)
            all_items.extend(items)
            logger.info("Feed {}: {} Information Asymmetry reports".format(
                feed_url.split("//")[1].split("/")[0], len(items)))
        except Exception as e:
            logger.error("RSS fetch failed for {}: {}".format(feed_url, e))
    return all_items


def extract_cycle_id(title):
    """Extract cycle ID like '20260214-2112' from title."""
    m = re.search(r'\((\d{8}-\d{4})\)\s*$', title)
    return m.group(1) if m else None


def extract_report_grade(title):
    """Extract report-level grade like 'B' from title '... - B (20260214-2112)'."""
    m = re.search(r'-\s+([A-E]|HIGH|LOW)\s+\(', title)
    return m.group(1) if m else None


def parse_pubdate(pubdate_str):
    """Parse RSS pubDate format like 'Sat, 14 Feb 2026 21:46:51 +0000'."""
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(pubdate_str)
    except Exception:
        return datetime.now(timezone.utc)


def fetch_report_html(url):
    """Fetch the full report HTML from the report URL."""
    try:
        # Ensure https
        url = url.replace("http://", "https://")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.error("Failed to fetch report HTML from {}: {}".format(url, e))
        return None


def parse_decision_table(html):
    """Parse the decision panel table from the report HTML.

    Report format has TWO table layouts:
    1. Summary table (8 cols): Asset/Theme, Ticker, Price, Call, Confidence, Market Condition, Publish?, Action
    2. Per-position tables (11 cols): Rank, Opportunity, Instrument, Price, Direction, Conviction, Edge, Market Regime, Publish?, Fresh, Action

    We prefer the per-position tables as they have more data (rank, edge, freshness).
    If only the summary table exists, we use that.
    """
    candidates = []
    seen_assets = set()

    # Find ALL tables in the HTML
    all_tables = re.findall(r'<table[^>]*>(.*?)</table>', html, re.DOTALL | re.IGNORECASE)

    for table_html in all_tables:
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL)
        if len(rows) < 2:
            continue

        # Parse header to determine column mapping
        header_cells = re.findall(r'<t[hd][^>]*>(.*?)</t[hd]>', rows[0], re.DOTALL)
        headers = [strip_html(h).strip().lower() for h in header_cells]

        # Build column index map
        col_map = {}
        for idx, h in enumerate(headers):
            if h in ('rank',):
                col_map['rank'] = idx
            elif h in ('asset / theme', 'asset/theme', 'opportunity'):
                col_map['asset'] = idx
            elif h in ('ticker', 'ticker(s)', 'instrument'):
                col_map['ticker'] = idx
            elif h in ('price', 'price(s)'):
                col_map['price'] = idx
            elif h in ('call', 'direction'):
                col_map['direction'] = idx
            elif h in ('confidence', 'conviction'):
                col_map['confidence'] = idx
            elif h in ('edge',):
                col_map['edge'] = idx
            elif h in ('action',):
                col_map['action'] = idx
            elif h in ('fresh',):
                col_map['fresh'] = idx

        # Must have at least asset and some signal columns
        if 'asset' not in col_map and 'ticker' not in col_map:
            continue

        # Parse data rows
        for row_html in rows[1:]:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL)
            if len(cells) < 4:
                continue

            candidate = _parse_mapped_row(cells, col_map)
            if candidate:
                # Deduplicate by asset theme
                key = candidate['asset_theme'][:30]
                if key not in seen_assets:
                    seen_assets.add(key)
                    candidates.append(candidate)

    # Fallback: markdown pipe-table
    if not candidates:
        pipe_rows = re.findall(r'^\|(.+)\|$', html, re.MULTILINE)
        if len(pipe_rows) >= 3:
            for row_str in pipe_rows[2:]:
                cells = [c.strip() for c in row_str.split('|')]
                if len(cells) >= 6:
                    candidate = _parse_pipe_row(cells)
                    if candidate:
                        candidates.append(candidate)

    return candidates


def _parse_mapped_row(cells, col_map):
    """Parse a table row using the header-derived column mapping."""
    try:
        cells_text = [strip_html(c) for c in cells]

        # Rank
        rank = 0
        if 'rank' in col_map and col_map['rank'] < len(cells_text):
            rank_text = cells_text[col_map['rank']].strip()
            m = re.search(r'\d+', rank_text)
            if m:
                rank = int(m.group())

        # Asset/theme
        asset_theme = ""
        if 'asset' in col_map and col_map['asset'] < len(cells_text):
            asset_theme = cells_text[col_map['asset']].strip()

        # Ticker
        tickers_raw = ""
        if 'ticker' in col_map and col_map['ticker'] < len(cells_text):
            tickers_raw = cells_text[col_map['ticker']].strip()
        tickers = re.sub(r'\s+', '', tickers_raw).replace(';', ',')
        primary_ticker = tickers.split(',')[0].strip() if tickers and tickers != '-' and tickers != '\u2013' else ""

        # Price
        price_raw = ""
        if 'price' in col_map and col_map['price'] < len(cells_text):
            price_raw = cells_text[col_map['price']].strip()

        # Direction — may contain "SHORT 69%" or just "SHORT"
        direction = "MIXED"
        if 'direction' in col_map and col_map['direction'] < len(cells_text):
            dir_text = cells_text[col_map['direction']].strip().upper()
            for d in ("SHORT", "LONG", "MIXED", "FADE"):
                if d in dir_text:
                    direction = d
                    break

        # Confidence
        confidence = 0
        if 'confidence' in col_map and col_map['confidence'] < len(cells_text):
            conf_text = cells_text[col_map['confidence']].strip()
            m = re.search(r'(\d+)', conf_text)
            if m:
                confidence = float(m.group(1))
        # Also try extracting from direction cell if it contains "SHORT 69%"
        if confidence == 0 and 'direction' in col_map and col_map['direction'] < len(cells_text):
            dir_text = cells_text[col_map['direction']].strip()
            m = re.search(r'(\d+)%?', dir_text)
            if m:
                val = float(m.group(1))
                if 20 <= val <= 100:
                    confidence = val

        # Edge quality
        edge = "HIGH"
        if 'edge' in col_map and col_map['edge'] < len(cells_text):
            edge_text = cells_text[col_map['edge']].strip().upper()
            if edge_text in ("HIGH", "DECAYING", "MEDIUM", "LOW"):
                edge = edge_text

        # Action
        action = "TRADE"
        if 'action' in col_map and col_map['action'] < len(cells_text):
            action_text = cells_text[col_map['action']].strip().upper()
            if action_text in ("TRADE", "AVOID", "INVESTIGATE"):
                action = action_text

        # Freshness
        freshness = None
        if 'fresh' in col_map and col_map['fresh'] < len(cells_text):
            fresh_text = cells_text[col_map['fresh']].strip()
            m = re.search(r'(\d+)', fresh_text)
            if m:
                freshness = float(m.group(1))

        # Parse prices
        prices = {}
        changes = {}
        # Multi-ticker format: "IHE $90.42 (+0.4%); XBI $122.86 (-0.3%)"
        price_parts = re.findall(r'(\w+)\s+\$?([\d.]+)\s*\(([+-]?[\d.]+)%\)', price_raw)
        if price_parts:
            for ticker, price, change in price_parts:
                prices[ticker] = float(price)
                changes[ticker] = float(change)
        else:
            # Simple format: "$12.24 (+7.9%)"
            simple = re.findall(r'\$?([\d.]+)\s*\(([+-]?[\d.]+)%\)', price_raw)
            if simple and primary_ticker:
                prices[primary_ticker] = float(simple[0][0])
                changes[primary_ticker] = float(simple[0][1])

        if not asset_theme:
            return None

        # Non-investable
        if primary_ticker == "" or primary_ticker == "-" or primary_ticker == "\u2013":
            action = "AVOID"

        result = {
            "rank": rank,
            "asset_theme": asset_theme,
            "tickers": tickers,
            "primary_ticker": primary_ticker,
            "prices_at_report": json.dumps(prices) if prices else "{}",
            "price_changes_at_report": json.dumps(changes) if changes else "{}",
            "direction": direction,
            "confidence_pct": confidence,
            "edge_quality": edge,
            "action": action,
        }
        if freshness is not None:
            result["freshness_score"] = freshness

        return result
    except Exception as e:
        logger.warning("Failed to parse table row: {}".format(e))
        return None


def _parse_pipe_row(cells):
    """Parse a markdown pipe-table row."""
    try:
        if len(cells) < 6:
            return None

        rank = int(cells[0]) if cells[0].isdigit() else 0
        asset_theme = cells[1].strip()
        tickers = cells[2].strip()
        primary_ticker = tickers.split(',')[0].strip() if tickers and tickers != '-' else ""
        price_raw = cells[3].strip()

        prices = {}
        changes = {}
        simple = re.findall(r'\$?([\d.]+)\s*\(([+-]?[\d.]+)%\)', price_raw)
        if simple and primary_ticker:
            prices[primary_ticker] = float(simple[0][0])
            changes[primary_ticker] = float(simple[0][1])

        direction = cells[4].strip().upper() if len(cells) > 4 else "MIXED"
        confidence = 0
        if len(cells) > 5:
            conf_str = cells[5].strip().rstrip('%')
            try:
                confidence = float(conf_str)
            except ValueError:
                pass

        edge = cells[6].strip().upper() if len(cells) > 6 else "HIGH"
        action = cells[7].strip().upper() if len(cells) > 7 else "TRADE"

        if not asset_theme:
            return None

        if primary_ticker == "" or primary_ticker == "-":
            action = "AVOID"

        return {
            "rank": rank,
            "asset_theme": asset_theme,
            "tickers": tickers,
            "primary_ticker": primary_ticker,
            "prices_at_report": json.dumps(prices) if prices else "{}",
            "price_changes_at_report": json.dumps(changes) if changes else "{}",
            "direction": direction if direction in ("SHORT", "LONG", "MIXED", "FADE") else "MIXED",
            "confidence_pct": confidence,
            "edge_quality": edge if edge in ("HIGH", "DECAYING", "MEDIUM", "LOW") else "HIGH",
            "action": action if action in ("TRADE", "AVOID", "INVESTIGATE") else "TRADE",
        }
    except Exception as e:
        logger.warning("Failed to parse pipe row: {}".format(e))
        return None


def parse_position_details(html):
    """Parse detailed position analysis cards from the report HTML.

    The report structure per position is:
    1. <h2> heading (thesis headline)
    2. <table> with rank, ticker, direction, etc.
    3. <p> blocks with bold labels: "The Opportunity:", "The Timing:", "The Evidence:"

    Returns dict keyed by rank number.
    """
    details = {}

    # Split the article body into sections by <hr /> boundaries
    # Each position section sits between two <hr /> tags
    hr_sections = re.split(r'<hr\s*/?\s*>', html)

    for section in hr_sections:
        # Check if this section contains a per-position table (has Rank column)
        rank_match = re.search(
            r'<td[^>]*>\s*(\d+)\s*</td>',
            section, re.DOTALL
        )
        if not rank_match:
            continue

        rank_num = int(rank_match.group(1))
        detail = {}

        # Headline from the <h2> in this section
        # Skip generic headings like "Our Analysis", "Decision Panel"
        skip_headings = {"our analysis", "decision panel", "market regime",
                         "appendix", "methodology", "disclaimer"}
        h2_matches = re.findall(r'<h2[^>]*>(.*?)</h2>', section, re.DOTALL)
        for h2_html in h2_matches:
            h2_text = strip_html(h2_html)
            if h2_text.lower() not in skip_headings and len(h2_text) > 10:
                detail["headline"] = h2_text
                break

        # Extract labelled paragraphs: "The Opportunity:", "The Timing:", "The Evidence:"
        # These are <p> tags with <strong> labels
        paragraphs = re.findall(r'<p>(.*?)</p>', section, re.DOTALL)

        for p_html in paragraphs:
            p_text = strip_html(p_html)

            if p_text.startswith("The Opportunity:"):
                detail["mechanism"] = p_text[len("The Opportunity:"):].strip()

            elif p_text.startswith("The Timing:"):
                timing_text = p_text[len("The Timing:"):].strip()
                detail["tripwire"] = timing_text
                # Also extract propagation posture
                prop = re.search(r'(IGNITE|CATALYTIC|SILENT|FRAGILE)', timing_text, re.IGNORECASE)
                if prop:
                    detail["propagation"] = prop.group(1).upper()
                # Extract freshness score
                fresh = re.search(r'Freshness\s+(?:is\s+)?(\d+)', timing_text, re.IGNORECASE)
                if not fresh:
                    fresh = re.search(r'Fresh(?:ness)?\s+(\d+)', timing_text, re.IGNORECASE)
                if fresh:
                    detail["freshness_score"] = float(fresh.group(1))

            elif p_text.startswith("The Evidence:"):
                detail["evidence"] = p_text[len("The Evidence:"):].strip()

            elif p_text.startswith("The Risk") or p_text.startswith("Key Risk"):
                detail["risks"] = p_text.split(":", 1)[-1].strip()

        # If no explicit risk paragraph, try to extract risks from timing text
        if "risks" not in detail and "tripwire" in detail:
            risk_match = re.search(
                r'(?:risk|whipsaw|downside)[^.]*\.',
                detail["tripwire"], re.IGNORECASE
            )
            if risk_match:
                detail["risks"] = risk_match.group(0)

        # Also extract the asset name from the per-position table for name-based matching
        asset_match = re.search(
            r'<td[^>]*>\s*\d+\s*</td>\s*<td[^>]*>(.*?)</td>',
            section, re.DOTALL
        )
        if asset_match:
            detail["_asset_name"] = strip_html(asset_match.group(1)).strip()

        if detail:
            details[rank_num] = detail
            logger.debug("Parsed details for rank {}: {} keys".format(rank_num, len(detail)))

    return details


def parse_market_regime(html):
    """Extract market regime data from the report."""
    regime = {}

    # Market regime value
    mr = re.search(r'(?:Market\s+Regime|Regime)[^:]*[:]\s*([\w\s]+\d+)', html, re.IGNORECASE)
    if mr:
        regime["market_regime"] = mr.group(1).strip()

    # Bull wind
    bw = re.search(r'Bull\s+Wind[^:]*[:]\s*(\d+)', html, re.IGNORECASE)
    if bw:
        regime["bull_wind"] = int(bw.group(1))

    # Bear wind
    bew = re.search(r'Bear\s+Wind[^:]*[:]\s*(\d+)', html, re.IGNORECASE)
    if bew:
        regime["bear_wind"] = int(bew.group(1))

    # Crosswind risk
    cw = re.search(r'Crosswind\s+Risk[^:]*[:]\s*(\d+)', html, re.IGNORECASE)
    if cw:
        regime["crosswind_risk"] = int(cw.group(1))

    # SPY price
    spy = re.search(r'SPY\s+\$?([\d.]+)\s*\(([+-]?[\d.]+)%\)', html, re.IGNORECASE)
    if spy:
        regime["spy_price"] = float(spy.group(1))
        regime["spy_change"] = float(spy.group(2))

    return regime


def check_position_continuity(candidate, conn):
    """Check if this candidate's ticker already has an active position.
    Returns 'new', 'momentum', or 'reversal' with the existing candidate id.
    """
    primary_ticker = candidate.get("primary_ticker")
    if not primary_ticker:
        return "new", None

    existing = conn.execute("""
        SELECT id, direction, state, asset_theme
        FROM candidates
        WHERE primary_ticker = ? AND is_active = 1 AND state != 'EXPIRED'
        ORDER BY discovered_at DESC LIMIT 1
    """, (primary_ticker,)).fetchone()

    if not existing:
        return "new", None

    old_dir = existing["direction"]
    new_dir = candidate.get("direction")

    if old_dir == new_dir:
        return "momentum", existing["id"]
    else:
        return "reversal", existing["id"]


def ingest_report(item):
    """Ingest a single RSS item: fetch report, parse candidates, store in DB.
    Returns number of new candidates inserted.
    """
    conn = get_conn()
    guid = item["guid"]

    # Check if already ingested
    existing = conn.execute(
        "SELECT 1 FROM reports WHERE rss_guid = ?", (guid,)
    ).fetchone()
    if existing:
        conn.close()
        return 0

    title = item["title"]
    link = item["link"]
    pubdate = parse_pubdate(item["pubDate"])
    cycle_id = extract_cycle_id(title)
    report_grade = extract_report_grade(title)

    logger.info("Ingesting report: {}".format(title[:80]))

    # Fetch full report HTML
    html = fetch_report_html(link)
    if not html:
        logger.error("Could not fetch report HTML, skipping")
        conn.close()
        return 0

    # Parse decision table
    table_candidates = parse_decision_table(html)
    if not table_candidates:
        # Try parsing from the description HTML
        desc_html = item.get("description", "")
        table_candidates = parse_decision_table(desc_html)

    if not table_candidates:
        logger.warning("No candidates found in report, storing report metadata only")

    # Parse position details
    position_details = parse_position_details(html)

    # Parse market regime
    regime = parse_market_regime(html)

    # Extract generated date
    gen_match = re.search(r'Generated[^:]*:\s*(\d{4}-\d{2}-\d{2})', html)
    generated_date = gen_match.group(1) if gen_match else pubdate.strftime("%Y-%m-%d")

    # Count trade/avoid
    trade_count = sum(1 for c in table_candidates if c.get("action") == "TRADE")
    avoid_count = sum(1 for c in table_candidates if c.get("action") == "AVOID")
    avg_conf = sum(c.get("confidence_pct", 0) for c in table_candidates) / max(len(table_candidates), 1)

    # Insert report
    report_id = guid
    conn.execute("""
        INSERT INTO reports (report_id, title, report_url, generated_date,
            published_date, cycle_id, market_regime, bull_wind, bear_wind,
            crosswind_risk, spy_price, spy_change, total_positions,
            trade_count, avoid_count, avg_confidence, rss_guid)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        report_id, title, link, generated_date,
        pubdate.isoformat(), cycle_id,
        regime.get("market_regime"), regime.get("bull_wind"),
        regime.get("bear_wind"), regime.get("crosswind_risk"),
        regime.get("spy_price"), regime.get("spy_change"),
        len(table_candidates), trade_count, avoid_count,
        round(avg_conf, 1), guid
    ))

    # Insert candidates
    now = datetime.now(timezone.utc)
    tracking_until = (now + timedelta(hours=TRACKING_WINDOW_HOURS)).isoformat()
    inserted = 0

    # Build asset-name lookup for position details (fallback when rank=0)
    asset_detail_map = {}
    for _rank, _det in position_details.items():
        aname = _det.get("_asset_name", "")
        if aname:
            asset_detail_map[aname.lower()[:30]] = _det

    for tc in table_candidates:
        rank = tc.get("rank", 0)
        details = position_details.get(rank, {})

        # Fallback: match by asset name when rank is 0
        if not details and rank == 0:
            asset_key = tc.get("asset_theme", "").lower()[:30]
            details = asset_detail_map.get(asset_key, {})

        # Merge table data with position details
        confidence = tc.get("confidence_pct", 0)
        band, band_label = assign_band(confidence)

        # Check position continuity
        continuity, existing_id = check_position_continuity(tc, conn)

        if continuity == "momentum" and existing_id:
            # Extend existing position
            conn.execute("""
                UPDATE candidates
                SET confirmations = confirmations + 1,
                    last_confirmed_by = ?,
                    last_confirmed_at = ?,
                    tracking_until = ?,
                    momentum_notes = COALESCE(momentum_notes, '') || ?
                WHERE id = ?
            """, (
                report_id,
                now.isoformat(),
                tracking_until,
                "\n[{}] Confirmed by report {}".format(
                    now.strftime("%Y-%m-%d %H:%M"), report_id[:12]
                ),
                existing_id
            ))
            logger.info("MOMENTUM: extended position {} for {}".format(
                existing_id, tc.get("primary_ticker")
            ))
            continue

        if continuity == "reversal" and existing_id:
            # Log the reversal but DON'T kill — let the position monitor decide
            old_dir = conn.execute("SELECT direction FROM candidates WHERE id = ?",
                                   (existing_id,)).fetchone()["direction"]
            conn.execute("""
                UPDATE candidates
                SET momentum_notes = COALESCE(momentum_notes, '') || ?
                WHERE id = ?
            """, (
                "\n[{}] REVERSAL WARNING: new report suggests {} (was {})".format(
                    now.strftime("%Y-%m-%d %H:%M"), tc.get("direction"), old_dir
                ),
                existing_id
            ))
            logger.info("REVERSAL NOTE (not killed): position {} for {} — was {}, now {}".format(
                existing_id, tc.get("primary_ticker"), old_dir, tc.get("direction")
            ))

        # Determine initial state
        # PHILOSOPHY: Keep everything alive. Let the bot decide after investigation.
        # No-ticker positions are silently deactivated (is_active=0) so they don't
        # clutter the trading sheet but are still in the DB for learning.
        initial_state = "PENDING"
        state_reason = "Awaiting due diligence"
        is_active = 1
        if not tc.get("primary_ticker") or tc["primary_ticker"] in ("", "-", "\u2013"):
            initial_state = "KILLED"
            state_reason = "No investable instrument (no ticker)"
            is_active = 0  # Hidden from trading sheet entirely

        conn.execute("""
            INSERT INTO candidates (
                report_id, rank, asset_theme, tickers, primary_ticker,
                prices_at_report, price_changes_at_report,
                direction, confidence_pct, trade_confidence_score,
                edge_quality, freshness_score, propagation, action,
                headline, mechanism, tripwire, evidence, risks,
                band, band_label,
                state, state_reason, state_changed_at,
                discovered_at, tracking_until, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            report_id, rank,
            tc.get("asset_theme"), tc.get("tickers"), tc.get("primary_ticker"),
            tc.get("prices_at_report", "{}"), tc.get("price_changes_at_report", "{}"),
            tc.get("direction"), confidence,
            details.get("trade_confidence_score"),
            tc.get("edge_quality"), details.get("freshness_score"),
            details.get("propagation"), tc.get("action"),
            details.get("headline"), details.get("mechanism"),
            details.get("tripwire"), details.get("evidence"),
            details.get("risks"),
            band, band_label,
            initial_state, state_reason, now.isoformat(),
            now.isoformat(), tracking_until, is_active
        ))
        inserted += 1
        logger.info("  #{}: {} ({}) {} {}% [{}] → {}".format(
            rank, tc.get("asset_theme", "?")[:40],
            tc.get("primary_ticker"), tc.get("direction"),
            confidence, band, initial_state
        ))

    conn.commit()
    conn.close()
    logger.info("Ingested {} candidates from report {}".format(inserted, title[:50]))
    return inserted


def scan():
    """Main scan function. Polls all RSS feeds and ingests new reports.
    Returns total number of new reports found.
    """
    logger.info("Scanning {} RSS feed(s)...".format(len(RSS_FEEDS)))
    try:
        items = fetch_all_rss()
    except Exception as e:
        logger.error("RSS fetch failed: {}".format(e))
        return 0

    logger.info("Found {} Information Asymmetry reports across all feeds".format(len(items)))

    new_reports = 0
    for item in items:
        try:
            inserted = ingest_report(item)
            if inserted > 0:
                new_reports += 1
        except Exception as e:
            logger.error("Failed to ingest report '{}': {}".format(
                item.get("title", "?")[:50], e
            ), exc_info=True)

    return new_reports


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    init_db()
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        # Test with specific report
        print("Testing scanner with live RSS feed...")
        n = scan()
        print("Ingested {} new report(s)".format(n))
    else:
        n = scan()
        print("Scan complete: {} new report(s)".format(n))
