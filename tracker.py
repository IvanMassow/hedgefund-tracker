"""
Hedge Fund Edge Tracker - Price Tracker
Fetches current stock prices via Alpha Vantage and stores hourly snapshots.
"""
import time
import json
import logging
from datetime import datetime, timezone, timedelta

import requests

from db import get_conn
from config import ALPHA_VANTAGE_KEY, ALPHA_VANTAGE_BASE, AV_RATE_LIMIT

logger = logging.getLogger("hedgefund.tracker")

# Module-level SPY cache — reset each tracking cycle
_spy_change_cache = {"value": None, "fetched_at": None}


def get_active_candidates():
    """Get all candidates still being tracked.
    Includes KILLED and WATCH candidates — we keep watching to validate decisions.
    Only stops when tracking window expires.
    """
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    rows = conn.execute("""
        SELECT c.*, r.title as report_title
        FROM candidates c
        JOIN reports r ON c.report_id = r.report_id
        WHERE c.is_active = 1 AND c.tracking_until > ?
            AND c.primary_ticker IS NOT NULL AND c.primary_ticker != ''
            AND c.primary_ticker != '-'
        ORDER BY c.discovered_at DESC
    """, (now,)).fetchall()
    conn.close()
    return rows


def deactivate_expired():
    """Mark candidates past their tracking window as inactive."""
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    result = conn.execute("""
        UPDATE candidates SET is_active = 0, state = CASE
            WHEN state IN ('PENDING', 'WATCH') THEN 'EXPIRED'
            ELSE state
        END
        WHERE is_active = 1 AND tracking_until <= ?
    """, (now,))
    if result.rowcount > 0:
        logger.info("Deactivated {} expired candidates".format(result.rowcount))
    conn.commit()
    conn.close()


def fetch_price_av(ticker):
    """Fetch current price from Alpha Vantage Global Quote endpoint."""
    if not ALPHA_VANTAGE_KEY:
        logger.warning("No ALPHA_VANTAGE_KEY set")
        return None

    try:
        resp = requests.get(
            ALPHA_VANTAGE_BASE,
            params={
                "function": "GLOBAL_QUOTE",
                "symbol": ticker,
                "apikey": ALPHA_VANTAGE_KEY,
            },
            timeout=15
        )
        resp.raise_for_status()
        data = resp.json()

        gq = data.get("Global Quote", {})
        if not gq or "05. price" not in gq:
            # Might be rate limited or invalid ticker
            if "Note" in data or "Information" in data:
                logger.warning("Alpha Vantage rate limit or info message for {}: {}".format(
                    ticker, data.get("Note", data.get("Information", ""))[:100]
                ))
            else:
                logger.warning("No quote data for {}: {}".format(ticker, str(data)[:200]))
            return None

        return {
            "price": float(gq["05. price"]),
            "open": float(gq.get("02. open", 0)),
            "high": float(gq.get("03. high", 0)),
            "low": float(gq.get("04. low", 0)),
            "volume": float(gq.get("06. volume", 0)),
            "change_pct": float(gq.get("10. change percent", "0").rstrip('%')),
        }
    except Exception as e:
        logger.warning("Alpha Vantage fetch failed for {}: {}".format(ticker, e))
        return None


def should_snapshot(candidate_id):
    """Check if we should take a snapshot (avoid duplicates within same hour)."""
    conn = get_conn()
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=50)).isoformat()
    existing = conn.execute("""
        SELECT 1 FROM price_snapshots
        WHERE candidate_id = ? AND timestamp > ?
    """, (candidate_id, cutoff)).fetchone()
    conn.close()
    return existing is None


def calculate_pnl(entry_price, current_price, direction):
    """Calculate P&L percentage based on direction."""
    if not entry_price or not current_price:
        return None
    if direction == "SHORT":
        return round((entry_price - current_price) / entry_price * 100, 2)
    elif direction == "LONG":
        return round((current_price - entry_price) / entry_price * 100, 2)
    return None


def fetch_spy_change():
    """Fetch SPY daily change percentage. Cached per tracking cycle.
    Used for market crash detection (S&P down >3% = flatten longs).
    """
    global _spy_change_cache
    # Return cached value if fetched within the last 30 minutes
    if _spy_change_cache["value"] is not None and _spy_change_cache["fetched_at"]:
        age = (datetime.now(timezone.utc) - _spy_change_cache["fetched_at"]).total_seconds()
        if age < 1800:  # 30 minutes
            return _spy_change_cache["value"]

    data = fetch_price_av("SPY")
    if data and data.get("change_pct") is not None:
        _spy_change_cache["value"] = data["change_pct"]
        _spy_change_cache["fetched_at"] = datetime.now(timezone.utc)
        logger.info("SPY change today: {:.2f}%".format(data["change_pct"]))
        return data["change_pct"]

    logger.warning("Could not fetch SPY change — crash detection unavailable this cycle")
    return None


def fetch_intraday_candles(ticker, interval="15min", outputsize="compact"):
    """Fetch intraday candle data from Alpha Vantage TIME_SERIES_INTRADAY.
    Returns last 10 candles as list of dicts with OHLCV data.
    Only call for ACTIVE/PUBLISH positions near exit thresholds to conserve API calls.
    """
    if not ALPHA_VANTAGE_KEY:
        return []

    try:
        resp = requests.get(
            ALPHA_VANTAGE_BASE,
            params={
                "function": "TIME_SERIES_INTRADAY",
                "symbol": ticker,
                "interval": interval,
                "outputsize": outputsize,
                "apikey": ALPHA_VANTAGE_KEY,
            },
            timeout=15
        )
        resp.raise_for_status()
        data = resp.json()

        series_key = "Time Series ({})".format(interval)
        series = data.get(series_key, {})
        if not series:
            if "Note" in data or "Information" in data:
                logger.warning("AV rate limit on intraday for {}: {}".format(
                    ticker, data.get("Note", data.get("Information", ""))[:100]
                ))
            return []

        # Sort by timestamp descending, take last 10
        candles = []
        for ts in sorted(series.keys(), reverse=True)[:10]:
            bar = series[ts]
            candles.append({
                "timestamp": ts,
                "open": float(bar["1. open"]),
                "high": float(bar["2. high"]),
                "low": float(bar["3. low"]),
                "close": float(bar["4. close"]),
                "volume": float(bar["5. volume"]),
            })
        logger.debug("Fetched {} intraday candles for {}".format(len(candles), ticker))
        return candles

    except Exception as e:
        logger.warning("Intraday candle fetch failed for {}: {}".format(ticker, e))
        return []


def store_intraday_candles(candidate_id, ticker, interval="15min"):
    """Fetch and store intraday candles for a candidate. Returns the candles."""
    candles = fetch_intraday_candles(ticker, interval)
    if not candles:
        return []

    conn = get_conn()
    for c in candles:
        # Upsert: skip if we already have this exact timestamp+interval
        existing = conn.execute(
            "SELECT 1 FROM intraday_candles WHERE candidate_id=? AND timestamp=? AND interval=?",
            (candidate_id, c["timestamp"], interval)
        ).fetchone()
        if not existing:
            conn.execute("""
                INSERT INTO intraday_candles
                (candidate_id, timestamp, interval, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (candidate_id, c["timestamp"], interval,
                  c["open"], c["high"], c["low"], c["close"], c["volume"]))
    conn.commit()
    conn.close()
    return candles


def get_recent_candles(candidate_id, limit=10):
    """Retrieve the most recent intraday candles for a candidate from DB."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT timestamp, interval, open, high, low, close, volume
        FROM intraday_candles
        WHERE candidate_id = ?
        ORDER BY timestamp DESC
        LIMIT ?
    """, (candidate_id, limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def track_prices():
    """Main tracking function. Fetches prices for all active candidates."""
    deactivate_expired()
    candidates = get_active_candidates()

    if not candidates:
        logger.info("No active candidates to track")
        return 0

    logger.info("Tracking {} active candidates".format(len(candidates)))
    conn = get_conn()
    now = datetime.now(timezone.utc)
    tracked = 0

    # Group by primary ticker to avoid duplicate API calls
    ticker_prices = {}
    tickers_needed = set()
    for c in candidates:
        t = c["primary_ticker"]
        if t:
            tickers_needed.add(t)

    # Fetch prices for unique tickers
    for ticker in sorted(tickers_needed):
        if ticker in ticker_prices:
            continue
        price_data = fetch_price_av(ticker)
        if price_data:
            ticker_prices[ticker] = price_data
        time.sleep(AV_RATE_LIMIT)  # Respect rate limit

    # Store snapshots
    for c in candidates:
        cid = c["id"]
        if not should_snapshot(cid):
            continue

        ticker = c["primary_ticker"]
        price_data = ticker_prices.get(ticker)
        if not price_data:
            continue

        # Calculate hours since discovery
        discovered = datetime.fromisoformat(c["discovered_at"])
        if discovered.tzinfo is None:
            discovered = discovered.replace(tzinfo=timezone.utc)
        hours_since_disc = (now - discovered).total_seconds() / 3600

        # Calculate hours since entry and P&L
        hours_since_entry = None
        pnl_pct = None
        if c["entry_time"] and c["entry_price"]:
            entry_time = datetime.fromisoformat(c["entry_time"])
            if entry_time.tzinfo is None:
                entry_time = entry_time.replace(tzinfo=timezone.utc)
            hours_since_entry = (now - entry_time).total_seconds() / 3600
            pnl_pct = calculate_pnl(c["entry_price"], price_data["price"], c["direction"])

        conn.execute("""
            INSERT INTO price_snapshots
            (candidate_id, timestamp, price, open_price, high, low,
             volume, change_pct, hours_since_discovery, hours_since_entry, pnl_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            cid, now.isoformat(),
            price_data["price"], price_data["open"],
            price_data["high"], price_data["low"],
            price_data["volume"], price_data["change_pct"],
            round(hours_since_disc, 2),
            round(hours_since_entry, 2) if hours_since_entry else None,
            pnl_pct
        ))
        # Update peak/trough for positions with entry prices (mechanical exit support)
        if c["state"] in ("ACTIVE", "PUBLISH") and c["entry_price"]:
            current_price = price_data["price"]
            peak = c["peak_price"] if c["peak_price"] else current_price
            trough = c["trough_price"] if c["trough_price"] else current_price
            updated = False
            if current_price > peak:
                conn.execute("UPDATE candidates SET peak_price=? WHERE id=?",
                             (current_price, cid))
                updated = True
            if current_price < trough:
                conn.execute("UPDATE candidates SET trough_price=? WHERE id=?",
                             (current_price, cid))
                updated = True
            # Initialize if not set yet
            if not c["peak_price"]:
                conn.execute("UPDATE candidates SET peak_price=? WHERE id=?",
                             (current_price, cid))
            if not c["trough_price"]:
                conn.execute("UPDATE candidates SET trough_price=? WHERE id=?",
                             (current_price, cid))

        tracked += 1
        logger.debug("  {} ({}): ${:.2f} P&L={}".format(
            c["asset_theme"][:30], ticker, price_data["price"],
            "{}%".format(pnl_pct) if pnl_pct is not None else "N/A"
        ))

    conn.commit()
    conn.close()
    logger.info("Tracked {}/{} candidates ({} unique tickers)".format(
        tracked, len(candidates), len(ticker_prices)
    ))
    return tracked


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        ticker = sys.argv[2] if len(sys.argv) > 2 else "KD"
        print("Testing Alpha Vantage fetch for {}...".format(ticker))
        data = fetch_price_av(ticker)
        if data:
            print("  Price: ${:.2f}".format(data["price"]))
            print("  Change: {}%".format(data["change_pct"]))
            print("  Volume: {:.0f}".format(data["volume"]))
        else:
            print("  Failed to fetch price")
    else:
        from db import init_db
        init_db()
        track_prices()
