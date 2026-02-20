"""
Hedge Fund Edge Tracker - Smart Trader
Manages the trading decision pipeline: staleness check, due diligence,
trade/watch/kill decisions.
"""
import json
import logging
from datetime import datetime, timezone, timedelta

from db import get_conn
from config import (
    MARKET_OPEN_UTC, MARKET_CLOSE_UTC, MARKET_DAYS,
    STALENESS_LOW, STALENESS_MEDIUM, STALENESS_HIGH, STALENESS_CRITICAL,
    PRICE_MOVE_SMALL, PRICE_MOVE_MEDIUM, PRICE_MOVE_LARGE,
    MAX_WATCH_CHECKS, TRACKING_WINDOW_HOURS
)
from tracker import fetch_price_av

logger = logging.getLogger("hedgefund.trader")


def next_trading_window(from_time=None):
    """Calculate the next market open datetime (UTC).
    NYSE opens at 14:30 UTC (9:30 ET) on weekdays.
    """
    if from_time is None:
        from_time = datetime.now(timezone.utc)

    dt = from_time
    # If we're in a trading window right now, return now
    if is_market_open(dt):
        return dt

    # Find next weekday at market open
    while True:
        if dt.weekday() in MARKET_DAYS:
            market_open = dt.replace(
                hour=int(MARKET_OPEN_UTC),
                minute=int((MARKET_OPEN_UTC % 1) * 60),
                second=0, microsecond=0
            )
            if market_open > from_time:
                return market_open
        dt += timedelta(days=1)
        # Reset to start of day
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)


def is_market_open(dt=None):
    """Check if NYSE is currently open."""
    if dt is None:
        dt = datetime.now(timezone.utc)
    if dt.weekday() not in MARKET_DAYS:
        return False
    hour_decimal = dt.hour + dt.minute / 60.0
    return MARKET_OPEN_UTC <= hour_decimal < MARKET_CLOSE_UTC


def calculate_staleness(report_published, from_time=None):
    """Calculate staleness in hours from report publication to from_time or now."""
    if from_time is None:
        from_time = datetime.now(timezone.utc)
    if isinstance(report_published, str):
        report_published = datetime.fromisoformat(report_published)
    if report_published.tzinfo is None:
        report_published = report_published.replace(tzinfo=timezone.utc)
    delta = from_time - report_published
    return delta.total_seconds() / 3600


def staleness_rating(hours):
    """Rate staleness level."""
    if hours < STALENESS_LOW:
        return "FRESH"
    elif hours < STALENESS_MEDIUM:
        return "MODERATE"
    elif hours < STALENESS_HIGH:
        return "STALE"
    elif hours < STALENESS_CRITICAL:
        return "VERY_STALE"
    else:
        return "CRITICAL"


def price_moved_pct(report_price, current_price):
    """Calculate percentage price movement from report to current."""
    if not report_price or not current_price or report_price == 0:
        return 0
    return abs(current_price - report_price) / report_price * 100


def price_moved_in_thesis_direction(report_price, current_price, direction):
    """Check if price moved in the thesis direction (bad — edge may be captured)."""
    if not report_price or not current_price:
        return False
    if direction == "SHORT":
        return current_price < report_price
    elif direction == "LONG":
        return current_price > report_price
    return False


def make_trade_decision(candidate, current_price, staleness_hours, llm_result=None):
    """Core decision engine. Returns (decision, reason, watch_conditions).

    Decision is one of: TRADE, WATCH, KILL
    """
    direction = candidate["direction"]
    confidence = candidate["confidence_pct"] or 0
    edge_quality = candidate["edge_quality"] or "HIGH"
    propagation = candidate.get("propagation") or "IGNITE"
    action = candidate["action"]

    # Report marked AVOID — but the bot investigates independently.
    # AVOID is just the report's opinion. If the analytics/thesis look interesting
    # and we have a tradeable instrument, let the bot decide for itself.
    # (Positions with no ticker were already killed on intake by scanner.py)

    # Get report price for comparison
    prices_json = candidate.get("prices_at_report") or "{}"
    try:
        prices = json.loads(prices_json)
    except (json.JSONDecodeError, TypeError):
        prices = {}
    primary_ticker = candidate["primary_ticker"]
    report_price = prices.get(primary_ticker, 0)

    # Price movement analysis
    move_pct = price_moved_pct(report_price, current_price) if report_price else 0
    moved_with_thesis = price_moved_in_thesis_direction(
        report_price, current_price, direction
    ) if report_price else False

    # Staleness assessment
    stale_rating = staleness_rating(staleness_hours)

    # LLM override (if available)
    if llm_result:
        llm_decision = llm_result.get("decision", "").upper()
        llm_confidence = llm_result.get("confidence", "MEDIUM")
        if llm_confidence == "HIGH":
            # Trust high-confidence LLM decisions
            reason = "LLM DD (HIGH confidence): {}".format(llm_result.get("reason", ""))
            conditions = llm_result.get("watch_conditions")
            return llm_decision, reason, conditions

    # === Decision Matrix ===

    # Edge is DECAYING — higher bar for trade
    if edge_quality == "DECAYING":
        if stale_rating in ("STALE", "VERY_STALE", "CRITICAL"):
            return "KILL", "Decaying edge + {} staleness ({:.0f}h) = edge likely gone".format(
                stale_rating, staleness_hours
            ), None
        if move_pct > PRICE_MOVE_MEDIUM and moved_with_thesis:
            return "KILL", "Decaying edge + price already moved {:.1f}% with thesis".format(
                move_pct
            ), None

    # Price has moved significantly with the thesis — edge captured
    if move_pct > PRICE_MOVE_LARGE and moved_with_thesis:
        return "KILL", "Price moved {:.1f}% in thesis direction — edge captured".format(
            move_pct
        ), None

    # Price moved significantly against thesis — thesis may be wrong
    if move_pct > PRICE_MOVE_LARGE and not moved_with_thesis:
        return "WATCH", "Price moved {:.1f}% against thesis — monitoring".format(
            move_pct
        ), json.dumps({"type": "price_recovery", "target_pct": move_pct / 2})

    # Critical staleness
    if stale_rating == "CRITICAL":
        if confidence >= 65:
            return "WATCH", "Critical staleness ({:.0f}h) but high confidence ({:.0f}%) — watching".format(
                staleness_hours, confidence
            ), json.dumps({"type": "freshness_check", "max_staleness": staleness_hours + 24})
        else:
            return "KILL", "Critical staleness ({:.0f}h) with moderate confidence ({:.0f}%)".format(
                staleness_hours, confidence
            ), None

    # Very stale
    if stale_rating == "VERY_STALE":
        if confidence >= 55 and propagation == "IGNITE":
            return "WATCH", "Stale ({:.0f}h) but IGNITE propagation — edge may still fire".format(
                staleness_hours
            ), json.dumps({"type": "propagation_check"})
        elif confidence >= 65:
            return "WATCH", "Stale ({:.0f}h) but high confidence — monitoring".format(
                staleness_hours
            ), json.dumps({"type": "price_check"})
        else:
            return "KILL", "Stale ({:.0f}h) with insufficient confidence ({:.0f}%)".format(
                staleness_hours, confidence
            ), None

    # Moderate staleness
    if stale_rating == "STALE":
        if move_pct < PRICE_MOVE_SMALL:
            return "TRADE", "Moderate staleness ({:.0f}h) but price stable (moved {:.1f}%)".format(
                staleness_hours, move_pct
            ), None
        elif move_pct < PRICE_MOVE_MEDIUM:
            return "WATCH", "Moderate staleness + some price movement ({:.1f}%)".format(
                move_pct
            ), json.dumps({"type": "price_stabilization"})
        else:
            if moved_with_thesis:
                return "WATCH", "Moderate staleness + price moved {:.1f}% with thesis".format(
                    move_pct
                ), json.dumps({"type": "partial_capture"})
            else:
                return "WATCH", "Moderate staleness + price moved {:.1f}% against thesis".format(
                    move_pct
                ), json.dumps({"type": "counter_move"})

    # Fresh or moderate — price is the key factor
    if move_pct < PRICE_MOVE_MEDIUM:
        return "TRADE", "Fresh signal ({:.0f}h), price stable ({:.1f}% move), confidence {:.0f}%".format(
            staleness_hours, move_pct, confidence
        ), None

    if move_pct < PRICE_MOVE_LARGE:
        if moved_with_thesis:
            return "TRADE", "Fresh signal, price partially moved with thesis ({:.1f}%) — still room".format(
                move_pct
            ), None
        else:
            return "TRADE", "Fresh signal, price moved against thesis ({:.1f}%) — better entry".format(
                move_pct
            ), None

    # Default: watch
    return "WATCH", "Uncertain conditions — defaulting to watch", json.dumps({"type": "general"})


def run_due_diligence(candidate_id, dd_type="pre_trade", llm_trader=None):
    """Execute due diligence for a single candidate.
    Returns the decision made (TRADE/WATCH/KILL).
    """
    conn = get_conn()
    candidate = conn.execute(
        "SELECT c.*, r.published_date FROM candidates c "
        "JOIN reports r ON c.report_id = r.report_id "
        "WHERE c.id = ?", (candidate_id,)
    ).fetchone()

    if not candidate:
        conn.close()
        return None

    candidate = dict(candidate)
    primary_ticker = candidate["primary_ticker"]
    now = datetime.now(timezone.utc)

    # 1. Staleness check
    staleness_hours = calculate_staleness(candidate["published_date"])

    # 2. Fresh price check
    current_price = None
    price_data = fetch_price_av(primary_ticker) if primary_ticker else None
    if price_data:
        current_price = price_data["price"]

    # Calculate price move since report
    prices_json = candidate.get("prices_at_report") or "{}"
    try:
        prices = json.loads(prices_json)
    except (json.JSONDecodeError, TypeError):
        prices = {}
    report_price = prices.get(primary_ticker, 0)
    price_move = price_moved_pct(report_price, current_price) if report_price and current_price else 0

    # 3. LLM due diligence (if available)
    llm_result = None
    if llm_trader:
        try:
            llm_result = llm_trader.assess_trade(candidate, current_price, staleness_hours)
        except Exception as e:
            logger.warning("LLM DD failed for candidate {}: {}".format(candidate_id, e))

    # 4. Make decision
    decision, reason, watch_conditions = make_trade_decision(
        candidate, current_price, staleness_hours, llm_result
    )

    # 5. Log the DD
    conn.execute("""
        INSERT INTO dd_log (candidate_id, dd_type, staleness_hours,
            price_at_check, price_move_since_report, thesis_still_valid,
            decision, decision_reason, llm_analysis)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        candidate_id, dd_type, round(staleness_hours, 1),
        current_price, round(price_move, 2),
        1 if decision != "KILL" else 0,
        decision, reason,
        json.dumps(llm_result) if llm_result else None
    ))

    # 6. Apply decision
    # PHILOSOPHY: DD validates the thesis, but does NOT auto-enter trades.
    # All positions go to WATCH. The position monitor + signal hunter
    # decide when to promote WATCH → ACTIVE (the "pounce").
    # This makes the bot a patient stalker, not an instant executor.
    if decision in ("TRADE", "PUBLISH"):
        dd_price = current_price or report_price
        watch_reason = "DD approved — watching for signal confirmation"
        if decision == "PUBLISH":
            watch_reason = "DD approved (publishable) — watching for signal confirmation"

        # Store editorial metadata for PUBLISH candidates
        publish_angle = ""
        publish_headline = ""
        if decision == "PUBLISH" and llm_result:
            publish_angle = llm_result.get("publish_angle", "")
            publish_headline = llm_result.get("publish_headline", "")

        conn.execute("""
            UPDATE candidates SET
                state = 'WATCH',
                state_reason = ?,
                state_changed_at = ?,
                dd_approved_price = ?,
                dd_approved_at = ?,
                entry_method = ?,
                watch_conditions = ?,
                last_dd_at = ?,
                dd_count = dd_count + 1
            WHERE id = ?
        """, (
            watch_reason, now.isoformat(),
            dd_price, now.isoformat(),
            "dd_publish" if decision == "PUBLISH" else "dd_approved",
            watch_conditions or "Signal confirmation + conviction build",
            now.isoformat(), candidate_id
        ))

        if publish_angle or publish_headline:
            conn.execute("""
                UPDATE candidates SET
                    publish_angle = ?,
                    publish_headline = ?,
                    pre_publish_price = ?
                WHERE id = ?
            """, (publish_angle, publish_headline, dd_price, candidate_id))

        logger.info("WATCH (DD approved): {} ({}) @ ${:.2f} — {}".format(
            candidate["asset_theme"][:30], primary_ticker,
            dd_price, reason[:60]
        ))
        if decision == "PUBLISH":
            logger.info("  Editorial angle: {}".format(publish_angle[:80] if publish_angle else "N/A"))

    elif decision == "WATCH":
        conn.execute("""
            UPDATE candidates SET
                state = 'WATCH',
                state_reason = ?,
                state_changed_at = ?,
                watch_conditions = ?,
                watch_checks = watch_checks + 1,
                last_dd_at = ?,
                dd_count = dd_count + 1
            WHERE id = ?
        """, (reason, now.isoformat(), watch_conditions,
              now.isoformat(), candidate_id))
        logger.info("WATCH: {} ({}) — {}".format(
            candidate["asset_theme"][:30], primary_ticker, reason[:60]
        ))

    elif decision == "KILL":
        conn.execute("""
            UPDATE candidates SET
                state = 'KILLED',
                state_reason = ?,
                state_changed_at = ?,
                killed_at = ?,
                kill_reason = ?,
                killed_by = 'dd',
                last_dd_at = ?,
                dd_count = dd_count + 1
            WHERE id = ?
        """, (reason, now.isoformat(), now.isoformat(), reason,
              now.isoformat(), candidate_id))
        logger.info("KILL: {} ({}) — {}".format(
            candidate["asset_theme"][:30], primary_ticker, reason[:60]
        ))

    conn.commit()
    conn.close()
    return decision


def process_pending_candidates(llm_trader=None):
    """Process all PENDING candidates. Run DD and decide trade/watch/kill.
    Respects market hours — schedules DD for market open if market closed.
    """
    conn = get_conn()
    pending = conn.execute("""
        SELECT c.id, c.primary_ticker, c.asset_theme, r.published_date
        FROM candidates c
        JOIN reports r ON c.report_id = r.report_id
        WHERE c.state = 'PENDING' AND c.is_active = 1
            AND c.primary_ticker IS NOT NULL AND c.primary_ticker != ''
    """).fetchall()
    conn.close()

    if not pending:
        return 0

    logger.info("Processing {} pending candidates".format(len(pending)))
    processed = 0

    now = datetime.now(timezone.utc)
    market_open = is_market_open(now)

    for p in pending:
        staleness = calculate_staleness(p["published_date"])

        if not market_open and staleness < STALENESS_MEDIUM:
            # Market is closed and report is reasonably fresh — wait for market open
            logger.info("Market closed, deferring DD for {} ({}) until market open".format(
                p["asset_theme"][:30], p["primary_ticker"]
            ))
            continue

        # Run DD (either market is open, or report is getting stale)
        decision = run_due_diligence(p["id"], "pre_trade", llm_trader)
        if decision:
            processed += 1

        # Rate limit between API calls
        import time
        time.sleep(12)  # Alpha Vantage rate limit

    logger.info("Processed {}/{} pending candidates".format(processed, len(pending)))
    return processed


def recheck_watched(llm_trader=None):
    """Re-run DD on WATCH candidates. Promote to ACTIVE or KILL if conditions met."""
    conn = get_conn()
    watched = conn.execute("""
        SELECT c.id, c.primary_ticker, c.asset_theme, c.watch_checks,
               r.published_date
        FROM candidates c
        JOIN reports r ON c.report_id = r.report_id
        WHERE c.state = 'WATCH' AND c.is_active = 1
    """).fetchall()
    conn.close()

    if not watched:
        return 0

    logger.info("Rechecking {} watched candidates".format(len(watched)))
    rechecked = 0

    for w in watched:
        # Auto-kill if too many watch checks
        if w["watch_checks"] >= MAX_WATCH_CHECKS:
            conn2 = get_conn()
            now = datetime.now(timezone.utc)
            conn2.execute("""
                UPDATE candidates SET
                    state = 'KILLED',
                    state_reason = 'Max watch checks exceeded ({} checks)',
                    state_changed_at = ?,
                    killed_at = ?,
                    kill_reason = 'Max watch checks exceeded',
                    killed_by = 'auto_expire'
                WHERE id = ?
            """.format(w["watch_checks"]), (now.isoformat(), now.isoformat(), w["id"]))
            conn2.commit()
            conn2.close()
            logger.info("WATCH→KILL (max checks): {} ({})".format(
                w["asset_theme"][:30], w["primary_ticker"]
            ))
            rechecked += 1
            continue

        decision = run_due_diligence(w["id"], "watch_check", llm_trader)
        if decision:
            rechecked += 1

        import time
        time.sleep(12)

    logger.info("Rechecked {}/{} watched candidates".format(rechecked, len(watched)))
    return rechecked


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    from db import init_db
    init_db()

    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        print("Testing trader with pending candidates...")
        n = process_pending_candidates()
        print("Processed {} candidates".format(n))
    else:
        process_pending_candidates()
