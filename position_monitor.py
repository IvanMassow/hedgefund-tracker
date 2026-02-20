"""
Hedge Fund Edge Tracker - Position Monitor
Intelligent ongoing monitoring of active positions.
Maintains a living thesis via a trader's journal — GPT reviews each position,
returns to the original reasoning, and builds on its own previous assessments.
"""
import json
import logging
import time
from datetime import datetime, timezone, timedelta

from db import get_conn
from tracker import fetch_price_av, calculate_pnl, fetch_spy_change, store_intraday_candles, get_recent_candles
from signal_hunter import build_signal_context
from config import (
    AV_RATE_LIMIT,
    EXIT_HARD_STOP_PCT, EXIT_SOFT_STOP_PCT,
    EXIT_PROFIT_TAKE_PCT, EXIT_PROFIT_STRONG_PCT,
    EXIT_DRAWDOWN_FROM_PEAK_PCT, EXIT_TIME_LIMIT_HOURS,
    EXIT_MARKET_CRASH_PCT,
)

logger = logging.getLogger("hedgefund.position_monitor")

# Monitoring frequency constants (seconds)
MONITOR_STANDARD_INTERVAL = 4 * 60 * 60   # 4 hours
MONITOR_INTENSIVE_INTERVAL = 2 * 60 * 60   # 2 hours (first 24h or high P&L)

# Thresholds
HIGH_PNL_THRESHOLD = 5.0       # Absolute P&L % that triggers intensive monitoring
HIGH_DRAWDOWN_THRESHOLD = -8.0  # Drawdown that triggers intensive monitoring
MAX_JOURNAL_CONTEXT = 5         # Number of previous journal entries to feed GPT


def get_monitorable_positions():
    """Get all WATCH, ACTIVE, and PUBLISH positions eligible for monitoring.

    WATCH positions are DD-approved but waiting for signal confirmation.
    The monitor watches them and decides when to promote to ACTIVE (the 'pounce').
    """
    conn = get_conn()
    rows = conn.execute("""
        SELECT c.*, r.published_date, r.title as report_title
        FROM candidates c
        JOIN reports r ON c.report_id = r.report_id
        WHERE c.state IN ('WATCH', 'ACTIVE', 'PUBLISH')
            AND c.is_active = 1
            AND c.primary_ticker IS NOT NULL
            AND c.primary_ticker != ''
        ORDER BY c.state DESC, c.confidence_pct DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def should_monitor(candidate):
    """Determine if a position should be monitored this cycle.

    Adaptive frequency:
    - First 24h after entry: every 2 hours (intensive)
    - High absolute P&L (>5% or <-8%): every 2 hours (intensive)
    - Standard: every 4 hours
    """
    last_monitor = candidate.get("last_monitor_at")
    entry_time_str = candidate.get("entry_time")

    if not entry_time_str:
        return True  # No entry time = something is off, monitor it

    now = datetime.now(timezone.utc)

    # Parse entry time
    try:
        entry_time = datetime.fromisoformat(entry_time_str)
        if entry_time.tzinfo is None:
            entry_time = entry_time.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return True

    hours_since_entry = (now - entry_time).total_seconds() / 3600

    # Determine required interval
    required_interval = MONITOR_STANDARD_INTERVAL

    # First 24 hours = intensive
    if hours_since_entry < 24:
        required_interval = MONITOR_INTENSIVE_INTERVAL

    # High P&L in either direction = intensive
    current_conviction = candidate.get("current_conviction")
    if current_conviction is not None and current_conviction <= 3:
        required_interval = MONITOR_INTENSIVE_INTERVAL

    # If never monitored, monitor now
    if not last_monitor:
        return True

    # Check elapsed time since last monitor
    try:
        last_dt = datetime.fromisoformat(last_monitor)
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=timezone.utc)
        elapsed = (now - last_dt).total_seconds()
        return elapsed >= required_interval
    except (ValueError, TypeError):
        return True


def build_journal_context(candidate_id):
    """Build a text summary of the last N journal entries for GPT context."""
    conn = get_conn()
    entries = conn.execute("""
        SELECT * FROM trader_journal
        WHERE candidate_id = ?
        ORDER BY cycle_number DESC
        LIMIT ?
    """, (candidate_id, MAX_JOURNAL_CONTEXT)).fetchall()
    conn.close()

    if not entries:
        return None

    # Reverse to chronological order
    entries = list(reversed(entries))

    parts = []
    for e in entries:
        e = dict(e)
        parts.append("--- Cycle {cycle} ({ts}, {hours:.0f}h into trade) ---".format(
            cycle=e["cycle_number"],
            ts=(e.get("timestamp") or "?")[:16],
            hours=e.get("hours_since_entry") or 0
        ))
        parts.append("Decision: {} | Conviction: {}/10 ({}) | Thesis: {}".format(
            e.get("decision", "?"),
            e.get("conviction_score", "?"),
            e.get("conviction_change", "?"),
            e.get("thesis_status", "?")
        ))
        parts.append("P&L at review: {:.1f}% | Peak: +{:.1f}% | Drawdown: {:.1f}%".format(
            e.get("pnl_pct") or 0,
            e.get("peak_gain_pct") or 0,
            e.get("max_drawdown_pct") or 0
        ))
        if e.get("situation_summary"):
            parts.append("Situation: {}".format(e["situation_summary"]))
        if e.get("what_changed"):
            parts.append("What changed: {}".format(e["what_changed"]))
        if e.get("watching_for"):
            parts.append("Watching for: {}".format(e["watching_for"]))
        if e.get("concerns"):
            parts.append("Concerns: {}".format(e["concerns"]))
        if e.get("narrative"):
            parts.append("Narrative: {}".format(e["narrative"]))
        parts.append("")

    return "\n".join(parts)


def build_price_history_context(candidate_id):
    """Build a summary of the price trajectory from snapshots."""
    conn = get_conn()
    snapshots = conn.execute("""
        SELECT timestamp, price, pnl_pct, hours_since_entry, change_pct
        FROM price_snapshots
        WHERE candidate_id = ?
        ORDER BY timestamp ASC
    """, (candidate_id,)).fetchall()
    conn.close()

    if not snapshots:
        return None

    parts = []
    parts.append("Price snapshots ({} data points):".format(len(snapshots)))

    # Show first, last, and evenly spaced samples
    if len(snapshots) <= 8:
        sample = snapshots
    else:
        # First, last, and 6 evenly spaced
        step = max(1, len(snapshots) // 6)
        indices = list(range(0, len(snapshots), step))
        if (len(snapshots) - 1) not in indices:
            indices.append(len(snapshots) - 1)
        sample = [snapshots[i] for i in indices[:8]]

    for s in sample:
        s = dict(s)
        ts = (s.get("timestamp") or "?")[:16]
        price = s.get("price", 0)
        pnl = s.get("pnl_pct")
        hours = s.get("hours_since_entry")

        pnl_str = ""
        if pnl is not None:
            sign = "+" if pnl >= 0 else ""
            pnl_str = " P&L: {}{:.1f}%".format(sign, pnl)

        hours_str = ""
        if hours is not None:
            hours_str = " ({:.0f}h)".format(hours)

        parts.append("  {} ${:.2f}{}{}".format(ts, price, hours_str, pnl_str))

    # Trend summary
    if len(snapshots) >= 3:
        first_price = snapshots[0]["price"]
        last_price = snapshots[-1]["price"]
        if first_price and last_price:
            total_move = (last_price - first_price) / first_price * 100
            parts.append("Overall trajectory: {}{:.1f}% ({} to {})".format(
                "+" if total_move >= 0 else "", total_move,
                "${:.2f}".format(first_price), "${:.2f}".format(last_price)
            ))

        # Recent trend (last 3 snapshots)
        recent = [dict(s) for s in snapshots[-3:]]
        prices = [s["price"] for s in recent if s.get("price")]
        if len(prices) >= 2:
            recent_move = (prices[-1] - prices[0]) / prices[0] * 100
            if recent_move > 1:
                parts.append("Recent trend: rising ({}{:.1f}%)".format(
                    "+" if recent_move >= 0 else "", recent_move))
            elif recent_move < -1:
                parts.append("Recent trend: falling ({:.1f}%)".format(recent_move))
            else:
                parts.append("Recent trend: flat")

    return "\n".join(parts)


def _get_position_metrics(candidate):
    """Calculate peak gain and max drawdown from snapshots."""
    conn = get_conn()
    snapshots = conn.execute("""
        SELECT pnl_pct FROM price_snapshots
        WHERE candidate_id = ? AND pnl_pct IS NOT NULL
        ORDER BY timestamp ASC
    """, (candidate["id"],)).fetchall()
    conn.close()

    peak_gain = 0.0
    max_drawdown = 0.0
    for s in snapshots:
        pnl = s["pnl_pct"]
        if pnl is not None:
            peak_gain = max(peak_gain, pnl)
            max_drawdown = min(max_drawdown, pnl)

    return peak_gain, max_drawdown


def _get_next_cycle_number(candidate_id):
    """Get the next cycle number for a candidate's journal."""
    conn = get_conn()
    row = conn.execute("""
        SELECT MAX(cycle_number) as max_cycle
        FROM trader_journal WHERE candidate_id = ?
    """, (candidate_id,)).fetchone()
    conn.close()
    current = row["max_cycle"] if row and row["max_cycle"] is not None else 0
    return current + 1


def check_mechanical_exits(candidate, current_price, pnl_pct, hours_since_entry):
    """6-checkpoint mechanical exit cascade. Runs BEFORE the LLM assessment.

    Adapted from company-watch's battle-tested exit system.
    Returns (should_exit, exit_type, exit_reason, skip_llm) tuple.
    - should_exit: True if position should be exited immediately
    - exit_type: "take_profit", "cut_loss", "market_crash", "time_limit"
    - exit_reason: Human-readable reason for the exit
    - skip_llm: True if LLM should be skipped entirely (hard exits)

    Only applies to ACTIVE/PUBLISH positions (not WATCH).
    """
    direction = candidate.get("direction", "MIXED")
    entry_price = candidate.get("entry_price")
    peak_price = candidate.get("peak_price")

    if not entry_price or not current_price:
        return False, None, None, False

    # Calculate peak P&L (from peak_price, not from snapshots)
    peak_pnl = calculate_pnl(entry_price, peak_price, direction) if peak_price else pnl_pct

    # ── CHECK 1: Hard stop-loss ──────────────────────────────────────
    # P&L below hard stop → EXIT immediately, no LLM
    if pnl_pct <= EXIT_HARD_STOP_PCT:
        return (True, "cut_loss",
                "HARD STOP: P&L {:.1f}% hit {:.0f}% stop-loss".format(
                    pnl_pct, EXIT_HARD_STOP_PCT),
                True)

    # ── CHECK 2: Profit protection (drawdown from peak) ──────────────
    # If we hit the profit threshold then pulled back by drawdown amount → EXIT
    if peak_pnl >= EXIT_PROFIT_TAKE_PCT:
        drawdown_from_peak = peak_pnl - pnl_pct
        if drawdown_from_peak >= EXIT_DRAWDOWN_FROM_PEAK_PCT:
            return (True, "take_profit",
                    "PROFIT PROTECTION: Peak P&L was +{:.1f}%, pulled back {:.1f}% to {:.1f}%".format(
                        peak_pnl, drawdown_from_peak, pnl_pct),
                    True)

    # ── CHECK 3: Strong profit-taking ────────────────────────────────
    # P&L above strong threshold → EXIT and lock in gains
    if pnl_pct >= EXIT_PROFIT_STRONG_PCT:
        return (True, "take_profit",
                "STRONG PROFIT: P&L +{:.1f}% exceeded +{:.0f}% threshold".format(
                    pnl_pct, EXIT_PROFIT_STRONG_PCT),
                True)

    # ── CHECK 4: Market crash override ───────────────────────────────
    # S&P down >3% on LONG positions → flatten immediately
    if direction == "LONG":
        spy_change = fetch_spy_change()
        if spy_change is not None and spy_change <= EXIT_MARKET_CRASH_PCT:
            return (True, "cut_loss",
                    "MARKET CRASH: S&P down {:.1f}% (threshold {:.0f}%), flattening LONG".format(
                        spy_change, EXIT_MARKET_CRASH_PCT),
                    True)

    # ── CHECK 5: Time limit ──────────────────────────────────────────
    # Held too long with mediocre P&L → force exit or escalate
    if hours_since_entry > EXIT_TIME_LIMIT_HOURS and pnl_pct < 5.0:
        return (True, "cut_loss" if pnl_pct < 0 else "take_profit",
                "TIME LIMIT: Held {:.0f}h (>{:.0f}h limit) with P&L {:.1f}%".format(
                    hours_since_entry, EXIT_TIME_LIMIT_HOURS, pnl_pct),
                True)

    # ── CHECK 6: Soft stop-loss ──────────────────────────────────────
    # P&L below soft stop → still run LLM but flag urgent review
    if pnl_pct <= EXIT_SOFT_STOP_PCT:
        return (False, None,
                "SOFT STOP WARNING: P&L {:.1f}% below {:.0f}% — urgent LLM review".format(
                    pnl_pct, EXIT_SOFT_STOP_PCT),
                False)

    return False, None, None, False


def execute_mechanical_exit(candidate, current_price, pnl_pct, hours_since_entry,
                            exit_type, exit_reason):
    """Execute a mechanical exit — updates DB and logs the decision.
    Uses the same pattern as _apply_decision in monitor_position.
    """
    now = datetime.now(timezone.utc)
    conn = get_conn()

    state_reason = "MECHANICAL {}: {}".format(exit_type.upper(), exit_reason)

    conn.execute("""
        UPDATE candidates SET
            state = 'KILLED',
            state_reason = ?,
            state_changed_at = ?,
            killed_at = ?,
            kill_reason = ?,
            killed_by = 'mechanical',
            exit_price = ?,
            exit_time = ?,
            exit_reason = ?,
            exit_pnl_pct = ?,
            total_held_hours = ?
        WHERE id = ?
    """, (
        state_reason[:200], now.isoformat(), now.isoformat(),
        state_reason[:200],
        current_price, now.isoformat(),
        exit_type,
        round(pnl_pct, 2),
        round(hours_since_entry, 1),
        candidate["id"]
    ))

    # Also log a journal entry for the mechanical exit
    cycle_number = _get_next_cycle_number(candidate["id"])
    peak_gain, max_drawdown = _get_position_metrics(candidate)

    conn.execute("""
        INSERT INTO trader_journal (
            candidate_id, cycle_number, timestamp,
            hours_since_entry, price_at_review, pnl_pct,
            peak_gain_pct, max_drawdown_pct,
            decision, conviction_score, conviction_change,
            thesis_status, situation_summary, narrative,
            risk_level, time_pressure
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        candidate["id"], cycle_number, now.isoformat(),
        round(hours_since_entry, 1), current_price, round(pnl_pct, 2),
        round(max(peak_gain, pnl_pct), 2), round(min(max_drawdown, pnl_pct), 2),
        exit_type.upper(), 0, "mechanical_exit",
        "mechanical_override", exit_reason,
        "Mechanical exit rule fired — no LLM consultation",
        "critical", "immediate"
    ))

    conn.commit()
    conn.close()

    logger.warning("MECHANICAL {}: {} ({}) @ ${:.2f} | P&L: {:.1f}% | {:.0f}h | {}".format(
        exit_type.upper(),
        candidate["asset_theme"][:30], candidate["primary_ticker"],
        current_price, pnl_pct, hours_since_entry, exit_reason
    ))


def monitor_position(candidate_id, llm_trader):
    """Run a single monitoring cycle for one position.

    Handles both WATCH positions (stalking, not yet traded) and
    ACTIVE/PUBLISH positions (traded, monitoring for exit).

    For WATCH positions, the bot is building conviction and hunting for
    signal confirmation. When ready, it decides ENTER to promote to ACTIVE.

    Steps:
    0. [NEW] Check mechanical exit rules (runs BEFORE LLM for ACTIVE/PUBLISH)
    1. Fetch current price
    2. Calculate metrics (P&L for ACTIVE, price movement for WATCH)
    3. Build journal context (last 5 entries)
    4. Build price/signal context + candle context
    5. Call GPT to assess position
    6. Log journal entry
    7. Apply decisions: ENTER (WATCH→ACTIVE), TAKE_PROFIT, CUT_LOSS, KILL, HOLD

    Returns the decision string or None on failure.
    """
    conn = get_conn()
    candidate = conn.execute("""
        SELECT c.*, r.published_date, r.title as report_title
        FROM candidates c
        JOIN reports r ON c.report_id = r.report_id
        WHERE c.id = ? AND c.state IN ('WATCH', 'ACTIVE', 'PUBLISH') AND c.is_active = 1
    """, (candidate_id,)).fetchone()
    conn.close()

    if not candidate:
        logger.warning("Candidate {} not found or not monitorable".format(candidate_id))
        return None

    candidate = dict(candidate)
    primary_ticker = candidate["primary_ticker"]
    direction = candidate.get("direction", "MIXED")
    is_watching = candidate["state"] == "WATCH"
    now = datetime.now(timezone.utc)

    # 1. Fetch current price
    price_data = fetch_price_av(primary_ticker)
    if not price_data:
        logger.warning("Could not fetch price for {} ({})".format(
            candidate["asset_theme"][:30], primary_ticker))
        return None

    current_price = price_data["price"]

    # 2. Calculate metrics
    if is_watching:
        # WATCH mode: use dd_approved_price as reference (not entry_price)
        ref_price = candidate.get("dd_approved_price") or current_price
        pnl_pct = calculate_pnl(ref_price, current_price, direction) or 0
        peak_gain, max_drawdown = _get_position_metrics(candidate)
        peak_gain = max(peak_gain, pnl_pct)
        max_drawdown = min(max_drawdown, pnl_pct)
        # Hours since DD approved (when watching started)
        hours_since_entry = 0
        ref_time = candidate.get("dd_approved_at") or candidate.get("state_changed_at")
        if ref_time:
            try:
                ref_dt = datetime.fromisoformat(ref_time)
                if ref_dt.tzinfo is None:
                    ref_dt = ref_dt.replace(tzinfo=timezone.utc)
                hours_since_entry = (now - ref_dt).total_seconds() / 3600
            except (ValueError, TypeError):
                pass
    else:
        # ACTIVE/PUBLISH mode: use entry_price
        entry_price = candidate["entry_price"]
        pnl_pct = calculate_pnl(entry_price, current_price, direction) or 0
        peak_gain, max_drawdown = _get_position_metrics(candidate)
        peak_gain = max(peak_gain, pnl_pct)
        max_drawdown = min(max_drawdown, pnl_pct)
        hours_since_entry = 0
        if candidate.get("entry_time"):
            try:
                entry_dt = datetime.fromisoformat(candidate["entry_time"])
                if entry_dt.tzinfo is None:
                    entry_dt = entry_dt.replace(tzinfo=timezone.utc)
                hours_since_entry = (now - entry_dt).total_seconds() / 3600
            except (ValueError, TypeError):
                pass

    # 2.5. MECHANICAL EXIT CHECK — runs BEFORE LLM for ACTIVE/PUBLISH positions
    soft_stop_warning = None
    if not is_watching and candidate.get("entry_price"):
        should_exit, exit_type, exit_reason, skip_llm = check_mechanical_exits(
            candidate, current_price, pnl_pct, hours_since_entry
        )
        if should_exit and skip_llm:
            # Hard exit — execute immediately, skip LLM entirely
            execute_mechanical_exit(
                candidate, current_price, pnl_pct, hours_since_entry,
                exit_type, exit_reason
            )
            return exit_type.upper()
        elif exit_reason and not skip_llm:
            # Soft stop warning — pass to LLM as urgent context
            soft_stop_warning = exit_reason
            logger.info("Soft stop flag for {} ({}): {}".format(
                candidate["asset_theme"][:30], primary_ticker, exit_reason
            ))

    # 2.6. Fetch intraday candles for ACTIVE/PUBLISH positions
    candle_context = None
    if not is_watching and candidate.get("entry_price"):
        try:
            candles = store_intraday_candles(candidate_id, primary_ticker)
            if candles:
                candle_lines = []
                green_count = 0
                day_low = None
                day_high = None
                for c_bar in reversed(candles):  # Chronological order
                    flag = ""
                    if c_bar["close"] >= c_bar["open"]:
                        flag = " ▲"
                        green_count += 1
                    else:
                        flag = " ▼"
                    candle_lines.append("  {} O:{:.2f} H:{:.2f} L:{:.2f} C:{:.2f} V:{:.0f}{}".format(
                        c_bar["timestamp"][11:16] if len(c_bar["timestamp"]) > 11 else c_bar["timestamp"],
                        c_bar["open"], c_bar["high"], c_bar["low"], c_bar["close"],
                        c_bar["volume"], flag
                    ))
                    if day_low is None or c_bar["low"] < day_low:
                        day_low = c_bar["low"]
                    if day_high is None or c_bar["high"] > day_high:
                        day_high = c_bar["high"]

                trend = "Rising" if green_count > len(candles) / 2 else "Falling" if green_count < len(candles) / 2 else "Mixed"
                candle_context = "=== INTRADAY PRICE ACTION (15min candles) ===\n"
                candle_context += "\n".join(candle_lines)
                candle_context += "\nTrend: {} ({} of {} candles green)".format(
                    trend, green_count, len(candles))
                if day_low and day_high:
                    candle_context += "\nRange: ${:.2f} - ${:.2f}".format(day_low, day_high)
                time.sleep(AV_RATE_LIMIT)  # Rate limit the candle fetch
        except Exception as e:
            logger.warning("Could not fetch candles for {} ({}): {}".format(
                candidate["asset_theme"][:30], primary_ticker, e))

    # 3. Build context
    journal_context = build_journal_context(candidate_id)
    price_history_context = build_price_history_context(candidate_id)

    # 3.5. Build signal propagation context
    signal_context = None
    try:
        signal_context = build_signal_context(candidate_id)
    except Exception as e:
        logger.warning("Could not build signal context for {}: {}".format(candidate_id, e))

    # 4. Call GPT
    cycle_number = _get_next_cycle_number(candidate_id)
    velocity = candidate.get("signal_velocity") or "quiet"
    mode_label = "STALKING" if is_watching else "MONITORING"
    logger.info("{} {} ({}) — cycle {}, price_move: {:.1f}%, conviction: {}, signal: {}".format(
        mode_label, candidate["asset_theme"][:30], primary_ticker,
        cycle_number, pnl_pct,
        candidate.get("current_conviction") or "first",
        velocity
    ))

    result = llm_trader.assess_position(
        candidate, current_price, peak_gain, max_drawdown,
        hours_since_entry, journal_context, price_history_context,
        signal_context=signal_context,
        candle_context=candle_context,
        soft_stop_warning=soft_stop_warning
    )

    if not result:
        logger.warning("GPT returned no result for position {}".format(candidate_id))
        return None

    decision = result.get("decision", "HOLD").upper()
    conviction = result.get("conviction_score", 5)
    conviction_change = result.get("conviction_change", "unchanged")
    thesis_status = result.get("thesis_status", "intact")

    # For WATCH positions, map trading decisions appropriately
    if is_watching:
        # WATCH positions can: ENTER (pounce), HOLD (keep watching), KILL (abandon)
        if decision in ("TAKE_PROFIT", "CUT_LOSS"):
            # Can't take profit or cut loss on a position we haven't entered
            # High conviction + signal = ENTER, low conviction = KILL
            if conviction >= 7 and velocity in ("propagating", "mainstream"):
                decision = "ENTER"
            elif conviction <= 3 or thesis_status == "invalidated":
                decision = "KILL"
            else:
                decision = "HOLD"
        elif decision == "ENTER":
            # GPT explicitly said ENTER — respect it
            pass
        elif decision in ("REDUCE", "ESCALATE"):
            decision = "HOLD"  # These don't apply to WATCH

    # 5. Log journal entry
    conn = get_conn()
    conn.execute("""
        INSERT INTO trader_journal (
            candidate_id, cycle_number, timestamp,
            hours_since_entry, price_at_review, pnl_pct,
            peak_gain_pct, max_drawdown_pct,
            decision, conviction_score, conviction_change,
            thesis_status, situation_summary, what_changed,
            watching_for, concerns, would_sell_if, would_hold_if,
            narrative, risk_level, time_pressure, llm_raw
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        candidate_id, cycle_number, now.isoformat(),
        round(hours_since_entry, 1), current_price, round(pnl_pct, 2),
        round(peak_gain, 2), round(max_drawdown, 2),
        decision, conviction, conviction_change,
        thesis_status,
        result.get("situation_summary", ""),
        result.get("what_changed", ""),
        result.get("watching_for", ""),
        result.get("concerns", ""),
        result.get("would_sell_if", ""),
        result.get("would_hold_if", ""),
        result.get("narrative", ""),
        result.get("risk_level", "medium"),
        result.get("time_pressure", "none"),
        json.dumps(result)
    ))

    # 6. Update candidate monitoring metadata
    conn.execute("""
        UPDATE candidates SET
            last_monitor_at = ?,
            monitor_count = COALESCE(monitor_count, 0) + 1,
            current_conviction = ?
        WHERE id = ?
    """, (now.isoformat(), conviction, candidate_id))

    # 7. Apply decisions
    if decision == "ENTER" and is_watching:
        # THE POUNCE: promote WATCH → ACTIVE
        # Record the entry price at the moment of decision
        entry_method = candidate.get("entry_method") or "dd_approved"
        conn.execute("""
            UPDATE candidates SET
                state = 'ACTIVE',
                state_reason = ?,
                state_changed_at = ?,
                entry_price = ?,
                entry_time = ?,
                entry_method = ?
            WHERE id = ?
        """, (
            "ENTERED: conviction {}/10, signal {}, {}".format(
                conviction, velocity,
                result.get("situation_summary", "")[:150]),
            now.isoformat(),
            current_price, now.isoformat(),
            entry_method + "_pounce",
            candidate_id
        ))
        logger.warning("POUNCE! {} ({}) WATCH→ACTIVE @ ${:.2f} | conviction {}/10 | signal {}".format(
            candidate["asset_theme"][:30], primary_ticker,
            current_price, conviction, velocity
        ))

    elif decision == "KILL" and is_watching:
        # Abandon the watch — thesis invalidated or too stale
        conn.execute("""
            UPDATE candidates SET
                state = 'KILLED',
                state_reason = ?,
                state_changed_at = ?,
                killed_at = ?,
                kill_reason = ?,
                killed_by = 'monitor'
            WHERE id = ?
        """, (
            "KILL (watching): {}".format(result.get("situation_summary", "")[:200]),
            now.isoformat(), now.isoformat(),
            "KILL: {}".format(result.get("situation_summary", "")[:200]),
            candidate_id
        ))
        logger.info("KILL (watching): {} ({}) — abandoned after {:.0f}h watching".format(
            candidate["asset_theme"][:30], primary_ticker, hours_since_entry
        ))

    elif decision == "TAKE_PROFIT" and not is_watching:
        conn.execute("""
            UPDATE candidates SET
                state = 'KILLED',
                state_reason = ?,
                state_changed_at = ?,
                killed_at = ?,
                kill_reason = ?,
                killed_by = 'monitor',
                exit_price = ?,
                exit_time = ?,
                exit_reason = ?,
                exit_pnl_pct = ?,
                total_held_hours = ?
            WHERE id = ?
        """, (
            "TAKE_PROFIT: {}".format(result.get("situation_summary", "")[:200]),
            now.isoformat(), now.isoformat(),
            "TAKE_PROFIT: {}".format(result.get("situation_summary", "")[:200]),
            current_price, now.isoformat(),
            "take_profit",
            round(pnl_pct, 2),
            round(hours_since_entry, 1),
            candidate_id
        ))
        logger.info("TAKE_PROFIT: {} ({}) at ${:.2f} P&L: {:.1f}% after {:.0f}h".format(
            candidate["asset_theme"][:30], primary_ticker,
            current_price, pnl_pct, hours_since_entry
        ))

    elif decision == "CUT_LOSS" and not is_watching:
        conn.execute("""
            UPDATE candidates SET
                state = 'KILLED',
                state_reason = ?,
                state_changed_at = ?,
                killed_at = ?,
                kill_reason = ?,
                killed_by = 'monitor',
                exit_price = ?,
                exit_time = ?,
                exit_reason = ?,
                exit_pnl_pct = ?,
                total_held_hours = ?
            WHERE id = ?
        """, (
            "CUT_LOSS: {}".format(result.get("situation_summary", "")[:200]),
            now.isoformat(), now.isoformat(),
            "CUT_LOSS: {}".format(result.get("situation_summary", "")[:200]),
            current_price, now.isoformat(),
            "cut_loss",
            round(pnl_pct, 2),
            round(hours_since_entry, 1),
            candidate_id
        ))
        logger.info("CUT_LOSS: {} ({}) at ${:.2f} P&L: {:.1f}% after {:.0f}h".format(
            candidate["asset_theme"][:30], primary_ticker,
            current_price, pnl_pct, hours_since_entry
        ))

    elif decision == "REDUCE":
        logger.info("REDUCE: {} ({}) conviction dropped to {}/10".format(
            candidate["asset_theme"][:30], primary_ticker, conviction
        ))

    elif decision == "ESCALATE":
        logger.warning("ESCALATE: {} ({}) — requires human attention: {}".format(
            candidate["asset_theme"][:30], primary_ticker,
            result.get("situation_summary", "")[:100]
        ))

    else:
        # HOLD (for both WATCH and ACTIVE)
        label = "WATCHING" if is_watching else "HOLD"
        logger.info("{}: {} ({}) conviction {}/10, thesis {}, signal {}".format(
            label, candidate["asset_theme"][:30], primary_ticker,
            conviction, thesis_status, velocity
        ))

    conn.commit()
    conn.close()
    return decision


def run_position_monitoring(llm_trader):
    """Top-level function: iterate all eligible positions and monitor them.

    Called from runner.py on the MONITOR_INTERVAL schedule.
    Returns the number of positions monitored.
    """
    positions = get_monitorable_positions()
    if not positions:
        logger.info("No active positions to monitor")
        return 0

    logger.info("Position monitoring: {} eligible positions".format(len(positions)))
    monitored = 0

    for pos in positions:
        if not should_monitor(pos):
            logger.debug("Skipping {} — not due for monitoring yet".format(
                pos["asset_theme"][:30]))
            continue

        try:
            decision = monitor_position(pos["id"], llm_trader)
            if decision:
                monitored += 1
        except Exception as e:
            logger.error("Monitor failed for {} ({}): {}".format(
                pos["asset_theme"][:30], pos["primary_ticker"], e
            ), exc_info=True)

        # Respect Alpha Vantage rate limit
        time.sleep(AV_RATE_LIMIT)

    logger.info("Position monitoring complete: {}/{} monitored".format(
        monitored, len(positions)))
    return monitored


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    from db import init_db
    import llm_trader as llm

    init_db()

    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        # Monitor a specific candidate by ID
        cid = int(sys.argv[2]) if len(sys.argv) > 2 else None
        if cid:
            print("Monitoring candidate {}...".format(cid))
            result = monitor_position(cid, llm)
            print("Decision: {}".format(result))
        else:
            print("Usage: python3 position_monitor.py --test <candidate_id>")
    else:
        n = run_position_monitoring(llm)
        print("Monitored {} positions".format(n))
