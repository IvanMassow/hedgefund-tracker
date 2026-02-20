"""
Hedge Fund Edge Tracker - Analytics & Learning
Performance metrics, confidence band analysis, and learning system.
"""
import json
import logging
from datetime import datetime, timezone
from collections import defaultdict

from db import get_conn
from config import BANDS, ALPHA_DIRECTIONS, ALPHA_BANDS, ALPHA_FORMULA_DESC

logger = logging.getLogger("hedgefund.analytics")


def generate_analytics():
    """Generate comprehensive analytics payload for report generation.
    Returns dict with all data needed for HTML report.
    """
    conn = get_conn()

    # Get all candidates with their snapshots
    candidates = conn.execute("""
        SELECT c.*, r.title as report_title, r.published_date,
               r.market_regime, r.cycle_id
        FROM candidates c
        JOIN reports r ON c.report_id = r.report_id
        ORDER BY c.discovered_at DESC
    """).fetchall()

    metrics = []
    for c in candidates:
        m = _compute_candidate_metrics(dict(c), conn)
        metrics.append(m)

    # Portfolio summary
    summary = _compute_portfolio_summary(metrics)

    # Band performance
    band_perf = _compute_band_performance(metrics)

    # Edge quality analysis
    edge_analysis = _compute_edge_analysis(metrics)

    # Direction analysis
    direction_analysis = _compute_direction_analysis(metrics)

    # Propagation analysis
    prop_analysis = _compute_propagation_analysis(metrics)

    # Kill/watch validation
    kill_validation = _compute_kill_validation(metrics)

    # Staleness impact
    staleness_impact = _compute_staleness_impact(metrics)

    # Optimal timing
    timing_analysis = _compute_timing_analysis(metrics, conn)

    conn.close()

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "candidates": metrics,
        "summary": summary,
        "band_performance": band_perf,
        "edge_analysis": edge_analysis,
        "direction_analysis": direction_analysis,
        "propagation_analysis": prop_analysis,
        "kill_validation": kill_validation,
        "staleness_impact": staleness_impact,
        "timing_analysis": timing_analysis,
    }


def _compute_candidate_metrics(c, conn):
    """Compute edge validation metrics for a single candidate."""
    cid = c["id"]

    # Get price snapshots
    snapshots = conn.execute("""
        SELECT * FROM price_snapshots
        WHERE candidate_id = ?
        ORDER BY timestamp ASC
    """, (cid,)).fetchall()
    snapshots = [dict(s) for s in snapshots]

    # Get DD log
    dd_entries = conn.execute("""
        SELECT * FROM dd_log
        WHERE candidate_id = ?
        ORDER BY timestamp ASC
    """, (cid,)).fetchall()

    # Get journal entries (for position monitor)
    journal_entries = conn.execute("""
        SELECT * FROM trader_journal
        WHERE candidate_id = ?
        ORDER BY cycle_number DESC
        LIMIT 5
    """, (cid,)).fetchall()
    journal_entries = [dict(j) for j in journal_entries]

    # Entry price
    entry_price = c.get("entry_price")
    direction = c.get("direction", "MIXED")

    # Report price
    prices_json = c.get("prices_at_report") or "{}"
    try:
        prices = json.loads(prices_json)
    except (json.JSONDecodeError, TypeError):
        prices = {}
    primary_ticker = c.get("primary_ticker", "")
    report_price = prices.get(primary_ticker, 0)

    # Current price (latest snapshot)
    current_price = snapshots[-1]["price"] if snapshots else None
    current_pnl = None
    peak_gain = 0
    max_drawdown = 0

    # Reference price for WATCH positions (dd_approved or report price)
    dd_approved_price = c.get("dd_approved_price")
    watch_ref_price = dd_approved_price or report_price

    # Build timeline
    timeline = []
    for snap in snapshots:
        pt = {
            "time": snap["timestamp"][:16].replace("T", " ") if snap["timestamp"] else "",
            "hours": snap.get("hours_since_discovery", 0),
            "price": snap["price"],
            "pnl_pct": snap.get("pnl_pct"),
        }

        # Status color — ACTIVE/PUBLISH use entry_price, WATCH uses dd_approved/report price
        if entry_price and snap["price"] and c["state"] in ("ACTIVE", "PUBLISH"):
            pnl = _calc_pnl(entry_price, snap["price"], direction)
            pt["pnl_pct"] = pnl
            if pnl > 0:
                pt["status"] = "green"
            elif pnl > -2:
                pt["status"] = "orange"
            else:
                pt["status"] = "red"

            peak_gain = max(peak_gain, pnl)
            max_drawdown = min(max_drawdown, pnl)
        elif c["state"] == "WATCH" and watch_ref_price and snap["price"]:
            # WATCH positions: purple P&L from dd_approved/report price
            pnl = _calc_pnl(watch_ref_price, snap["price"], direction)
            pt["pnl_pct"] = pnl
            pt["status"] = "purple"  # Always purple for watching
            pt["watched"] = True
        elif entry_price and snap["price"]:
            # Killed positions with entry price
            pnl = _calc_pnl(entry_price, snap["price"], direction)
            pt["pnl_pct"] = pnl
            if pnl > 0:
                pt["status"] = "green"
            elif pnl > -2:
                pt["status"] = "orange"
            else:
                pt["status"] = "red"
            peak_gain = max(peak_gain, pnl)
            max_drawdown = min(max_drawdown, pnl)
        else:
            pt["status"] = "grey"

        # Mark post-kill/watch points
        if c.get("killed_at"):
            try:
                kill_time = datetime.fromisoformat(c["killed_at"])
                snap_time = datetime.fromisoformat(snap["timestamp"])
                if snap_time >= kill_time:
                    pt["killed"] = True
                    pt["status"] = "purple"
            except (ValueError, TypeError):
                pass

        if c.get("state") == "WATCH":
            pt["watched"] = True

        timeline.append(pt)

    if snapshots and entry_price:
        current_pnl = _calc_pnl(entry_price, snapshots[-1]["price"], direction)

    # Report P&L — movement since dd_approved_price (or report_price as fallback)
    # This shows how the stock has moved since we first looked, regardless of trade entry
    report_pnl = None
    dd_approved_price = c.get("dd_approved_price")
    ref_price = dd_approved_price or report_price  # DD price preferred, report price fallback
    if snapshots and ref_price and ref_price > 0:
        report_pnl = _calc_pnl(ref_price, snapshots[-1]["price"], direction)

    # Status determination
    status = "grey"
    if c["state"] in ("ACTIVE", "PUBLISH"):
        if current_pnl is not None:
            if current_pnl > 0:
                status = "green"
            elif current_pnl > -2:
                status = "orange"
            else:
                status = "red"
    elif c["state"] == "WATCH":
        status = "purple"
    elif c["state"] == "KILLED":
        status = "killed"
    elif c["state"] == "PENDING":
        status = "grey"

    # Kill hour
    kill_hour = None
    if c.get("killed_at") and c.get("discovered_at"):
        try:
            disc = datetime.fromisoformat(c["discovered_at"])
            kill = datetime.fromisoformat(c["killed_at"])
            kill_hour = (kill - disc).total_seconds() / 3600
        except (ValueError, TypeError):
            pass

    # Extract latest journal metadata
    latest_conviction = c.get("current_conviction")
    latest_thesis_status = None
    latest_watching_for = None
    latest_concerns = None
    latest_narrative_entries = []

    if journal_entries:
        latest_j = journal_entries[0]  # Most recent (DESC order)
        latest_conviction = latest_j.get("conviction_score") or latest_conviction
        latest_thesis_status = latest_j.get("thesis_status")
        latest_watching_for = latest_j.get("watching_for")
        latest_concerns = latest_j.get("concerns")
        # Last 2 narrative entries for display
        for j in journal_entries[:2]:
            if j.get("narrative"):
                latest_narrative_entries.append({
                    "cycle": j["cycle_number"],
                    "timestamp": (j.get("timestamp") or "")[:16],
                    "narrative": j["narrative"],
                    "conviction": j.get("conviction_score"),
                    "decision": j.get("decision"),
                    "thesis_status": j.get("thesis_status"),
                })

    # Alpha group classification — does this signal match our trading formula?
    is_alpha = (
        c.get("direction") in ALPHA_DIRECTIONS
        and c.get("band") in ALPHA_BANDS
    )

    return {
        **c,
        "snapshots": snapshots,
        "timeline": timeline,
        "current_price": current_price,
        "current_pnl": current_pnl,
        "report_pnl": round(report_pnl, 2) if report_pnl is not None else None,
        "dd_approved_price": dd_approved_price,
        "peak_gain": round(peak_gain, 2),
        "max_drawdown": round(max_drawdown, 2),
        "report_price": report_price,
        "status": status,
        "kill_hour": kill_hour,
        "dd_entries": [dict(d) for d in dd_entries],
        "journal_entries": journal_entries,
        "journal_count": len(journal_entries),
        "latest_conviction": latest_conviction,
        "latest_thesis_status": latest_thesis_status,
        "latest_watching_for": latest_watching_for,
        "latest_concerns": latest_concerns,
        "latest_narrative_entries": latest_narrative_entries,
        "snapshot_count": len(snapshots),
        "signal_velocity": c.get("signal_velocity") or "quiet",
        "signal_hits_24h": c.get("signal_hits_24h") or 0,
        "signal_query": c.get("signal_query") or "",
        "alpha": is_alpha,
    }


def _calc_pnl(entry, current, direction):
    """Calculate P&L percentage."""
    if not entry or not current or entry == 0:
        return 0
    if direction == "SHORT":
        return round((entry - current) / entry * 100, 2)
    elif direction == "LONG":
        return round((current - entry) / entry * 100, 2)
    return 0


def _compute_portfolio_summary(metrics):
    """Compute overall portfolio summary.
    Only count is_active positions for display metrics.
    """
    # Filter to active (tradeable) positions for summary counts
    tradeable = [m for m in metrics if m.get("is_active", 1)]
    active = [m for m in tradeable if m["state"] in ("ACTIVE", "PUBLISH")]
    killed = [m for m in tradeable if m["state"] == "KILLED"]
    watched = [m for m in tradeable if m["state"] == "WATCH"]
    pending = [m for m in tradeable if m["state"] == "PENDING"]
    published = [m for m in tradeable if m["state"] == "PUBLISH"]

    # Bot trade stats (old current_pnl based — kept for reference)
    bot_pnls = [m["current_pnl"] for m in tradeable
                if m["current_pnl"] is not None and m["state"] in ("ACTIVE", "PUBLISH", "KILLED")]
    bot_winners = [p for p in bot_pnls if p > 0]
    bot_losers = [p for p in bot_pnls if p <= 0]

    # Signal performance: report_pnl for ALL tradeable candidates
    signal_pnls = [m["report_pnl"] for m in tradeable if m["report_pnl"] is not None]
    signal_winners = [p for p in signal_pnls if p > 0]
    signal_losers = [p for p in signal_pnls if p <= 0]

    signal_winner_sum = sum(signal_winners) if signal_winners else 0
    signal_loser_sum = sum(signal_losers) if signal_losers else 0
    signal_profit_factor = abs(signal_winner_sum) / abs(signal_loser_sum) if signal_loser_sum else (
        float("inf") if signal_winner_sum > 0 else 0
    )

    # Alpha Group: signals matching our trading formula (LONG + Band A/B)
    alpha_all = [m for m in tradeable if m.get("alpha")]
    alpha_with_pnl = [m for m in alpha_all if m["report_pnl"] is not None]
    alpha_pnls = [m["report_pnl"] for m in alpha_with_pnl]
    alpha_winners = [p for p in alpha_pnls if p > 0]
    alpha_losers = [p for p in alpha_pnls if p <= 0]
    alpha_winner_sum = sum(alpha_winners) if alpha_winners else 0
    alpha_loser_sum = sum(alpha_losers) if alpha_losers else 0
    alpha_profit_factor = abs(alpha_winner_sum) / abs(alpha_loser_sum) if alpha_loser_sum else (
        float("inf") if alpha_winner_sum > 0 else 0
    )

    # Research Group: everything NOT alpha that has report_pnl
    research_all = [m for m in tradeable if not m.get("alpha")]
    research_with_pnl = [m for m in research_all if m["report_pnl"] is not None]
    research_pnls = [m["report_pnl"] for m in research_with_pnl]
    research_winners = [p for p in research_pnls if p > 0]

    # Pipeline: alpha candidates in WATCH state
    pipeline = [m for m in alpha_all if m["state"] == "WATCH"]

    return {
        "total_candidates": len(tradeable),
        "active_count": len(active),
        "publish_count": len(published),
        "killed_count": len(killed),
        "watch_count": len(watched),
        "pending_count": len(pending),
        "avg_peak_gain": round(
            sum(m["peak_gain"] for m in metrics if m["peak_gain"]) / max(len(metrics), 1), 2
        ),
        "short_count": len([m for m in metrics if m["direction"] == "SHORT"]),
        "long_count": len([m for m in metrics if m["direction"] == "LONG"]),
        # All signals (report_pnl based)
        "measurable_signals": len(signal_pnls),
        "signal_total_pnl": round(sum(signal_pnls), 2) if signal_pnls else 0,
        "signal_avg_pnl": round(sum(signal_pnls) / len(signal_pnls), 2) if signal_pnls else 0,
        "signal_win_rate": round(len(signal_winners) / len(signal_pnls) * 100, 1) if signal_pnls else 0,
        "signal_best": round(max(signal_pnls), 2) if signal_pnls else 0,
        "signal_worst": round(min(signal_pnls), 2) if signal_pnls else 0,
        "signal_profit_factor": round(signal_profit_factor, 2) if signal_profit_factor != float("inf") else 99.99,
        # Alpha Group stats (the headline numbers)
        "alpha_formula": ALPHA_FORMULA_DESC,
        "alpha_count": len(alpha_all),
        "alpha_measured": len(alpha_with_pnl),
        "alpha_total_pnl": round(sum(alpha_pnls), 2) if alpha_pnls else 0,
        "alpha_avg_pnl": round(sum(alpha_pnls) / len(alpha_pnls), 2) if alpha_pnls else 0,
        "alpha_win_rate": round(len(alpha_winners) / len(alpha_pnls) * 100, 1) if alpha_pnls else 0,
        "alpha_best": round(max(alpha_pnls), 2) if alpha_pnls else 0,
        "alpha_worst": round(min(alpha_pnls), 2) if alpha_pnls else 0,
        "alpha_profit_factor": round(alpha_profit_factor, 2) if alpha_profit_factor != float("inf") else 99.99,
        # Research Group stats (non-alpha, for comparison)
        "research_count": len(research_all),
        "research_measured": len(research_with_pnl),
        "research_total_pnl": round(sum(research_pnls), 2) if research_pnls else 0,
        "research_avg_pnl": round(sum(research_pnls) / len(research_pnls), 2) if research_pnls else 0,
        "research_win_rate": round(len(research_winners) / len(research_pnls) * 100, 1) if research_pnls else 0,
        "pipeline_count": len(pipeline),
        # Bot trade stats (kept for reference)
        "bot_total_pnl": round(sum(bot_pnls), 2) if bot_pnls else 0,
        "bot_win_rate": round(len(bot_winners) / len(bot_pnls) * 100, 1) if bot_pnls else 0,
    }


def _compute_band_performance(metrics):
    """Compute performance by confidence band (A-E)."""
    bands = {}
    for band_key in ["A", "B", "C", "D", "E"]:
        band_info = BANDS[band_key]
        members = [m for m in metrics if m.get("band") == band_key]

        # Signal stats from report_pnl (all members with data)
        signal_pnls = [m["report_pnl"] for m in members if m["report_pnl"] is not None]
        signal_winners = [p for p in signal_pnls if p > 0]

        # Old traded stats kept for reference
        traded = [m for m in members if m["state"] in ("ACTIVE", "PUBLISH", "KILLED")]

        bands[band_key] = {
            "label": band_info["label"],
            "color": band_info["color"],
            "bg": band_info["bg"],
            "count": len(members),
            "traded_count": len(traded),
            "signal_count": len(signal_pnls),
            "win_rate": round(len(signal_winners) / len(signal_pnls) * 100, 1) if signal_pnls else 0,
            "avg_pnl": round(sum(signal_pnls) / len(signal_pnls), 2) if signal_pnls else 0,
            "total_pnl": round(sum(signal_pnls), 2) if signal_pnls else 0,
            "best": max(signal_pnls) if signal_pnls else 0,
            "worst": min(signal_pnls) if signal_pnls else 0,
            "members": [{
                "asset_theme": m["asset_theme"],
                "primary_ticker": m["primary_ticker"],
                "direction": m["direction"],
                "confidence_pct": m["confidence_pct"],
                "state": m["state"],
                "current_pnl": m["current_pnl"],
                "report_pnl": m["report_pnl"],
                "status": m["status"],
            } for m in members],
        }

    return bands


def _compute_edge_analysis(metrics):
    """Compare HIGH vs DECAYING edge performance."""
    result = {}
    for eq in ["HIGH", "DECAYING"]:
        members = [m for m in metrics if m.get("edge_quality") == eq]
        pnls = [m["report_pnl"] for m in members if m["report_pnl"] is not None]
        winners = [p for p in pnls if p > 0]

        result[eq] = {
            "count": len(members),
            "traded": len(pnls),
            "win_rate": round(len(winners) / len(pnls) * 100, 1) if pnls else 0,
            "avg_pnl": round(sum(pnls) / len(pnls), 2) if pnls else 0,
        }
    return result


def _compute_direction_analysis(metrics):
    """Compare SHORT vs LONG performance."""
    result = {}
    for d in ["SHORT", "LONG", "MIXED"]:
        members = [m for m in metrics if m.get("direction") == d]
        pnls = [m["report_pnl"] for m in members if m["report_pnl"] is not None]
        winners = [p for p in pnls if p > 0]

        result[d] = {
            "count": len(members),
            "traded": len(pnls),
            "win_rate": round(len(winners) / len(pnls) * 100, 1) if pnls else 0,
            "avg_pnl": round(sum(pnls) / len(pnls), 2) if pnls else 0,
        }
    return result


def _compute_propagation_analysis(metrics):
    """Compare IGNITE vs CATALYTIC vs SILENT propagation posture performance."""
    result = {}
    for p in ["IGNITE", "CATALYTIC", "SILENT", "FRAGILE"]:
        members = [m for m in metrics if m.get("propagation") == p]
        pnls = [m["report_pnl"] for m in members if m["report_pnl"] is not None]
        winners = [p2 for p2 in pnls if p2 > 0]

        if members:
            result[p] = {
                "count": len(members),
                "traded": len(pnls),
                "win_rate": round(len(winners) / len(pnls) * 100, 1) if pnls else 0,
                "avg_pnl": round(sum(pnls) / len(pnls), 2) if pnls else 0,
            }
    return result


def _compute_kill_validation(metrics):
    """Analyze whether kills were correct decisions."""
    killed = [m for m in metrics if m["state"] == "KILLED" and m.get("killed_at")]

    good_kills = 0  # Price moved against thesis after kill (correct to exit)
    bad_kills = 0   # Price moved with thesis after kill (missed upside)
    neutral_kills = 0

    for m in killed:
        # Look at price movement after kill
        if not m.get("killed_at") or not m.get("timeline"):
            neutral_kills += 1
            continue

        post_kill = [t for t in m["timeline"] if t.get("killed")]
        if not post_kill:
            neutral_kills += 1
            continue

        # Did price move in thesis direction after kill?
        entry_or_kill_price = m.get("entry_price") or m.get("report_price", 0)
        if not entry_or_kill_price:
            neutral_kills += 1
            continue

        last_post_kill = post_kill[-1]
        if last_post_kill.get("price"):
            post_pnl = _calc_pnl(entry_or_kill_price, last_post_kill["price"], m["direction"])
            if post_pnl > 2:
                bad_kills += 1  # We would have been profitable
            elif post_pnl < -2:
                good_kills += 1  # Good thing we exited
            else:
                neutral_kills += 1

    total = good_kills + bad_kills + neutral_kills
    return {
        "total_kills": len(killed),
        "good_kills": good_kills,
        "bad_kills": bad_kills,
        "neutral_kills": neutral_kills,
        "kill_accuracy": round(good_kills / total * 100, 1) if total > 0 else 0,
        "by_killer": _kills_by_source(killed),
    }


def _kills_by_source(killed):
    """Break down kill accuracy by source."""
    sources = defaultdict(lambda: {"total": 0, "good": 0, "bad": 0})
    for m in killed:
        src = m.get("killed_by") or "unknown"
        sources[src]["total"] += 1
    return dict(sources)


def _compute_staleness_impact(metrics):
    """Analyze how staleness affects performance."""
    buckets = {
        "0-6h": {"min": 0, "max": 6},
        "6-24h": {"min": 6, "max": 24},
        "24-48h": {"min": 24, "max": 48},
        "48h+": {"min": 48, "max": 9999},
    }

    result = {}
    for label, bounds in buckets.items():
        members = []
        for m in metrics:
            if m.get("dd_entries"):
                first_dd = m["dd_entries"][0]
                staleness = first_dd.get("staleness_hours", 0)
                if bounds["min"] <= staleness < bounds["max"]:
                    members.append(m)

        pnls = [m["report_pnl"] for m in members if m["report_pnl"] is not None]
        winners = [p for p in pnls if p > 0]

        result[label] = {
            "count": len(members),
            "traded": len(pnls),
            "win_rate": round(len(winners) / len(pnls) * 100, 1) if pnls else 0,
            "avg_pnl": round(sum(pnls) / len(pnls), 2) if pnls else 0,
        }

    return result


def _compute_timing_analysis(metrics, conn):
    """Find optimal holding period by confidence band."""
    result = {}
    windows = ["0-6h", "6-12h", "12-24h", "24-48h", "48h+"]

    for band_key in ["A", "B", "C", "D", "E"]:
        band_members = [m for m in metrics
                       if m.get("band") == band_key and m["state"] in ("ACTIVE", "PUBLISH", "KILLED")]
        if not band_members:
            result[band_key] = {"best_window": "N/A", "windows": {}}
            continue

        window_perf = {}
        for w in windows:
            if w == "0-6h":
                lo, hi = 0, 6
            elif w == "6-12h":
                lo, hi = 6, 12
            elif w == "12-24h":
                lo, hi = 12, 24
            elif w == "24-48h":
                lo, hi = 24, 48
            else:
                lo, hi = 48, 9999

            pnls = []
            for m in band_members:
                for snap in m.get("snapshots", []):
                    h = snap.get("hours_since_entry")
                    if h is not None and lo <= h < hi and snap.get("pnl_pct") is not None:
                        pnls.append(snap["pnl_pct"])

            window_perf[w] = {
                "avg_pnl": round(sum(pnls) / len(pnls), 2) if pnls else 0,
                "data_points": len(pnls),
            }

        # Find best window
        best = max(window_perf.items(), key=lambda x: x[1]["avg_pnl"]) if window_perf else ("N/A", {})
        result[band_key] = {
            "best_window": best[0],
            "windows": window_perf,
        }

    return result


def generate_claude_briefing():
    """Generate text briefing optimized for Claude analysis."""
    data = generate_analytics()
    s = data["summary"]

    lines = []
    lines.append("=== HEDGE FUND EDGE TRACKER BRIEFING ===")
    lines.append("Generated: {}".format(data["generated_at"][:19]))
    lines.append("")
    lines.append("PORTFOLIO OVERVIEW:")
    lines.append("  Total candidates: {}".format(s["total_candidates"]))
    lines.append("  Active: {} | Watch: {} | Killed: {} | Pending: {}".format(
        s["active_count"], s["watch_count"], s["killed_count"], s["pending_count"]
    ))
    lines.append("  Measurable signals: {} | Win rate: {}% | Avg P&L: {}%".format(
        s["measurable_signals"], s["signal_win_rate"], s["signal_avg_pnl"]))
    lines.append("  Best: {}% | Worst: {}%".format(s["signal_best"], s["signal_worst"]))
    lines.append("  Direction: {} SHORT, {} LONG".format(s["short_count"], s["long_count"]))
    lines.append("")
    lines.append("ALPHA GROUP ({})".format(s.get("alpha_formula", "")))
    lines.append("  Signals: {} measured ({} total) | Win rate: {}% | Avg P&L: {}%".format(
        s.get("alpha_measured", 0), s.get("alpha_count", 0),
        s.get("alpha_win_rate", 0), s.get("alpha_avg_pnl", 0)))
    lines.append("  Total P&L: {}% | Profit Factor: {}x".format(
        s.get("alpha_total_pnl", 0), s.get("alpha_profit_factor", 0)))
    lines.append("  Best: {}% | Worst: {}%".format(
        s.get("alpha_best", 0), s.get("alpha_worst", 0)))
    lines.append("")
    lines.append("RESEARCH GROUP (non-alpha):")
    lines.append("  Signals: {} measured | Win rate: {}% | Avg P&L: {}%".format(
        s.get("research_measured", 0), s.get("research_win_rate", 0),
        s.get("research_avg_pnl", 0)))
    lines.append("")

    lines.append("CONFIDENCE BAND PERFORMANCE:")
    for band_key in ["A", "B", "C", "D", "E"]:
        bp = data["band_performance"].get(band_key, {})
        if bp.get("count", 0) > 0:
            lines.append("  Band {} ({}): {} candidates, {}% win rate, {}% avg P&L".format(
                band_key, bp["label"], bp["count"], bp["win_rate"], bp["avg_pnl"]
            ))
    lines.append("")

    lines.append("EDGE QUALITY:")
    for eq, ed in data["edge_analysis"].items():
        if ed["count"] > 0:
            lines.append("  {}: {} candidates, {}% win rate, {}% avg P&L".format(
                eq, ed["count"], ed["win_rate"], ed["avg_pnl"]
            ))
    lines.append("")

    lines.append("KILL VALIDATION:")
    kv = data["kill_validation"]
    lines.append("  {} kills total | {} good | {} bad | {} neutral".format(
        kv["total_kills"], kv["good_kills"], kv["bad_kills"], kv["neutral_kills"]
    ))
    lines.append("  Kill accuracy: {}%".format(kv["kill_accuracy"]))
    lines.append("")

    lines.append("INDIVIDUAL POSITIONS:")
    for m in data["candidates"]:
        state_icon = {
            "ACTIVE": "[ACTIVE]", "WATCH": "[WATCH]", "KILLED": "[KILLED]",
            "PENDING": "[PENDING]", "EXPIRED": "[EXPIRED]",
            "PUBLISH": "[PUBLISH]"
        }.get(m["state"], "[?]")
        pnl_str = "{}%".format(m["current_pnl"]) if m["current_pnl"] is not None else "N/A"
        conviction_str = ""
        if m.get("latest_conviction"):
            conviction_str = " conviction={}/10".format(m["latest_conviction"])
        thesis_str = ""
        if m.get("latest_thesis_status"):
            thesis_str = " thesis={}".format(m["latest_thesis_status"])
        lines.append("  {} {} ({}) {} {}% band={} P&L={} peak={}% dd={}{}{} journal={}".format(
            state_icon, m["asset_theme"][:40], m["primary_ticker"],
            m["direction"], m["confidence_pct"], m["band"],
            pnl_str, m["peak_gain"], m["dd_count"],
            conviction_str, thesis_str,
            m.get("journal_count", 0)
        ))
        if m.get("state_reason"):
            lines.append("    Reason: {}".format(m["state_reason"][:80]))
        if m.get("latest_watching_for"):
            lines.append("    Watching: {}".format(m["latest_watching_for"][:80]))
        if m.get("latest_concerns"):
            lines.append("    Concerns: {}".format(m["latest_concerns"][:80]))
        if m.get("signal_velocity") and m["signal_velocity"] != "quiet":
            lines.append("    Signal: {} ({} hits/24h)".format(
                m["signal_velocity"].upper(), m.get("signal_hits_24h", 0)))

    return "\n".join(lines)
