"""
Hedge Fund Edge Tracker - Analytics & Learning
Performance metrics, confidence band analysis, and learning system.
"""
import json
import logging
from datetime import datetime, timezone
from collections import defaultdict

from db import get_conn
from config import BANDS

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

    # Build timeline
    timeline = []
    for snap in snapshots:
        pt = {
            "time": snap["timestamp"][:16].replace("T", " ") if snap["timestamp"] else "",
            "hours": snap.get("hours_since_discovery", 0),
            "price": snap["price"],
            "pnl_pct": snap.get("pnl_pct"),
        }

        # Status color
        if entry_price and snap["price"]:
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
            if pt["status"] == "grey":
                pt["status"] = "purple"

        timeline.append(pt)

    if snapshots and entry_price:
        current_pnl = _calc_pnl(entry_price, snapshots[-1]["price"], direction)

    # Status determination
    status = "grey"
    if c["state"] == "ACTIVE":
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

    return {
        **c,
        "snapshots": snapshots,
        "timeline": timeline,
        "current_price": current_price,
        "current_pnl": current_pnl,
        "peak_gain": round(peak_gain, 2),
        "max_drawdown": round(max_drawdown, 2),
        "report_price": report_price,
        "status": status,
        "kill_hour": kill_hour,
        "dd_entries": [dict(d) for d in dd_entries],
        "snapshot_count": len(snapshots),
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
    """Compute overall portfolio summary."""
    active = [m for m in metrics if m["state"] == "ACTIVE"]
    killed = [m for m in metrics if m["state"] == "KILLED"]
    watched = [m for m in metrics if m["state"] == "WATCH"]
    pending = [m for m in metrics if m["state"] == "PENDING"]

    active_pnls = [m["current_pnl"] for m in active if m["current_pnl"] is not None]
    all_pnls = [m["current_pnl"] for m in metrics
                if m["current_pnl"] is not None and m["state"] in ("ACTIVE", "KILLED")]

    winners = [p for p in all_pnls if p > 0]
    losers = [p for p in all_pnls if p <= 0]

    return {
        "total_candidates": len(metrics),
        "active_count": len(active),
        "killed_count": len(killed),
        "watch_count": len(watched),
        "pending_count": len(pending),
        "total_pnl": round(sum(all_pnls), 2) if all_pnls else 0,
        "avg_pnl": round(sum(all_pnls) / len(all_pnls), 2) if all_pnls else 0,
        "win_rate": round(len(winners) / len(all_pnls) * 100, 1) if all_pnls else 0,
        "best_trade": max(all_pnls) if all_pnls else 0,
        "worst_trade": min(all_pnls) if all_pnls else 0,
        "avg_peak_gain": round(
            sum(m["peak_gain"] for m in metrics if m["peak_gain"]) / max(len(metrics), 1), 2
        ),
        "short_count": len([m for m in metrics if m["direction"] == "SHORT"]),
        "long_count": len([m for m in metrics if m["direction"] == "LONG"]),
    }


def _compute_band_performance(metrics):
    """Compute performance by confidence band (A-E)."""
    bands = {}
    for band_key in ["A", "B", "C", "D", "E"]:
        band_info = BANDS[band_key]
        members = [m for m in metrics if m.get("band") == band_key]
        traded = [m for m in members if m["state"] in ("ACTIVE", "KILLED")]
        pnls = [m["current_pnl"] for m in traded if m["current_pnl"] is not None]
        winners = [p for p in pnls if p > 0]

        bands[band_key] = {
            "label": band_info["label"],
            "color": band_info["color"],
            "bg": band_info["bg"],
            "count": len(members),
            "traded_count": len(traded),
            "win_rate": round(len(winners) / len(pnls) * 100, 1) if pnls else 0,
            "avg_pnl": round(sum(pnls) / len(pnls), 2) if pnls else 0,
            "total_pnl": round(sum(pnls), 2) if pnls else 0,
            "best": max(pnls) if pnls else 0,
            "worst": min(pnls) if pnls else 0,
            "members": [{
                "asset_theme": m["asset_theme"],
                "primary_ticker": m["primary_ticker"],
                "direction": m["direction"],
                "confidence_pct": m["confidence_pct"],
                "state": m["state"],
                "current_pnl": m["current_pnl"],
                "status": m["status"],
            } for m in members],
        }

    return bands


def _compute_edge_analysis(metrics):
    """Compare HIGH vs DECAYING edge performance."""
    result = {}
    for eq in ["HIGH", "DECAYING"]:
        members = [m for m in metrics if m.get("edge_quality") == eq]
        traded = [m for m in members if m["state"] in ("ACTIVE", "KILLED")]
        pnls = [m["current_pnl"] for m in traded if m["current_pnl"] is not None]
        winners = [p for p in pnls if p > 0]

        result[eq] = {
            "count": len(members),
            "traded": len(traded),
            "win_rate": round(len(winners) / len(pnls) * 100, 1) if pnls else 0,
            "avg_pnl": round(sum(pnls) / len(pnls), 2) if pnls else 0,
        }
    return result


def _compute_direction_analysis(metrics):
    """Compare SHORT vs LONG performance."""
    result = {}
    for d in ["SHORT", "LONG", "MIXED"]:
        members = [m for m in metrics if m.get("direction") == d]
        traded = [m for m in members if m["state"] in ("ACTIVE", "KILLED")]
        pnls = [m["current_pnl"] for m in traded if m["current_pnl"] is not None]
        winners = [p for p in pnls if p > 0]

        result[d] = {
            "count": len(members),
            "traded": len(traded),
            "win_rate": round(len(winners) / len(pnls) * 100, 1) if pnls else 0,
            "avg_pnl": round(sum(pnls) / len(pnls), 2) if pnls else 0,
        }
    return result


def _compute_propagation_analysis(metrics):
    """Compare IGNITE vs CATALYTIC vs SILENT propagation posture performance."""
    result = {}
    for p in ["IGNITE", "CATALYTIC", "SILENT", "FRAGILE"]:
        members = [m for m in metrics if m.get("propagation") == p]
        traded = [m for m in members if m["state"] in ("ACTIVE", "KILLED")]
        pnls = [m["current_pnl"] for m in traded if m["current_pnl"] is not None]
        winners = [p2 for p2 in pnls if p2 > 0]

        if members:
            result[p] = {
                "count": len(members),
                "traded": len(traded),
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

        traded = [m for m in members if m["state"] in ("ACTIVE", "KILLED")]
        pnls = [m["current_pnl"] for m in traded if m["current_pnl"] is not None]
        winners = [p for p in pnls if p > 0]

        result[label] = {
            "count": len(members),
            "traded": len(traded),
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
                       if m.get("band") == band_key and m["state"] in ("ACTIVE", "KILLED")]
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
    lines.append("  Win rate: {}% | Avg P&L: {}%".format(s["win_rate"], s["avg_pnl"]))
    lines.append("  Best: {}% | Worst: {}%".format(s["best_trade"], s["worst_trade"]))
    lines.append("  Direction: {} SHORT, {} LONG".format(s["short_count"], s["long_count"]))
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
            "PENDING": "[PENDING]", "EXPIRED": "[EXPIRED]"
        }.get(m["state"], "[?]")
        pnl_str = "{}%".format(m["current_pnl"]) if m["current_pnl"] is not None else "N/A"
        lines.append("  {} {} ({}) {} {}% band={} P&L={} peak={}% dd={}".format(
            state_icon, m["asset_theme"][:40], m["primary_ticker"],
            m["direction"], m["confidence_pct"], m["band"],
            pnl_str, m["peak_gain"], m["dd_count"]
        ))
        if m.get("state_reason"):
            lines.append("    Reason: {}".format(m["state_reason"][:80]))

    return "\n".join(lines)
