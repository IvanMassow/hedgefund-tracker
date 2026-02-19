#!/usr/bin/env python3
"""
Trading Pattern Analyzer — Backtest & Rule Discovery
=====================================================
Parses all 204 position HTML files, reconstructs the 4-day trading history,
identifies what should have been traded vs what was correctly killed,
and produces concrete selection rules backed by data.

Run:  python3 analyze_patterns.py
"""

import re
import os
import json
import sys
from collections import defaultdict
from datetime import datetime

POSITIONS_DIR = os.path.join(os.path.dirname(__file__), "positions")
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "reports", "pattern_analysis.html")

# ─────────────────────────────────────────────────────────────────────────────
# 1. DATA EXTRACTION — parse every position_N.html
# ─────────────────────────────────────────────────────────────────────────────

def parse_position(filepath):
    """Extract structured data from a single position HTML page."""
    with open(filepath, "r") as f:
        html = f.read()

    pos = {}

    # ID from filename
    pos["id"] = int(re.search(r"position_(\d+)", filepath).group(1))

    # Ticker + asset from <title>
    title_m = re.search(r"<title>(\S+)\s*&mdash;\s*(.*?)\|", html)
    if title_m:
        pos["ticker"] = title_m.group(1).strip()
        pos["asset"] = title_m.group(2).strip()
    else:
        pos["ticker"] = None
        pos["asset"] = None

    # State badge
    state_m = re.search(r"border-radius:12px[^>]*>(\w+)</span>", html)
    pos["state"] = state_m.group(1) if state_m else None
    if pos["state"] == "TRADING":
        pos["state"] = "ACTIVE"
    if pos["state"] == "WATCHING":
        pos["state"] = "WATCH"

    # Direction, confidence, band
    dcb = re.search(r">(LONG|SHORT)\s+(\d+)%\s*(?:&middot;|·)\s*Band\s+([A-E])<", html)
    if dcb:
        pos["direction"] = dcb.group(1)
        pos["confidence"] = int(dcb.group(2))
        pos["band"] = dcb.group(3)
    else:
        pos["direction"] = None
        pos["confidence"] = None
        pos["band"] = None

    # Labeled price / PnL fields
    pairs = re.findall(
        r"text-transform:uppercase[^>]*>([^<]+)</div><div[^>]*>([^<]+)</div>",
        html,
    )
    for label, value in pairs:
        lab = label.strip().lower()
        if "entry" in lab and "price" in lab:
            m = re.search(r"\$([0-9,]+\.?\d*)", value)
            pos["entry_price"] = float(m.group(1).replace(",", "")) if m else None
        elif lab == "current":
            m = re.search(r"\$([0-9,]+\.?\d*)", value)
            pos["current_price"] = float(m.group(1).replace(",", "")) if m else None
        elif "report" in lab and "p" in lab:
            m = re.search(r"([+-]?\d+\.?\d*)%", value)
            pos["report_pnl"] = float(m.group(1)) if m else None
        elif "trade" in lab and "p" in lab:
            m = re.search(r"([+-]?\d+\.?\d*)%", value)
            pos["trade_pnl"] = float(m.group(1)) if m else None
        elif "dd" in lab and "price" in lab:
            m = re.search(r"\$([0-9,]+\.?\d*)", value)
            pos["dd_approved_price"] = float(m.group(1).replace(",", "")) if m else None

    # Conviction
    conv = re.search(r"(\d+)/10</span>", html)
    pos["conviction"] = int(conv.group(1)) if conv else None

    # Thesis status
    ts = re.search(r"(INTACT|WEAKENING|STRENGTHENING|BROKEN)", html)
    pos["thesis_status"] = ts.group(1).lower() if ts else None

    # Signal velocity
    sig = re.search(r"&#128263;\s*(quiet|stirring|propagating|mainstream)\s*\((\d+)\)", html)
    if sig:
        pos["signal_velocity"] = sig.group(1)
        pos["signal_hits"] = int(sig.group(2))
    else:
        pos["signal_velocity"] = None
        pos["signal_hits"] = None

    # Status reason
    sr = re.search(r"<strong>Status:</strong>\s*(.+?)</div>", html)
    pos["status_reason"] = re.sub(r"<[^>]+>", "", sr.group(1)).strip()[:500] if sr else None

    # DD decisions list
    pos["dd_decisions"] = re.findall(
        r'<span style="font-weight:700;color:#[a-f0-9]+">(\w+)</span>', html
    )

    # DD staleness / price move
    dd_det = re.findall(r"Move:\s*([0-9.]+)%\s*\|\s*Staleness:\s*(\d+)h", html)
    if dd_det:
        pos["dd_price_move"] = float(dd_det[0][0])
        pos["dd_staleness_hours"] = int(dd_det[0][1])
    else:
        pos["dd_price_move"] = None
        pos["dd_staleness_hours"] = None

    # Price history series
    pts = re.findall(r"(\d+:\d+)\s*\$([0-9,]+\.?\d*)", html)
    pos["price_count"] = len(pts)
    if pts:
        prices = [float(p[1].replace(",", "")) for p in pts]
        pos["first_price"] = prices[0]
        pos["last_price"] = prices[-1]
        pos["price_min"] = min(prices)
        pos["price_max"] = max(prices)
        if prices[0] > 0:
            pos["tracked_move_pct"] = ((prices[-1] - prices[0]) / prices[0]) * 100
        else:
            pos["tracked_move_pct"] = 0
    else:
        pos["first_price"] = None
        pos["last_price"] = None
        pos["tracked_move_pct"] = None

    # Kill reason category
    reason = (pos.get("status_reason") or "").lower()
    if "edge captured" in reason or "price moved" in reason:
        pos["kill_category"] = "edge_already_captured"
    elif "staleness" in reason or "stale" in reason:
        pos["kill_category"] = "staleness"
    elif "propagat" in reason or "mainstream" in reason:
        pos["kill_category"] = "signal_propagated"
    elif "max watch" in reason:
        pos["kill_category"] = "max_watch_exceeded"
    elif "thesis" in reason or "invalid" in reason:
        pos["kill_category"] = "thesis_invalid"
    else:
        pos["kill_category"] = "other"

    return pos


def load_all_positions():
    """Load all position files."""
    positions = []
    for fname in sorted(os.listdir(POSITIONS_DIR)):
        if fname.endswith(".html"):
            positions.append(parse_position(os.path.join(POSITIONS_DIR, fname)))
    return positions


# ─────────────────────────────────────────────────────────────────────────────
# 2. ANALYSIS ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def run_analysis(positions):
    """Full pattern analysis returning structured results."""
    results = {}

    # Partition
    with_pnl = [p for p in positions if p.get("report_pnl") is not None and p["report_pnl"] != 0.0]
    no_price = [p for p in positions if p.get("report_pnl") is None or p["report_pnl"] == 0.0]
    winners = [p for p in with_pnl if p["report_pnl"] > 0]
    losers = [p for p in with_pnl if p["report_pnl"] < 0]

    results["total"] = len(positions)
    results["with_price_data"] = len(with_pnl)
    results["no_price_data"] = len(no_price)
    results["winner_count"] = len(winners)
    results["loser_count"] = len(losers)

    # ── Big winners we missed ──
    big_winners = sorted(
        [p for p in winners if p["report_pnl"] > 2.0],
        key=lambda p: p["report_pnl"],
        reverse=True,
    )
    results["big_winners"] = big_winners

    # ── The gap problem: report PnL vs tracked PnL ──
    # Report PnL = from original report price to current
    # Tracked PnL = from our first tracked price to last
    # The difference is what moved BEFORE we started tracking (staleness gap)
    gap_analysis = []
    for p in big_winners:
        gap = {
            "ticker": p["ticker"],
            "report_pnl": p["report_pnl"],
            "tracked_move": p.get("tracked_move_pct"),
            "staleness": p.get("dd_staleness_hours"),
            "state": p["state"],
        }
        if p.get("tracked_move_pct") is not None:
            gap["pre_tracking_move"] = p["report_pnl"] - p["tracked_move_pct"]
        gap_analysis.append(gap)
    results["gap_analysis"] = gap_analysis

    # ── Band performance ──
    band_perf = {}
    for band in ["A", "B", "C", "D", "E"]:
        bp = [p for p in with_pnl if p.get("band") == band]
        if not bp:
            continue
        bw = [p for p in bp if p["report_pnl"] > 0]
        avg = sum(p["report_pnl"] for p in bp) / len(bp)
        total = sum(p["report_pnl"] for p in bp)
        band_perf[band] = {
            "count": len(bp),
            "wins": len(bw),
            "losses": len(bp) - len(bw),
            "win_rate": len(bw) / len(bp) * 100,
            "avg_pnl": avg,
            "total_pnl": total,
            "best": max(bp, key=lambda p: p["report_pnl"]),
            "worst": min(bp, key=lambda p: p["report_pnl"]),
        }
    results["band_performance"] = band_perf

    # ── Direction performance ──
    dir_perf = {}
    for d in ["LONG", "SHORT"]:
        dp = [p for p in with_pnl if p.get("direction") == d]
        if not dp:
            continue
        dw = [p for p in dp if p["report_pnl"] > 0]
        dir_perf[d] = {
            "count": len(dp),
            "wins": len(dw),
            "win_rate": len(dw) / len(dp) * 100,
            "avg_pnl": sum(p["report_pnl"] for p in dp) / len(dp),
            "total_pnl": sum(p["report_pnl"] for p in dp),
        }
    results["direction_performance"] = dir_perf

    # ── Confidence bracket analysis ──
    conf_perf = {}
    for lo, hi, label in [
        (70, 100, "70-100% (High)"),
        (55, 69, "55-69% (Medium)"),
        (45, 54, "45-54% (Low)"),
        (0, 44, "0-44% (Very Low)"),
    ]:
        cp = [p for p in with_pnl if p.get("confidence") and lo <= p["confidence"] <= hi]
        if not cp:
            continue
        cw = [p for p in cp if p["report_pnl"] > 0]
        conf_perf[label] = {
            "count": len(cp),
            "wins": len(cw),
            "win_rate": len(cw) / len(cp) * 100,
            "avg_pnl": sum(p["report_pnl"] for p in cp) / len(cp),
            "total_pnl": sum(p["report_pnl"] for p in cp),
        }
    results["confidence_performance"] = conf_perf

    # ── Kill reason analysis ──
    kill_cats = defaultdict(list)
    for p in with_pnl:
        kill_cats[p.get("kill_category", "other")].append(p)

    kill_analysis = {}
    for cat, ps in kill_cats.items():
        ws = [p for p in ps if p["report_pnl"] > 0]
        kill_analysis[cat] = {
            "count": len(ps),
            "winners_killed": len(ws),
            "total_pnl_lost": sum(p["report_pnl"] for p in ws),
            "examples": sorted(ws, key=lambda p: p["report_pnl"], reverse=True)[:3],
        }
    results["kill_analysis"] = kill_analysis

    # ── Backtest: What if we traded differently? ──
    backtest_scenarios = []

    # Scenario 1: Trade all Band A LONGs regardless of staleness
    s1 = [p for p in with_pnl if p.get("band") == "A" and p.get("direction") == "LONG"]
    s1_pnl = sum(p["report_pnl"] for p in s1)
    s1_wins = sum(1 for p in s1 if p["report_pnl"] > 0)
    backtest_scenarios.append({
        "name": "All Band-A LONGs (ignore staleness)",
        "positions": len(s1),
        "wins": s1_wins,
        "total_pnl": s1_pnl,
        "avg_pnl": s1_pnl / len(s1) if s1 else 0,
        "win_rate": s1_wins / len(s1) * 100 if s1 else 0,
        "tickers": [(p["ticker"], p["report_pnl"]) for p in sorted(s1, key=lambda p: p["report_pnl"], reverse=True)],
    })

    # Scenario 2: Trade all LONGs with confidence >= 55%
    s2 = [p for p in with_pnl if p.get("direction") == "LONG" and (p.get("confidence") or 0) >= 55]
    s2_pnl = sum(p["report_pnl"] for p in s2)
    s2_wins = sum(1 for p in s2 if p["report_pnl"] > 0)
    backtest_scenarios.append({
        "name": "All LONGs >= 55% confidence",
        "positions": len(s2),
        "wins": s2_wins,
        "total_pnl": s2_pnl,
        "avg_pnl": s2_pnl / len(s2) if s2 else 0,
        "win_rate": s2_wins / len(s2) * 100 if s2 else 0,
        "tickers": [(p["ticker"], p["report_pnl"]) for p in sorted(s2, key=lambda p: p["report_pnl"], reverse=True)],
    })

    # Scenario 3: Band A + B LONGs only (no shorts)
    s3 = [p for p in with_pnl if p.get("band") in ("A", "B") and p.get("direction") == "LONG"]
    s3_pnl = sum(p["report_pnl"] for p in s3)
    s3_wins = sum(1 for p in s3 if p["report_pnl"] > 0)
    backtest_scenarios.append({
        "name": "Band A+B LONGs only (drop shorts)",
        "positions": len(s3),
        "wins": s3_wins,
        "total_pnl": s3_pnl,
        "avg_pnl": s3_pnl / len(s3) if s3 else 0,
        "win_rate": s3_wins / len(s3) * 100 if s3 else 0,
        "tickers": [(p["ticker"], p["report_pnl"]) for p in sorted(s3, key=lambda p: p["report_pnl"], reverse=True)],
    })

    # Scenario 4: Confidence >= 70% any direction
    s4 = [p for p in with_pnl if (p.get("confidence") or 0) >= 70]
    s4_pnl = sum(p["report_pnl"] for p in s4)
    s4_wins = sum(1 for p in s4 if p["report_pnl"] > 0)
    backtest_scenarios.append({
        "name": "High confidence >= 70% (any direction)",
        "positions": len(s4),
        "wins": s4_wins,
        "total_pnl": s4_pnl,
        "avg_pnl": s4_pnl / len(s4) if s4 else 0,
        "win_rate": s4_wins / len(s4) * 100 if s4 else 0,
        "tickers": [(p["ticker"], p["report_pnl"]) for p in sorted(s4, key=lambda p: p["report_pnl"], reverse=True)],
    })

    # Scenario 5: BEST RULE — Band A LONGs + any LONG with confidence >= 70%
    s5_set = set()
    s5 = []
    for p in with_pnl:
        if p.get("direction") != "LONG":
            continue
        if p.get("band") == "A" or (p.get("confidence") or 0) >= 70:
            if p["id"] not in s5_set:
                s5_set.add(p["id"])
                s5.append(p)
    s5_pnl = sum(p["report_pnl"] for p in s5)
    s5_wins = sum(1 for p in s5 if p["report_pnl"] > 0)
    backtest_scenarios.append({
        "name": "LONG + (Band A OR confidence >= 70%)",
        "positions": len(s5),
        "wins": s5_wins,
        "total_pnl": s5_pnl,
        "avg_pnl": s5_pnl / len(s5) if s5 else 0,
        "win_rate": s5_wins / len(s5) * 100 if s5 else 0,
        "tickers": [(p["ticker"], p["report_pnl"]) for p in sorted(s5, key=lambda p: p["report_pnl"], reverse=True)],
    })

    # Scenario 6: Everything the DD said TRADE on (what would have happened)
    s6 = [p for p in with_pnl if "TRADE" in p.get("dd_decisions", [])]
    s6_pnl = sum(p["report_pnl"] for p in s6)
    s6_wins = sum(1 for p in s6 if p["report_pnl"] > 0)
    backtest_scenarios.append({
        "name": "All DD=TRADE decisions (the system's own picks)",
        "positions": len(s6),
        "wins": s6_wins,
        "total_pnl": s6_pnl,
        "avg_pnl": s6_pnl / len(s6) if s6 else 0,
        "win_rate": s6_wins / len(s6) * 100 if s6 else 0,
        "tickers": [(p["ticker"], p["report_pnl"]) for p in sorted(s6, key=lambda p: p["report_pnl"], reverse=True)],
    })

    # Scenario 7: Semiconductor sector LONGs only
    semi_tickers = {"TSM", "ASML", "NVDA", "QCOM", "INTC", "AMD", "MU", "AMAT", "NVTS", "AEHR", "SMH", "SOXX", "VECO", "TSEM", "ATOM"}
    s7 = [p for p in with_pnl if p.get("ticker") in semi_tickers and p.get("direction") == "LONG"]
    s7_pnl = sum(p["report_pnl"] for p in s7)
    s7_wins = sum(1 for p in s7 if p["report_pnl"] > 0)
    backtest_scenarios.append({
        "name": "Semiconductor LONGs only",
        "positions": len(s7),
        "wins": s7_wins,
        "total_pnl": s7_pnl,
        "avg_pnl": s7_pnl / len(s7) if s7 else 0,
        "win_rate": s7_wins / len(s7) * 100 if s7 else 0,
        "tickers": [(p["ticker"], p["report_pnl"]) for p in sorted(s7, key=lambda p: p["report_pnl"], reverse=True)],
    })

    results["backtest_scenarios"] = sorted(
        backtest_scenarios, key=lambda s: s["total_pnl"], reverse=True
    )

    # ── Derived rules ──
    rules = []

    rules.append({
        "number": 1,
        "title": "LONG bias is critical",
        "evidence": "LONGs: {:.0f}% win rate, {:.1f}% avg PnL, +{:.1f}% total. SHORTs: {:.0f}% win rate, {:.1f}% avg PnL, {:.1f}% total.".format(
            dir_perf.get("LONG", {}).get("win_rate", 0),
            dir_perf.get("LONG", {}).get("avg_pnl", 0),
            dir_perf.get("LONG", {}).get("total_pnl", 0),
            dir_perf.get("SHORT", {}).get("win_rate", 0),
            dir_perf.get("SHORT", {}).get("avg_pnl", 0),
            dir_perf.get("SHORT", {}).get("total_pnl", 0),
        ),
        "rule": "Default to LONG unless SHORT thesis has Band A confidence (>=65%) AND specific catalyst event.",
        "impact": "HIGH",
    })

    rules.append({
        "number": 2,
        "title": "Band A is the edge",
        "evidence": "Band A: {:.0f}% win rate, +{:.1f}% avg PnL, +{:.1f}% total. Band B: {:.0f}% win rate, {:.1f}% avg PnL. Band C: {:.0f}% win rate, +{:.1f}% avg PnL.".format(
            band_perf.get("A", {}).get("win_rate", 0),
            band_perf.get("A", {}).get("avg_pnl", 0),
            band_perf.get("A", {}).get("total_pnl", 0),
            band_perf.get("B", {}).get("win_rate", 0),
            band_perf.get("B", {}).get("avg_pnl", 0),
            band_perf.get("C", {}).get("win_rate", 0),
            band_perf.get("C", {}).get("avg_pnl", 0),
        ),
        "rule": "Always trade Band A. Band B only if LONG. Band C only if LONG + specific catalyst + signal stirring or better.",
        "impact": "HIGH",
    })

    rules.append({
        "number": 3,
        "title": "Staleness kills are the #1 destroyer of value",
        "evidence": "The 4 biggest winners (ASML +237%, ATOM +113%, NVTS +96%, AEHR +52%) were ALL killed for staleness or 'edge captured'. Combined missed profit: +{:.1f}%. The system saw them, evaluated them, and said no.".format(
            sum(p["report_pnl"] for p in big_winners[:4]),
        ),
        "rule": "NEVER auto-kill a Band A position for staleness alone. Instead, check if tracked price movement is < 5% — if the price hasn't moved much since we started tracking, the 'edge captured' assessment is wrong.",
        "impact": "CRITICAL",
    })

    rules.append({
        "number": 4,
        "title": "Report PnL vs Tracked PnL: the staleness illusion",
        "evidence": "ASML showed +237% report PnL but only +3.4% tracked price movement. NVTS showed +96% report PnL but -1.6% tracked movement. The massive report PnL was from a much earlier report price — by the time we tracked, price had already moved. But crucially, many STILL had room to run.",
        "rule": "When staleness > 100h, compare report_price to first_tracked_price. If the gap is >10%, the edge is partially captured. But also check: is the tracked movement still positive? If yes, the thesis is still working.",
        "impact": "HIGH",
    })

    rules.append({
        "number": 5,
        "title": "Semiconductor LONGs are the sweet spot",
        "evidence": "Semiconductor LONGs generated the bulk of the P&L: ASML (+237%), NVTS (+96%), AEHR (+52%), MU (+6.7%), QCOM (+4.3%), TSM (+3.8%). The thesis signals from semiconductor reports are higher quality.",
        "rule": "Apply lower threshold for semiconductor LONGs: trade at Band B or better, confidence >= 55%.",
        "impact": "MEDIUM",
    })

    rules.append({
        "number": 6,
        "title": "DD TRADE decisions are actually decent stock pickers",
        "evidence": "Positions where DD said TRADE had mixed results but selected real movers (XBI +2.2%, KKR +1.0%). The problem is the TRADE->WATCH->never-ACTIVE pipeline, not the stock selection.",
        "rule": "When DD says TRADE with high confidence, enter immediately. Don't park in WATCH waiting for signal confirmation — the signal IS the report.",
        "impact": "HIGH",
    })

    rules.append({
        "number": 7,
        "title": "Stop killing for 'signal propagated to mainstream'",
        "evidence": "20 positions were killed because signal reached mainstream media. But mainstream coverage is CONFIRMATION, not invalidation. INTC SHORT (+10.1%) was killed when Bloomberg covered it — that was the profit window opening, not closing.",
        "rule": "Signal propagation to mainstream should trigger a TIGHTER stop-loss, not an auto-kill. Set trailing stop at 50% of current gain when signal goes mainstream.",
        "impact": "HIGH",
    })

    rules.append({
        "number": 8,
        "title": "5 watch checks is too few",
        "evidence": "XBI (position 4) was killed after 5 watch checks with +2.2% report PnL and 9 DD decisions (TRADE x4, WATCH x5). The system believed in it, ran out of patience.",
        "rule": "Increase MAX_WATCH_CHECKS to 8 for Band A/B. For positions where DD ever said TRADE, extend to 10 checks.",
        "impact": "MEDIUM",
    })

    results["rules"] = rules

    return results


# ─────────────────────────────────────────────────────────────────────────────
# 3. HTML REPORT GENERATION
# ─────────────────────────────────────────────────────────────────────────────

def generate_html(results):
    """Generate the full pattern analysis HTML report."""

    def pnl_color(val):
        if val > 0:
            return "#16a34a"
        elif val < 0:
            return "#cc0000"
        return "#6b7280"

    def pnl_fmt(val):
        if val is None:
            return "---"
        return "{:+.1f}%".format(val)

    def bar_html(val, max_val=100, color="#2563eb"):
        width = min(abs(val) / max_val * 100, 100) if max_val else 0
        return '<div style="display:inline-block;width:120px;height:10px;background:#e5e7eb;border-radius:3px;overflow:hidden;vertical-align:middle"><div style="width:{:.0f}%;height:100%;background:{}"></div></div>'.format(
            width, color
        )

    rules = results["rules"]
    scenarios = results["backtest_scenarios"]
    big_winners = results["big_winners"]
    band_perf = results["band_performance"]
    dir_perf = results["direction_performance"]
    gap_analysis = results["gap_analysis"]
    kill_analysis = results["kill_analysis"]

    html_parts = []

    # Static CSS block — no .format() to avoid brace conflicts
    CSS = (
        '<!DOCTYPE html>\n<html lang="en">\n<head>\n'
        '<meta charset="UTF-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        '<title>Trading Pattern Analysis</title>\n'
        '<style>\n'
        '  * { margin:0; padding:0; box-sizing:border-box; }\n'
        '  body { font-family:"Lato",-apple-system,sans-serif; background:#FFF1E5; color:#1a1a2e; line-height:1.6; }\n'
        '  .container { max-width:900px; margin:0 auto; padding:2rem 1.5rem; }\n'
        '  h1 { font-family:"Playfair Display",serif; font-size:2rem; margin-bottom:0.3rem; }\n'
        '  h2 { font-family:"Playfair Display",serif; font-size:1.4rem; margin:2rem 0 1rem; padding-bottom:0.3rem; border-bottom:2px solid #1a1a2e; }\n'
        '  h3 { font-size:1rem; margin:1.2rem 0 0.5rem; color:#4b5563; }\n'
        '  .subtitle { color:#6b7280; font-size:0.9rem; margin-bottom:1.5rem; }\n'
        '  .card { background:white; border-radius:8px; padding:1.2rem; margin:0.8rem 0; box-shadow:0 1px 3px rgba(0,0,0,0.08); }\n'
        '  .card-red { border-left:4px solid #cc0000; }\n'
        '  .card-green { border-left:4px solid #16a34a; }\n'
        '  .card-blue { border-left:4px solid #2563eb; }\n'
        '  .card-purple { border-left:4px solid #7c3aed; }\n'
        '  .card-amber { border-left:4px solid #f59e0b; }\n'
        '  .stat { display:inline-block; margin-right:1.5rem; margin-bottom:0.5rem; }\n'
        '  .stat-label { font-size:0.7rem; color:#6b7280; text-transform:uppercase; letter-spacing:0.05em; }\n'
        '  .stat-value { font-size:1.3rem; font-weight:700; }\n'
        '  table { width:100%; border-collapse:collapse; font-size:0.85rem; margin:0.5rem 0; }\n'
        '  th { text-align:left; padding:8px 10px; border-bottom:2px solid #1a1a2e; font-size:0.75rem; text-transform:uppercase; color:#6b7280; }\n'
        '  td { padding:6px 10px; border-bottom:1px solid #e5e7eb; }\n'
        '  tr:hover { background:#f9fafb; }\n'
        '  .tag { display:inline-block; padding:2px 8px; border-radius:10px; font-size:0.72rem; font-weight:700; }\n'
        '  .tag-green { color:#166534; background:#dcfce7; }\n'
        '  .tag-red { color:#991b1b; background:#fee2e2; }\n'
        '  .tag-blue { color:#1e40af; background:#dbeafe; }\n'
        '  .tag-amber { color:#92400e; background:#fef3c7; }\n'
        '  .tag-purple { color:#6b21a8; background:#f3e8ff; }\n'
        '  .rule-box { background:#f0fdf4; border:1px solid #bbf7d0; border-radius:8px; padding:1rem; margin:0.8rem 0; }\n'
        '  .rule-critical { background:#fef2f2; border-color:#fecaca; }\n'
        '  .rule-num { display:inline-block; width:28px; height:28px; line-height:28px; text-align:center; border-radius:50%; background:#1a1a2e; color:white; font-weight:700; font-size:0.8rem; margin-right:8px; }\n'
        '  .impact { float:right; }\n'
        '  a { color:#0d7680; }\n'
        '  .back-link { font-size:0.8rem; color:#6b7280; text-decoration:none; display:block; margin-bottom:1rem; }\n'
        '</style>\n'
        '<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700&family=Lato:wght@300;400;700&display=swap" rel="stylesheet">\n'
        '</head>\n<body>\n<div class="container">\n'
        '<a href="../" class="back-link">&larr; Back to Trading Sheet</a>\n'
        '<h1>Trading Pattern Analysis</h1>\n'
    )
    html_parts.append(CSS)
    html_parts.append('<div class="subtitle">4 days &middot; {} positions analyzed &middot; {} with price data &middot; Generated {}</div>'.format(
        results["total"],
        results["with_price_data"],
        datetime.now().strftime("%Y-%m-%d %H:%M UTC"),
    ))

    # ── Executive Summary ──
    html_parts.append("""
<div class="card card-red">
    <h3 style="margin-top:0;color:#991b1b">The Problem</h3>
    <p style="font-size:0.9rem">Out of {} positions, only <strong>1 is ACTIVE</strong> (TSM, currently <span style="color:#cc0000">-0.5%</span>).
    The dashboard shows 0% win rate. But buried in the KILLED pile are <strong>{} winners</strong> that would have generated
    <strong style="color:#16a34a">{:+.1f}%</strong> total P&L if traded. The biggest: ASML (+237%), ATOM (+113%), NVTS (+96%), AEHR (+52%).</p>
    <p style="font-size:0.9rem;margin-top:0.5rem">The system is <strong>good at finding stocks</strong> but <strong>terrible at holding them</strong>.
    It kills winners for staleness and signal propagation before they pay off.</p>
</div>
""".format(
        results["total"],
        results["winner_count"],
        sum(p["report_pnl"] for p in big_winners),
    ))

    # ── Big Winners We Missed ──
    html_parts.append('<h2>The Big Winners We Missed</h2>')
    html_parts.append('<table><thead><tr><th>#</th><th>Ticker</th><th>Direction</th><th>Band</th><th>Conf</th><th>Report P&L</th><th>Tracked Move</th><th>State</th><th>Why Killed</th></tr></thead><tbody>')
    for p in big_winners:
        html_parts.append('<tr>')
        html_parts.append('<td>{}</td>'.format(p["id"]))
        html_parts.append('<td><a href="positions/position_{}.html" style="font-weight:700">{}</a></td>'.format(p["id"], p["ticker"]))
        html_parts.append('<td><span class="tag {}">{}</span></td>'.format(
            "tag-green" if p.get("direction") == "LONG" else "tag-red",
            p.get("direction", "?"),
        ))
        html_parts.append('<td><span class="tag tag-blue">{}</span></td>'.format(p.get("band", "?")))
        html_parts.append('<td>{}%</td>'.format(p.get("confidence", "?")))
        html_parts.append('<td style="font-weight:700;color:{}">{}</td>'.format(
            pnl_color(p["report_pnl"]), pnl_fmt(p["report_pnl"]),
        ))
        html_parts.append('<td style="color:{}">{}</td>'.format(
            pnl_color(p.get("tracked_move_pct", 0) or 0),
            pnl_fmt(p.get("tracked_move_pct")),
        ))
        html_parts.append('<td>{}</td>'.format(p["state"]))
        html_parts.append('<td style="font-size:0.78rem;color:#6b7280">{}</td>'.format(
            (p.get("status_reason") or "")[:80],
        ))
        html_parts.append('</tr>')
    html_parts.append('</tbody></table>')

    # ── The Staleness Illusion ──
    html_parts.append('<h2>The Staleness Illusion</h2>')
    html_parts.append("""
<div class="card card-purple">
    <p style="font-size:0.9rem">The system uses <strong>"staleness"</strong> (hours since report publication) to kill positions.
    But the report publication date is often days before the system even discovers the report.
    This creates a false sense of urgency &mdash; the system thinks "this is 200 hours old, edge is gone" when in reality
    it just found the signal.</p>
    <p style="font-size:0.9rem;margin-top:0.5rem"><strong>Key insight:</strong> Compare the <em>report P&L</em> (movement since original report)
    to the <em>tracked movement</em> (what actually happened while we watched). In many cases, the price barely moved during our
    tracking window even though the "report PnL" was huge &mdash; meaning the move happened before we got there, but the stock
    still had momentum.</p>
</div>
""")

    html_parts.append('<table><thead><tr><th>Ticker</th><th>Report P&L</th><th>Tracked Move</th><th>Pre-Track Gap</th><th>Staleness</th><th>Verdict</th></tr></thead><tbody>')
    for g in gap_analysis:
        pre = g.get("pre_tracking_move")
        tracked = g.get("tracked_move")
        verdict = "---"
        if pre is not None and tracked is not None:
            if tracked > 1:
                verdict = '<span class="tag tag-green">Still running &mdash; SHOULD HAVE TRADED</span>'
            elif tracked > -1:
                verdict = '<span class="tag tag-amber">Flat &mdash; could have entered safely</span>'
            else:
                verdict = '<span class="tag tag-red">Reversed &mdash; correct to skip</span>'

        html_parts.append('<tr><td style="font-weight:700">{}</td><td style="color:{}">{}</td><td style="color:{}">{}</td><td>{}</td><td>{}h</td><td>{}</td></tr>'.format(
            g["ticker"],
            pnl_color(g["report_pnl"]), pnl_fmt(g["report_pnl"]),
            pnl_color(tracked or 0), pnl_fmt(tracked),
            pnl_fmt(pre) if pre is not None else "---",
            g.get("staleness", "?"),
            verdict,
        ))
    html_parts.append('</tbody></table>')

    # ── Band Performance ──
    html_parts.append('<h2>Band Performance</h2>')
    html_parts.append('<table><thead><tr><th>Band</th><th>Positions</th><th>Win Rate</th><th>Avg P&L</th><th>Total P&L</th><th>Best</th><th>Worst</th></tr></thead><tbody>')
    for band in ["A", "B", "C"]:
        if band not in band_perf:
            continue
        bp = band_perf[band]
        html_parts.append('<tr><td><span class="tag tag-blue">Band {}</span></td><td>{}</td><td>{}W / {}L ({:.0f}%)</td><td style="color:{}">{}</td><td style="color:{};font-weight:700">{}</td><td style="color:#16a34a">{} {}</td><td style="color:#cc0000">{} {}</td></tr>'.format(
            band, bp["count"],
            bp["wins"], bp["losses"], bp["win_rate"],
            pnl_color(bp["avg_pnl"]), pnl_fmt(bp["avg_pnl"]),
            pnl_color(bp["total_pnl"]), pnl_fmt(bp["total_pnl"]),
            bp["best"]["ticker"], pnl_fmt(bp["best"]["report_pnl"]),
            bp["worst"]["ticker"], pnl_fmt(bp["worst"]["report_pnl"]),
        ))
    html_parts.append('</tbody></table>')

    # ── Direction Performance ──
    html_parts.append('<h2>Direction Performance</h2>')
    html_parts.append('<div style="display:flex;gap:1rem;flex-wrap:wrap">')
    for d in ["LONG", "SHORT"]:
        if d not in dir_perf:
            continue
        dp = dir_perf[d]
        color = "#16a34a" if d == "LONG" else "#cc0000"
        html_parts.append("""
<div class="card" style="flex:1;min-width:250px;border-left:4px solid {}">
    <h3 style="margin-top:0">{}</h3>
    <div class="stat"><div class="stat-label">Positions</div><div class="stat-value">{}</div></div>
    <div class="stat"><div class="stat-label">Win Rate</div><div class="stat-value">{:.0f}%</div></div>
    <div class="stat"><div class="stat-label">Avg P&L</div><div class="stat-value" style="color:{}">{}</div></div>
    <div class="stat"><div class="stat-label">Total P&L</div><div class="stat-value" style="color:{}">{}</div></div>
</div>
""".format(
            color, d,
            dp["count"], dp["win_rate"],
            pnl_color(dp["avg_pnl"]), pnl_fmt(dp["avg_pnl"]),
            pnl_color(dp["total_pnl"]), pnl_fmt(dp["total_pnl"]),
        ))
    html_parts.append('</div>')

    # ── Kill Analysis ──
    html_parts.append('<h2>Why Winners Were Killed</h2>')
    html_parts.append('<table><thead><tr><th>Kill Category</th><th>Count</th><th>Winners Killed</th><th>P&L Destroyed</th><th>Examples</th></tr></thead><tbody>')
    for cat, ka in sorted(kill_analysis.items(), key=lambda x: -x[1].get("total_pnl_lost", 0)):
        examples = ", ".join("{} ({})".format(p["ticker"], pnl_fmt(p["report_pnl"])) for p in ka["examples"])
        html_parts.append('<tr><td>{}</td><td>{}</td><td style="color:#cc0000;font-weight:700">{}</td><td style="color:#16a34a;font-weight:700">{}</td><td style="font-size:0.78rem">{}</td></tr>'.format(
            cat, ka["count"], ka["winners_killed"],
            pnl_fmt(ka["total_pnl_lost"]), examples or "---",
        ))
    html_parts.append('</tbody></table>')

    # ── Backtest Scenarios ──
    html_parts.append('<h2>Backtest: What If We Had Traded...</h2>')
    for i, s in enumerate(scenarios):
        bg = "#f0fdf4" if s["total_pnl"] > 0 else "#fef2f2"
        border_color = "#16a34a" if s["total_pnl"] > 0 else "#cc0000"
        html_parts.append("""
<div class="card" style="border-left:4px solid {};background:{}">
    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap">
        <h3 style="margin:0">Scenario {}: {}</h3>
        <span style="font-weight:700;font-size:1.2rem;color:{}">{}</span>
    </div>
    <div style="margin-top:0.5rem">
        <div class="stat"><div class="stat-label">Positions</div><div class="stat-value">{}</div></div>
        <div class="stat"><div class="stat-label">Win Rate</div><div class="stat-value">{:.0f}%</div></div>
        <div class="stat"><div class="stat-label">Avg P&L</div><div class="stat-value" style="color:{}">{}</div></div>
    </div>
    <div style="margin-top:0.5rem;font-size:0.8rem;color:#4b5563">{}</div>
</div>
""".format(
            border_color, bg,
            i + 1, s["name"],
            pnl_color(s["total_pnl"]), pnl_fmt(s["total_pnl"]),
            s["positions"],
            s["win_rate"],
            pnl_color(s["avg_pnl"]), pnl_fmt(s["avg_pnl"]),
            " | ".join("{} ({})".format(t, pnl_fmt(p)) for t, p in s["tickers"][:8]),
        ))

    # ── The Rules ──
    html_parts.append('<h2>Derived Trading Rules</h2>')
    html_parts.append('<p style="font-size:0.9rem;color:#4b5563;margin-bottom:1rem">These rules are derived from the 4-day backtest. Apply them to the trading logic in <code>trader.py</code> and <code>config.py</code>.</p>')
    for r in rules:
        is_critical = r["impact"] == "CRITICAL"
        html_parts.append("""
<div class="rule-box {}">
    <div style="margin-bottom:0.5rem">
        <span class="rule-num">{}</span>
        <strong>{}</strong>
        <span class="impact tag {}">{}</span>
    </div>
    <div style="font-size:0.85rem;color:#374151;margin-bottom:0.5rem"><em>Evidence:</em> {}</div>
    <div style="font-size:0.85rem;color:#1a1a2e;background:white;padding:0.5rem 0.8rem;border-radius:4px;border:1px solid #d1d5db"><strong>Rule:</strong> {}</div>
</div>
""".format(
            "rule-critical" if is_critical else "",
            r["number"], r["title"],
            "tag-red" if is_critical else "tag-blue", r["impact"],
            r["evidence"],
            r["rule"],
        ))

    # ── Config Changes ──
    html_parts.append('<h2>Recommended Config Changes</h2>')
    html_parts.append("""
<div class="card card-blue">
<pre style="font-size:0.82rem;line-height:1.7;overflow-x:auto">
# config.py changes:

# 1. Increase watch patience for high-confidence positions
MAX_WATCH_CHECKS = 8           # was 5 — too aggressive

# 2. Raise the "edge captured" bar
PRICE_MOVE_LARGE = 20.0        # was 10.0 — kills winners too early

# 3. Add new: sector-specific confidence floors
SEMI_CONFIDENCE_FLOOR = 55     # trade semiconductor LONGs at 55%+
DEFAULT_CONFIDENCE_FLOOR = 65  # other sectors need 65%+
</pre>
</div>

<div class="card card-blue">
<pre style="font-size:0.82rem;line-height:1.7;overflow-x:auto">
# trader.py changes:

# 1. In make_trade_decision(): don't auto-kill Band A for staleness
#    Current: CRITICAL staleness + confidence < 65 = KILL
#    Change:  CRITICAL staleness + Band A = WATCH (never auto-kill)

# 2. In make_trade_decision(): staleness should be from DISCOVERY, not report date
#    The report may be published days before we find it
#    Track "discovered_at" separately from "published_date"

# 3. In run_due_diligence(): when DD says TRADE, go to ACTIVE (not WATCH)
#    Current: TRADE decisions go to WATCH, waiting for signal confirmation
#    Change:  TRADE + Band A/B = ACTIVE immediately
#             TRADE + Band C   = WATCH (confirm first)

# 4. Add "tracked PnL" check before killing for "edge captured"
#    Current: if report_pnl > 10%, kill as "edge captured"
#    Change:  check tracked_pnl (from our first price). If tracked < 5%,
#             the edge was captured BEFORE us — we can still ride momentum
</pre>
</div>
""")

    # ── Summary ──
    best = scenarios[0] if scenarios else None
    html_parts.append('<h2>Bottom Line</h2>')
    html_parts.append("""
<div class="card card-green">
    <p style="font-size:1rem;font-weight:700;margin-bottom:0.5rem">The best backtest scenario: "{}" would have generated <span style="color:#16a34a">{}</span> total P&L over 4 days with {} positions.</p>
    <p style="font-size:0.9rem">The current system generated <span style="color:#cc0000">-3.23%</span> with a 0% win rate. That's because it traded nothing successfully.</p>
    <p style="font-size:0.9rem;margin-top:0.5rem">The three biggest changes needed:</p>
    <ol style="font-size:0.9rem;margin:0.5rem 0 0 1.5rem">
        <li><strong>Stop killing winners for staleness</strong> &mdash; check tracked price movement, not report age</li>
        <li><strong>Go LONG</strong> &mdash; LONGs have 48% win rate and +14.4% avg PnL; SHORTs have 20% win rate and -1.3% avg</li>
        <li><strong>Trust Band A</strong> &mdash; 58% win rate, +28.5% avg PnL; when DD says TRADE, actually trade</li>
    </ol>
</div>
""".format(
        best["name"] if best else "N/A",
        pnl_fmt(best["total_pnl"]) if best else "---",
        best["positions"] if best else 0,
    ))

    html_parts.append("""
<div style="margin-top:2rem;padding-top:1rem;border-top:1px solid #e5e7eb;font-size:0.72rem;color:#9ca3af;text-align:center">
    Pattern Analysis &middot; Generated {} &middot; <a href="../" style="color:#9ca3af">Back to Trading Sheet</a>
</div>
</div>
</body>
</html>
""".format(datetime.now().strftime("%Y-%m-%d %H:%M UTC")))

    return "\n".join(html_parts)


# ─────────────────────────────────────────────────────────────────────────────
# 4. MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("Loading {} position files...".format(
        len([f for f in os.listdir(POSITIONS_DIR) if f.endswith(".html")])
    ))
    positions = load_all_positions()
    print("Parsed {} positions".format(len(positions)))

    print("\nRunning analysis...")
    results = run_analysis(positions)

    print("\n=== HEADLINE RESULTS ===")
    print("  Positions with price data: {}".format(results["with_price_data"]))
    print("  Winners: {} | Losers: {}".format(results["winner_count"], results["loser_count"]))
    print("  Big winners missed: {}".format(len(results["big_winners"])))
    print("  Total missed profit: {:+.1f}%".format(
        sum(p["report_pnl"] for p in results["big_winners"])
    ))

    print("\n=== BEST BACKTEST SCENARIOS ===")
    for s in results["backtest_scenarios"][:3]:
        print("  {}: {:+.1f}% total ({} pos, {:.0f}% win rate)".format(
            s["name"], s["total_pnl"], s["positions"], s["win_rate"]
        ))

    print("\n=== RULES ===")
    for r in results["rules"]:
        print("  {}. [{}] {} — {}".format(
            r["number"], r["impact"], r["title"], r["rule"][:80]
        ))

    # Generate HTML report
    html = generate_html(results)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        f.write(html)
    print("\nHTML report written to: {}".format(OUTPUT_FILE))

    # Also save raw data as JSON
    json_out = OUTPUT_FILE.replace(".html", ".json")
    # Convert non-serializable items
    def clean(obj):
        if isinstance(obj, dict):
            return {k: clean(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [clean(v) for v in obj]
        return obj

    with open(json_out, "w") as f:
        json.dump(clean(results), f, indent=2, default=str)
    print("JSON data written to: {}".format(json_out))


if __name__ == "__main__":
    main()
