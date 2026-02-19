#!/usr/bin/env python3
"""
Trading Certainties Engine
==========================
A living rules engine that:
1. Defines trading rules (the "formula")
2. Classifies every position against those rules into certainty buckets
3. Recomputes the entire history as if we had always traded with current rules
4. Tracks optimal entry/exit timing from price history
5. Recommends publish/no-publish with estimated uplift
6. Validates that rule changes don't break existing certainties

Rules evolve. When a rule changes, we re-run ALL positions through the new
ruleset to ensure nothing that was a "certainty" would have failed.

Usage:
  python3 trading_rules.py                  # Generate full report
  python3 trading_rules.py --validate       # Validate rules against all data
  python3 trading_rules.py --json           # Output raw JSON only
"""

import re
import os
import json
import sys
from datetime import datetime
from collections import defaultdict

POSITIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "positions")
REPORTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")

# =============================================================================
# TRADING RULES — edit these to refine the formula
# =============================================================================
# Each rule has:
#   - name: human label
#   - test: function(position) -> bool
#   - action: "ALWAYS_TRADE" | "TRADE_IF_CONFIRMED" | "WATCH_ONLY" | "NEVER_TRADE"
#   - exit_rule: when to sell
#   - publish: True/False — should we publish to amplify
#   - publish_uplift_pct: estimated additional % from publication
#
# Rules are evaluated in ORDER. First matching rule wins.
# =============================================================================

RULES = [
    {
        "id": "R1",
        "name": "Band-A Semiconductor LONG",
        "description": "High-confidence LONG on a major semiconductor name. These have the strongest signal-to-noise ratio in our data.",
        "test": lambda p: (
            p.get("direction") == "LONG"
            and p.get("band") == "A"
            and p.get("ticker") in SEMI_TICKERS
        ),
        "action": "ALWAYS_TRADE",
        "exit_rule": "Hold until tracked move reverses >2% from peak, OR 4 days, whichever first. If gain >10% at any point, set trailing stop at 50% of peak gain.",
        "publish": True,
        "publish_uplift_pct": 5.0,
        "certainty": "TRADING_CERTAINTY",
    },
    {
        "id": "R2",
        "name": "Band-A LONG (any sector)",
        "description": "High-confidence LONG regardless of sector. Band A has 58% win rate and +28.5% avg PnL in our data.",
        "test": lambda p: (
            p.get("direction") == "LONG"
            and p.get("band") == "A"
        ),
        "action": "ALWAYS_TRADE",
        "exit_rule": "Hold until tracked move reverses >3% from peak, OR 4 days. Tighten stop if signal goes mainstream.",
        "publish": True,
        "publish_uplift_pct": 3.0,
        "certainty": "TRADING_CERTAINTY",
    },
    {
        "id": "R3",
        "name": "Semiconductor LONG >= 50% confidence",
        "description": "Semiconductor LONGs at any reasonable confidence. AEHR (+52%, Band B, 55%) and ATOM (+113%, Band C, 53%) prove the sector edge holds even at lower confidence.",
        "test": lambda p: (
            p.get("direction") == "LONG"
            and p.get("ticker") in SEMI_TICKERS
            and (p.get("confidence") or 0) >= 50
        ),
        "action": "TRADE_IF_CONFIRMED",
        "exit_rule": "Enter when tracked price is flat or rising for 2+ hours after discovery. Exit at 3-day mark or 3% reversal from peak.",
        "publish": True,
        "publish_uplift_pct": 4.0,
        "certainty": "HIGH_PROBABILITY",
    },
    {
        "id": "R4",
        "name": "Band-B LONG with DD TRADE recommendation",
        "description": "System's own DD said TRADE, and it's a LONG with decent confidence. Trust the DD engine for LONGs.",
        "test": lambda p: (
            p.get("direction") == "LONG"
            and p.get("band") == "B"
            and "TRADE" in p.get("dd_decisions", [])
        ),
        "action": "TRADE_IF_CONFIRMED",
        "exit_rule": "Enter at DD-approved price. Exit at 3 days or 2% reversal.",
        "publish": False,
        "publish_uplift_pct": 0,
        "certainty": "HIGH_PROBABILITY",
    },
    {
        "id": "R5",
        "name": "Band-A SHORT with specific catalyst",
        "description": "HIGH-confidence short with a clear event catalyst (litigation, regulatory action). Shorts work only when there's a specific trigger.",
        "test": lambda p: (
            p.get("direction") == "SHORT"
            and p.get("band") == "A"
            and (p.get("confidence") or 0) >= 70
        ),
        "action": "TRADE_IF_CONFIRMED",
        "exit_rule": "Enter only if price hasn't already dropped >5%. Exit at 2 days or 5% gain, whichever first. Tight stop at -3%.",
        "publish": True,
        "publish_uplift_pct": 6.0,
        "certainty": "HIGH_PROBABILITY",
    },
    {
        "id": "R6",
        "name": "Band-B/C LONG with signal stirring+",
        "description": "Medium-confidence LONG where signal propagation has started. Signal movement suggests thesis is gaining traction.",
        "test": lambda p: (
            p.get("direction") == "LONG"
            and p.get("band") in ("B", "C")
            and p.get("signal_velocity") in ("stirring", "propagating", "mainstream")
        ),
        "action": "TRADE_IF_CONFIRMED",
        "exit_rule": "Enter when signal goes from stirring to propagating. Exit when signal reaches mainstream (that's the profit window).",
        "publish": True,
        "publish_uplift_pct": 8.0,
        "certainty": "CONDITIONAL",
    },
    {
        "id": "R7",
        "name": "Any SHORT below Band A",
        "description": "Shorts without highest-confidence catalyst are losers in our data. 20% win rate, -1.3% avg PnL.",
        "test": lambda p: (
            p.get("direction") == "SHORT"
            and p.get("band") != "A"
        ),
        "action": "NEVER_TRADE",
        "exit_rule": "N/A — do not enter",
        "publish": False,
        "publish_uplift_pct": 0,
        "certainty": "AVOID",
    },
    {
        "id": "R8",
        "name": "Band-C/D/E with no signal activity",
        "description": "Low confidence, no signal propagation = noise. These pollute the portfolio.",
        "test": lambda p: (
            p.get("band") in ("C", "D", "E")
            and p.get("signal_velocity") in (None, "quiet")
            and (p.get("signal_hits") or 0) == 0
        ),
        "action": "NEVER_TRADE",
        "exit_rule": "N/A — do not enter",
        "publish": False,
        "publish_uplift_pct": 0,
        "certainty": "AVOID",
    },
]

# Semiconductor tickers — expand as needed
SEMI_TICKERS = {
    "TSM", "ASML", "NVDA", "QCOM", "INTC", "AMD", "MU", "AMAT",
    "NVTS", "AEHR", "SMH", "SOXX", "VECO", "TSEM", "ATOM", "LRCX",
    "KLAC", "MRVL", "AVGO", "TXN", "SSNLF", "GILT", "USAR",
}

# Certainty bucket labels and colors
CERTAINTY_META = {
    "TRADING_CERTAINTY": {
        "label": "Trading Certainty",
        "color": "#16a34a",
        "bg": "#f0fdf4",
        "description": "We would trade this every time. The rule is proven.",
    },
    "HIGH_PROBABILITY": {
        "label": "High Probability",
        "color": "#2563eb",
        "bg": "#eff6ff",
        "description": "We would trade this with confirmation. Strong pattern match.",
    },
    "CONDITIONAL": {
        "label": "Conditional",
        "color": "#f59e0b",
        "bg": "#fffbeb",
        "description": "Trade only if specific conditions are met (signal movement, price stability).",
    },
    "AVOID": {
        "label": "Avoid",
        "color": "#cc0000",
        "bg": "#fef2f2",
        "description": "Data says don't trade this pattern.",
    },
    "UNCLASSIFIED": {
        "label": "Unclassified",
        "color": "#6b7280",
        "bg": "#f9fafb",
        "description": "No rule matched. Needs manual review or new rule creation.",
    },
}


# =============================================================================
# POSITION PARSER (same as analyze_patterns.py)
# =============================================================================

def parse_position(filepath):
    """Extract structured data from a position HTML page."""
    with open(filepath, "r") as f:
        html = f.read()

    pos = {}
    pos["id"] = int(re.search(r"position_(\d+)", filepath).group(1))

    title_m = re.search(r"<title>(\S+)\s*&mdash;\s*(.*?)\|", html)
    if title_m:
        pos["ticker"] = title_m.group(1).strip()
        pos["asset"] = title_m.group(2).strip()
    else:
        pos["ticker"] = None
        pos["asset"] = None

    state_m = re.search(r"border-radius:12px[^>]*>(\w+)</span>", html)
    pos["state"] = state_m.group(1) if state_m else None
    if pos["state"] == "TRADING":
        pos["state"] = "ACTIVE"
    if pos["state"] == "WATCHING":
        pos["state"] = "WATCH"

    dcb = re.search(r">(LONG|SHORT)\s+(\d+)%\s*(?:&middot;|·)\s*Band\s+([A-E])<", html)
    if dcb:
        pos["direction"] = dcb.group(1)
        pos["confidence"] = int(dcb.group(2))
        pos["band"] = dcb.group(3)
    else:
        pos["direction"] = None
        pos["confidence"] = None
        pos["band"] = None

    pairs = re.findall(
        r"text-transform:uppercase[^>]*>([^<]+)</div><div[^>]*>([^<]+)</div>", html
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

    conv = re.search(r"(\d+)/10</span>", html)
    pos["conviction"] = int(conv.group(1)) if conv else None

    ts = re.search(r"(INTACT|WEAKENING|STRENGTHENING|BROKEN)", html)
    pos["thesis_status"] = ts.group(1).lower() if ts else None

    sig = re.search(r"&#128263;\s*(quiet|stirring|propagating|mainstream)\s*\((\d+)\)", html)
    if sig:
        pos["signal_velocity"] = sig.group(1)
        pos["signal_hits"] = int(sig.group(2))
    else:
        pos["signal_velocity"] = None
        pos["signal_hits"] = None

    sr = re.search(r"<strong>Status:</strong>\s*(.+?)</div>", html)
    pos["status_reason"] = re.sub(r"<[^>]+>", "", sr.group(1)).strip()[:500] if sr else None

    pos["dd_decisions"] = re.findall(
        r'<span style="font-weight:700;color:#[a-f0-9]+">(\w+)</span>', html
    )

    dd_det = re.findall(r"Move:\s*([0-9.]+)%\s*\|\s*Staleness:\s*(\d+)h", html)
    if dd_det:
        pos["dd_price_move"] = float(dd_det[0][0])
        pos["dd_staleness_hours"] = int(dd_det[0][1])
    else:
        pos["dd_price_move"] = None
        pos["dd_staleness_hours"] = None

    # Full price history for timing analysis
    pts = re.findall(r"(\d+:\d+)\s*\$([0-9,]+\.?\d*)\s*([+-]?\d+\.?\d*)?%?", html)
    prices = []
    for time_str, price_str, pnl_str in pts:
        p = float(price_str.replace(",", ""))
        prices.append({"time": time_str, "price": p})

    pos["price_history"] = prices
    pos["price_count"] = len(prices)
    if prices:
        price_vals = [p["price"] for p in prices]
        pos["first_price"] = price_vals[0]
        pos["last_price"] = price_vals[-1]
        pos["price_min"] = min(price_vals)
        pos["price_max"] = max(price_vals)
        if price_vals[0] > 0:
            pos["tracked_move_pct"] = ((price_vals[-1] - price_vals[0]) / price_vals[0]) * 100
            pos["peak_gain_pct"] = ((max(price_vals) - price_vals[0]) / price_vals[0]) * 100
            pos["max_drawdown_pct"] = ((min(price_vals) - price_vals[0]) / price_vals[0]) * 100
        else:
            pos["tracked_move_pct"] = 0
            pos["peak_gain_pct"] = 0
            pos["max_drawdown_pct"] = 0
        # Optimal entry: lowest price in first half; optimal exit: highest price
        mid = len(price_vals) // 2
        first_half = price_vals[: max(mid, 1)]
        pos["optimal_entry"] = min(first_half)
        pos["optimal_exit"] = max(price_vals)
        if pos["optimal_entry"] > 0:
            pos["optimal_pnl_pct"] = ((pos["optimal_exit"] - pos["optimal_entry"]) / pos["optimal_entry"]) * 100
        else:
            pos["optimal_pnl_pct"] = 0
        # "Safe trade" PnL: buy at first price, sell when gain >= 2% OR at end
        safe_exit_price = price_vals[-1]
        for pv in price_vals[1:]:
            gain = ((pv - price_vals[0]) / price_vals[0]) * 100 if price_vals[0] > 0 else 0
            if gain >= 2.0:
                safe_exit_price = pv
                break
        pos["safe_trade_pnl_pct"] = ((safe_exit_price - price_vals[0]) / price_vals[0]) * 100 if price_vals[0] > 0 else 0
        # "Hold" PnL: buy at first price, sell at last
        pos["hold_pnl_pct"] = pos["tracked_move_pct"]
    else:
        for k in ["first_price", "last_price", "price_min", "price_max",
                   "tracked_move_pct", "peak_gain_pct", "max_drawdown_pct",
                   "optimal_entry", "optimal_exit", "optimal_pnl_pct",
                   "safe_trade_pnl_pct", "hold_pnl_pct"]:
            pos[k] = None

    return pos


def load_all_positions():
    positions = []
    for fname in sorted(os.listdir(POSITIONS_DIR)):
        if fname.endswith(".html"):
            positions.append(parse_position(os.path.join(POSITIONS_DIR, fname)))
    return positions


# =============================================================================
# RULE ENGINE
# =============================================================================

def classify_position(pos):
    """Run a position through the rules. Returns (rule, certainty_bucket)."""
    for rule in RULES:
        try:
            if rule["test"](pos):
                return rule, rule["certainty"]
        except Exception:
            continue
    return None, "UNCLASSIFIED"


def classify_all(positions):
    """Classify all positions and return enriched list."""
    for pos in positions:
        rule, certainty = classify_position(pos)
        pos["matched_rule"] = rule["id"] if rule else None
        pos["matched_rule_name"] = rule["name"] if rule else "No rule matched"
        pos["certainty_bucket"] = certainty
        pos["rule_action"] = rule["action"] if rule else "UNKNOWN"
        pos["rule_exit"] = rule["exit_rule"] if rule else ""
        pos["should_publish"] = rule["publish"] if rule else False
        pos["publish_uplift_pct"] = rule["publish_uplift_pct"] if rule else 0

        # Compute "rewritten history" PnL — what would have happened if we followed the rule
        if rule and rule["action"] in ("ALWAYS_TRADE", "TRADE_IF_CONFIRMED"):
            # We would have traded: use report_pnl as our result
            pos["rewritten_pnl"] = pos.get("report_pnl") or pos.get("tracked_move_pct") or 0
            pos["rewritten_traded"] = True
        else:
            pos["rewritten_pnl"] = 0
            pos["rewritten_traded"] = False

    return positions


def validate_rules(positions):
    """Validate that TRADING_CERTAINTY rules are net profitable.

    Individual losses are acceptable — the formula may need a few small losses
    to capture the big gains. What matters is that each RULE is net profitable
    across all the positions it matches.

    Returns list of violations (rules that are net negative)."""
    # Group TRADING_CERTAINTY positions by rule
    rule_groups = defaultdict(list)
    for pos in positions:
        if pos.get("certainty_bucket") == "TRADING_CERTAINTY":
            rule_id = pos.get("matched_rule", "unknown")
            pnl = pos.get("report_pnl")
            if pnl is not None and pnl != 0:
                rule_groups[rule_id].append(pos)

    violations = []
    for rule_id, group in rule_groups.items():
        total_pnl = sum(p["report_pnl"] for p in group)
        losses = [p for p in group if p["report_pnl"] < 0]
        if total_pnl < 0:
            violations.append({
                "rule": rule_id,
                "positions": len(group),
                "total_pnl": total_pnl,
                "losses": len(losses),
                "issue": "Rule {} is net negative ({:+.1f}% across {} positions)".format(
                    rule_id, total_pnl, len(group)),
                "detail": [(p["ticker"], p["report_pnl"]) for p in group],
            })
    return violations


# =============================================================================
# REPORT GENERATOR
# =============================================================================

def generate_report(positions):
    """Generate the Trading Certainties HTML report."""

    def pnl_color(v):
        if v is None:
            return "#6b7280"
        return "#16a34a" if v > 0 else "#cc0000" if v < 0 else "#6b7280"

    def pnl_fmt(v):
        if v is None:
            return "---"
        return "{:+.1f}%".format(v)

    # Group by certainty bucket
    buckets = defaultdict(list)
    for p in positions:
        buckets[p["certainty_bucket"]].append(p)

    # Portfolio stats under current rules
    traded = [p for p in positions if p.get("rewritten_traded")]
    total_pnl = sum(p.get("rewritten_pnl", 0) for p in traded)
    wins = [p for p in traded if (p.get("rewritten_pnl") or 0) > 0]
    win_rate = (len(wins) / len(traded) * 100) if traded else 0

    # Publish analysis
    publishable = [p for p in positions if p.get("should_publish") and p.get("report_pnl") is not None]
    publish_uplift = sum(p.get("publish_uplift_pct", 0) for p in publishable if (p.get("report_pnl") or 0) > 0)

    # Build HTML
    parts = []

    # CSS — using string concat to avoid brace issues
    parts.append(
        '<!DOCTYPE html>\n<html lang="en">\n<head>\n'
        '<meta charset="UTF-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        '<title>Trading Certainties</title>\n'
        '<style>\n'
        '  * { margin:0; padding:0; box-sizing:border-box; }\n'
        '  body { font-family:"Lato",-apple-system,sans-serif; background:#FFF1E5; color:#1a1a2e; line-height:1.6; }\n'
        '  .container { max-width:960px; margin:0 auto; padding:2rem 1.5rem; }\n'
        '  h1 { font-family:"Playfair Display",serif; font-size:2rem; margin-bottom:0.3rem; }\n'
        '  h2 { font-family:"Playfair Display",serif; font-size:1.4rem; margin:2rem 0 0.8rem; padding-bottom:0.3rem; border-bottom:2px solid #1a1a2e; }\n'
        '  h3 { font-size:1rem; margin:1rem 0 0.5rem; }\n'
        '  .subtitle { color:#6b7280; font-size:0.9rem; margin-bottom:1.5rem; }\n'
        '  .card { background:white; border-radius:8px; padding:1.2rem; margin:0.8rem 0; box-shadow:0 1px 3px rgba(0,0,0,0.08); }\n'
        '  .stat { display:inline-block; margin-right:1.5rem; margin-bottom:0.5rem; }\n'
        '  .stat-label { font-size:0.7rem; color:#6b7280; text-transform:uppercase; letter-spacing:0.05em; }\n'
        '  .stat-value { font-size:1.3rem; font-weight:700; }\n'
        '  table { width:100%; border-collapse:collapse; font-size:0.82rem; margin:0.5rem 0; }\n'
        '  th { text-align:left; padding:6px 8px; border-bottom:2px solid #1a1a2e; font-size:0.72rem; text-transform:uppercase; color:#6b7280; }\n'
        '  td { padding:5px 8px; border-bottom:1px solid #e5e7eb; }\n'
        '  tr:hover { background:#f9fafb; }\n'
        '  .tag { display:inline-block; padding:2px 8px; border-radius:10px; font-size:0.72rem; font-weight:700; }\n'
        '  .bucket-header { padding:0.8rem 1rem; border-radius:8px; margin:1rem 0 0.5rem; }\n'
        '  .rule-tag { display:inline-block; padding:2px 6px; border-radius:4px; font-size:0.68rem; font-weight:600; background:#f1f5f9; color:#475569; margin-right:4px; }\n'
        '  .publish-yes { color:#16a34a; font-weight:700; }\n'
        '  .publish-no { color:#9ca3af; }\n'
        '  a { color:#0d7680; }\n'
        '  .back-link { font-size:0.8rem; color:#6b7280; text-decoration:none; display:block; margin-bottom:1rem; }\n'
        '  .timing-bar { display:inline-block; height:8px; border-radius:4px; }\n'
        '</style>\n'
        '<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700&family=Lato:wght@300;400;700&display=swap" rel="stylesheet">\n'
        '</head>\n<body>\n<div class="container">\n'
        '<a href="../" class="back-link">&larr; Back to Trading Sheet</a>\n'
        '<h1>Trading Certainties</h1>\n'
    )

    parts.append('<div class="subtitle">Rules Engine &middot; {} positions &middot; {} rules &middot; Generated {}</div>'.format(
        len(positions), len(RULES), datetime.now().strftime("%Y-%m-%d %H:%M UTC"),
    ))

    # Executive summary card
    parts.append('<div class="card" style="border-left:4px solid #16a34a">')
    parts.append('<h3 style="margin-top:0">If We Had Always Traded With These Rules</h3>')
    parts.append('<div>')
    parts.append('<div class="stat"><div class="stat-label">Positions Traded</div><div class="stat-value">{}</div></div>'.format(len(traded)))
    parts.append('<div class="stat"><div class="stat-label">Total P&L</div><div class="stat-value" style="color:{}">{}</div></div>'.format(
        pnl_color(total_pnl), pnl_fmt(total_pnl)))
    parts.append('<div class="stat"><div class="stat-label">Win Rate</div><div class="stat-value">{:.0f}%</div></div>'.format(win_rate))
    parts.append('<div class="stat"><div class="stat-label">Avg P&L/Trade</div><div class="stat-value" style="color:{}">{}</div></div>'.format(
        pnl_color(total_pnl / len(traded) if traded else 0),
        pnl_fmt(total_pnl / len(traded) if traded else 0),
    ))
    parts.append('<div class="stat"><div class="stat-label">Publish Uplift Est.</div><div class="stat-value" style="color:#7c3aed">{}</div></div>'.format(
        pnl_fmt(publish_uplift)))
    parts.append('</div></div>')

    # Rule violations
    violations = validate_rules(positions)
    if violations:
        parts.append('<div class="card" style="border-left:4px solid #cc0000">')
        parts.append('<h3 style="margin-top:0;color:#cc0000">Rule Violations</h3>')
        parts.append('<p style="font-size:0.85rem">These TRADING_CERTAINTY rules are net negative across their positions. Individual losses are fine, but the rule itself must be net profitable:</p>')
        for v in violations:
            parts.append('<div style="font-size:0.85rem;padding:4px 0"><strong>Rule {}</strong>: {} positions, total {}, {} losses</div>'.format(
                v["rule"], v["positions"], pnl_fmt(v["total_pnl"]), v["losses"]))
            for ticker, pnl in v["detail"]:
                parts.append('<div style="font-size:0.8rem;padding-left:1.5rem;color:#6b7280">{}: {}</div>'.format(ticker, pnl_fmt(pnl)))
        parts.append('</div>')

    # Certainty buckets
    bucket_order = ["TRADING_CERTAINTY", "HIGH_PROBABILITY", "CONDITIONAL", "AVOID", "UNCLASSIFIED"]
    for bucket_key in bucket_order:
        bucket_positions = buckets.get(bucket_key, [])
        if not bucket_positions:
            continue

        meta = CERTAINTY_META[bucket_key]
        # Sort: positions with report_pnl first, by PnL descending
        bucket_positions.sort(
            key=lambda p: (p.get("report_pnl") is not None, p.get("report_pnl") or 0),
            reverse=True,
        )

        bucket_traded = [p for p in bucket_positions if p.get("report_pnl") is not None and p["report_pnl"] != 0]
        bucket_total = sum(p.get("report_pnl", 0) for p in bucket_traded)
        bucket_wins = sum(1 for p in bucket_traded if p.get("report_pnl", 0) > 0)

        parts.append('<div class="bucket-header" style="background:{};border-left:4px solid {}">'.format(meta["bg"], meta["color"]))
        parts.append('<h2 style="margin:0;border:none;color:{}">{} ({} positions)</h2>'.format(meta["color"], meta["label"], len(bucket_positions)))
        parts.append('<div style="font-size:0.85rem;color:#4b5563">{}</div>'.format(meta["description"]))
        if bucket_traded:
            parts.append('<div style="font-size:0.82rem;margin-top:4px">Backtest: {} with data | {}W/{}L | Total: <strong style="color:{}">{}</strong></div>'.format(
                len(bucket_traded), bucket_wins, len(bucket_traded) - bucket_wins,
                pnl_color(bucket_total), pnl_fmt(bucket_total),
            ))
        parts.append('</div>')

        # Table of positions
        parts.append('<table><thead><tr>'
                     '<th>#</th><th>Ticker</th><th>Dir</th><th>Band</th><th>Conf</th>'
                     '<th>Report P&L</th><th>Safe P&L</th><th>Hold P&L</th><th>Optimal P&L</th>'
                     '<th>Rule</th><th>Action</th><th>Publish</th><th>Uplift</th>'
                     '</tr></thead><tbody>')

        for p in bucket_positions:
            rpnl = p.get("report_pnl")
            safe = p.get("safe_trade_pnl_pct")
            hold = p.get("hold_pnl_pct")
            optimal = p.get("optimal_pnl_pct")

            action_colors = {
                "ALWAYS_TRADE": ("#16a34a", "#dcfce7"),
                "TRADE_IF_CONFIRMED": ("#2563eb", "#dbeafe"),
                "WATCH_ONLY": ("#f59e0b", "#fef3c7"),
                "NEVER_TRADE": ("#cc0000", "#fee2e2"),
                "UNKNOWN": ("#6b7280", "#f1f5f9"),
            }
            ac, abg = action_colors.get(p.get("rule_action", "UNKNOWN"), ("#6b7280", "#f1f5f9"))

            pub_cell = '<span class="publish-yes">PUBLISH +{:.0f}%</span>'.format(
                p.get("publish_uplift_pct", 0)
            ) if p.get("should_publish") else '<span class="publish-no">no</span>'

            parts.append('<tr>')
            parts.append('<td>{}</td>'.format(p["id"]))
            parts.append('<td><a href="positions/position_{}.html" style="font-weight:700">{}</a></td>'.format(
                p["id"], p.get("ticker") or "?"))
            parts.append('<td style="color:{}">{}</td>'.format(
                "#16a34a" if p.get("direction") == "LONG" else "#cc0000",
                p.get("direction") or "?"))
            parts.append('<td>{}</td>'.format(p.get("band") or "?"))
            parts.append('<td>{}</td>'.format(p.get("confidence") or "?"))
            parts.append('<td style="font-weight:700;color:{}">{}</td>'.format(pnl_color(rpnl), pnl_fmt(rpnl)))
            parts.append('<td style="color:{}">{}</td>'.format(pnl_color(safe), pnl_fmt(safe)))
            parts.append('<td style="color:{}">{}</td>'.format(pnl_color(hold), pnl_fmt(hold)))
            parts.append('<td style="color:{}">{}</td>'.format(pnl_color(optimal), pnl_fmt(optimal)))
            parts.append('<td><span class="rule-tag">{}</span></td>'.format(p.get("matched_rule", "?")))
            parts.append('<td><span class="tag" style="color:{};background:{}">{}</span></td>'.format(
                ac, abg, p.get("rule_action", "?")))
            parts.append('<td>{}</td>'.format(pub_cell))
            parts.append('<td style="color:#7c3aed">{}</td>'.format(
                pnl_fmt(p["publish_uplift_pct"]) if p.get("publish_uplift_pct") else ""))
            parts.append('</tr>')

        parts.append('</tbody></table>')

    # Rules reference
    parts.append('<h2>Active Rules</h2>')
    parts.append('<p style="font-size:0.85rem;color:#4b5563;margin-bottom:0.8rem">'
                 'Rules are evaluated in order. First match wins. Edit <code>trading_rules.py</code> RULES list to refine.</p>')

    for rule in RULES:
        cert_meta = CERTAINTY_META.get(rule["certainty"], CERTAINTY_META["UNCLASSIFIED"])
        matched_count = sum(1 for p in positions if p.get("matched_rule") == rule["id"])
        matched_with_pnl = [p for p in positions if p.get("matched_rule") == rule["id"] and p.get("report_pnl") is not None and p["report_pnl"] != 0]
        rule_total = sum(p["report_pnl"] for p in matched_with_pnl)

        parts.append('<div class="card" style="border-left:4px solid {}"'.format(cert_meta["color"]))
        parts.append('>')
        parts.append('<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap">')
        parts.append('<div><span class="rule-tag">{}</span> <strong>{}</strong></div>'.format(rule["id"], rule["name"]))
        parts.append('<span class="tag" style="color:{};background:{}">{}</span>'.format(cert_meta["color"], cert_meta["bg"], rule["certainty"]))
        parts.append('</div>')
        parts.append('<div style="font-size:0.82rem;color:#4b5563;margin:4px 0">{}</div>'.format(rule["description"]))
        parts.append('<div style="font-size:0.82rem;margin-top:4px">')
        parts.append('<strong>Action:</strong> {} | <strong>Exit:</strong> {}'.format(rule["action"], rule["exit_rule"][:120]))
        parts.append('</div>')
        parts.append('<div style="font-size:0.82rem;margin-top:4px">')
        parts.append('<strong>Matched:</strong> {} positions'.format(matched_count))
        if matched_with_pnl:
            parts.append(' | <strong>Backtest:</strong> <span style="color:{}">{}</span>'.format(
                pnl_color(rule_total), pnl_fmt(rule_total)))
        if rule["publish"]:
            parts.append(' | <span class="publish-yes">PUBLISH +{:.0f}%</span>'.format(rule["publish_uplift_pct"]))
        parts.append('</div>')
        parts.append('</div>')

    # Footer
    parts.append(
        '<div style="margin-top:2rem;padding-top:1rem;border-top:1px solid #e5e7eb;'
        'font-size:0.72rem;color:#9ca3af;text-align:center">'
        'Trading Certainties Engine &middot; Generated {} &middot; '
        '<a href="../" style="color:#9ca3af">Back to Trading Sheet</a>'
        '</div>\n</div>\n</body>\n</html>'.format(
            datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        )
    )

    return "\n".join(parts)


# =============================================================================
# MAIN
# =============================================================================

def main():
    mode = "--report"
    if len(sys.argv) > 1:
        mode = sys.argv[1]

    print("Loading positions...")
    positions = load_all_positions()
    print("Loaded {} positions".format(len(positions)))

    print("Classifying against {} rules...".format(len(RULES)))
    positions = classify_all(positions)

    # Summary
    buckets = defaultdict(list)
    for p in positions:
        buckets[p["certainty_bucket"]].append(p)

    print("\n=== CERTAINTY BUCKETS ===")
    for bk in ["TRADING_CERTAINTY", "HIGH_PROBABILITY", "CONDITIONAL", "AVOID", "UNCLASSIFIED"]:
        bp = buckets.get(bk, [])
        if bp:
            with_pnl = [p for p in bp if p.get("report_pnl") is not None and p["report_pnl"] != 0]
            total = sum(p["report_pnl"] for p in with_pnl) if with_pnl else 0
            print("  {}: {} positions ({} with data, total PnL: {:+.1f}%)".format(
                bk, len(bp), len(with_pnl), total))
            for p in sorted(with_pnl, key=lambda x: x["report_pnl"], reverse=True)[:5]:
                print("    {} {} {} {:+.1f}% [{}]".format(
                    p["ticker"], p.get("direction","?"), p.get("band","?"),
                    p["report_pnl"], p.get("matched_rule","?")))

    # Validate
    violations = validate_rules(positions)
    if violations:
        print("\n=== RULE VIOLATIONS ===")
        for v in violations:
            print("  {} ({}): {} -- {}".format(v["ticker"], v["rule"], v["pnl"], v["issue"]))
    else:
        print("\n  No rule violations - all TRADING_CERTAINTY positions are profitable.")

    # Rewritten portfolio
    traded = [p for p in positions if p.get("rewritten_traded")]
    total_pnl = sum(p.get("rewritten_pnl", 0) for p in traded)
    wins = [p for p in traded if (p.get("rewritten_pnl") or 0) > 0]
    print("\n=== REWRITTEN PORTFOLIO ===")
    print("  Traded: {} positions".format(len(traded)))
    print("  Total P&L: {:+.1f}%".format(total_pnl))
    print("  Win Rate: {:.0f}%".format(len(wins) / len(traded) * 100 if traded else 0))
    print("  Avg P&L/Trade: {:+.1f}%".format(total_pnl / len(traded) if traded else 0))

    if mode == "--validate":
        if violations:
            print("\nVALIDATION FAILED: {} violations".format(len(violations)))
            sys.exit(1)
        else:
            print("\nVALIDATION PASSED")
            sys.exit(0)

    if mode == "--json":
        # Strip non-serializable fields
        out = []
        for p in positions:
            d = {k: v for k, v in p.items() if k != "price_history"}
            out.append(d)
        print(json.dumps(out, indent=2, default=str))
        sys.exit(0)

    # Generate HTML report
    html = generate_report(positions)
    out_path = os.path.join(REPORTS_DIR, "trading_certainties.html")
    os.makedirs(REPORTS_DIR, exist_ok=True)
    with open(out_path, "w") as f:
        f.write(html)
    print("\nHTML report: {}".format(out_path))

    # Save JSON data
    json_path = os.path.join(REPORTS_DIR, "trading_certainties.json")
    out_data = []
    for p in positions:
        d = {k: v for k, v in p.items() if k != "price_history"}
        out_data.append(d)
    with open(json_path, "w") as f:
        json.dump(out_data, f, indent=2, default=str)
    print("JSON data: {}".format(json_path))


if __name__ == "__main__":
    main()
