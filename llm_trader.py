"""
Hedge Fund Edge Tracker - LLM Trader
Uses OpenAI GPT for due diligence assessment and trade decisions.
Also handles kill switch analysis when new reports arrive.
"""
import os
import json
import logging
from datetime import datetime, timezone

import requests as http_requests

from db import get_conn
from config import OPENAI_API_KEY

logger = logging.getLogger("hedgefund.llm_trader")

DD_SYSTEM_PROMPT = """You are a senior hedge fund trader's due diligence analyst. You assess whether a trading recommendation is still valid given elapsed time and market conditions.

CONTEXT:
- You receive trade recommendations from an information asymmetry analysis system
- Each recommendation has a thesis, evidence, confidence level, and direction (SHORT/LONG)
- Your job is to determine: should we TRADE this now, WATCH it (wait for better conditions), or KILL it (thesis is dead)?
- A TRADE means paper-entering the position at current price
- A WATCH means the thesis might still be valid but conditions aren't right — monitor and re-assess later
- A KILL means the thesis is invalidated or the opportunity has passed

ASSESSMENT CRITERIA:
1. STALENESS: How old is the underlying information? News from Friday being traded Monday is 64+ hours stale. Consider whether the market has already digested this.
2. PRICE MOVEMENT: Has the price already moved in the thesis direction? If so, the edge may be captured. Has it moved against? The thesis may be wrong.
3. THESIS VALIDITY: Given what you know about markets and the specific sector, is the reasoning still sound?
4. TIMING: Is this the right entry point? Would waiting improve the risk/reward?
5. RISK/REWARD: At current prices, is the trade still asymmetric?

DECISION GUIDELINES:
- TRADE: Thesis valid, price favorable, timing acceptable, risk/reward attractive
- WATCH: Thesis plausible but one or more conditions suboptimal. Set specific conditions for re-entry.
- KILL: Thesis invalidated, edge captured, risk/reward no longer attractive, or information too stale to act on

RESPONSE FORMAT (JSON only, no markdown):
{
    "decision": "TRADE" | "WATCH" | "KILL",
    "confidence": "HIGH" | "MEDIUM" | "LOW",
    "reason": "1-3 sentence explanation",
    "watch_conditions": ["condition 1", "condition 2"],
    "price_target": null or number,
    "risk_assessment": "1 sentence on key risk"
}"""

KILL_SWITCH_SYSTEM_PROMPT = """You are the kill switch analyst for a hedge fund paper trading system. Your PRIMARY job is to PROTECT the portfolio. Safety first. When in doubt, kill.

CONTEXT:
- The system paper-trades based on information asymmetry analysis of pharma, litigation, and regulatory signals
- Each position has a thesis, direction (SHORT/LONG), confidence, and entry price
- A "kill" means the thesis is invalidated — we close the paper trade
- Kill = sell. We'd rather miss upside than eat a loss.

YOUR TASK:
Given a NEW report with fresh candidates, determine if any EXISTING active positions should be killed.

Think laterally about:
1. Does any new information contradict an existing thesis?
2. Has a tripwire/catalyst condition been triggered?
3. Have market conditions changed materially?
4. Is there a thematic connection that weakens an existing position?
5. Has new evidence emerged that strengthens a headwind?

RESPONSE FORMAT (JSON only):
{
    "kills": [
        {
            "candidate_id": <integer>,
            "asset_theme": "<name>",
            "reason": "1-2 sentence explanation",
            "connection_type": "DIRECT|THEMATIC|CAUSAL_CHAIN",
            "confidence": "HIGH|MEDIUM|LOW"
        }
    ],
    "reasoning_summary": "2-3 sentences about overall assessment"
}"""


def assess_trade(candidate, current_price, staleness_hours):
    """Call Claude to assess whether a trade recommendation is still valid."""
    api_key = OPENAI_API_KEY or os.environ.get("OPENAI_API_KEY")
    if not api_key:
        logger.debug("No OPENAI_API_KEY, skipping LLM DD")
        return None

    # Build context
    prices_json = candidate.get("prices_at_report") or "{}"
    try:
        prices = json.loads(prices_json)
    except (json.JSONDecodeError, TypeError):
        prices = {}

    primary_ticker = candidate.get("primary_ticker", "?")
    report_price = prices.get(primary_ticker, 0)

    user_prompt = """TRADE RECOMMENDATION TO ASSESS:

Asset: {asset}
Ticker: {ticker}
Direction: {direction}
Confidence: {conf}%
Edge Quality: {edge}
Propagation: {prop}
Action (from report): {action}

HEADLINE: {headline}

MECHANISM: {mechanism}

TRIPWIRE: {tripwire}

EVIDENCE: {evidence}

RISKS: {risks}

CURRENT CONDITIONS:
- Report price: ${report_price}
- Current price: ${current_price}
- Price change: {price_change:+.1f}%
- Staleness: {staleness:.0f} hours since report published
- Market is {market_status}

Should we TRADE, WATCH, or KILL this position?""".format(
        asset=candidate.get("asset_theme", "Unknown"),
        ticker=primary_ticker,
        direction=candidate.get("direction", "?"),
        conf=candidate.get("confidence_pct", 0),
        edge=candidate.get("edge_quality", "?"),
        prop=candidate.get("propagation", "?"),
        action=candidate.get("action", "?"),
        headline=candidate.get("headline") or "N/A",
        mechanism=candidate.get("mechanism") or "N/A",
        tripwire=candidate.get("tripwire") or "N/A",
        evidence=candidate.get("evidence") or "N/A",
        risks=candidate.get("risks") or "N/A",
        report_price=report_price,
        current_price=current_price or 0,
        price_change=((current_price - report_price) / report_price * 100) if report_price and current_price else 0,
        staleness=staleness_hours,
        market_status="open" if _is_market_hours() else "closed"
    )

    return _call_llm(api_key, DD_SYSTEM_PROMPT, user_prompt)


def kill_switch_assessment(active_candidates, new_candidates, new_report_title):
    """Assess whether new report information should kill any active positions."""
    api_key = OPENAI_API_KEY or os.environ.get("OPENAI_API_KEY")
    if not api_key:
        logger.debug("No OPENAI_API_KEY, skipping kill switch")
        return []

    if not active_candidates:
        return []

    parts = []
    parts.append("=== NEW REPORT ===")
    parts.append("Title: {}".format(new_report_title))
    parts.append("New candidates:")

    for nc in new_candidates:
        parts.append(
            "  - {} ({}) {} {}% edge={}".format(
                nc.get("asset_theme", "?"),
                nc.get("primary_ticker", "?"),
                nc.get("direction", "?"),
                nc.get("confidence_pct", 0),
                nc.get("edge_quality", "?")
            )
        )

    parts.append("")
    parts.append("=== ACTIVE POSITIONS (assess each for potential kill) ===")

    for ac in active_candidates:
        parts.append(
            "  ID={}: {} ({}) {} {}%\n"
            "    Thesis: {}\n"
            "    Entry: ${}\n"
            "    State: {}".format(
                ac["id"],
                ac["asset_theme"][:50],
                ac.get("primary_ticker", "?"),
                ac.get("direction", "?"),
                ac.get("confidence_pct", 0),
                (ac.get("headline") or ac.get("mechanism") or "N/A")[:100],
                ac.get("entry_price", "?"),
                ac.get("state", "?")
            )
        )

    user_prompt = "\n".join(parts)
    result = _call_llm(api_key, KILL_SWITCH_SYSTEM_PROMPT, user_prompt)

    if result:
        return result.get("kills", [])
    return []


def apply_llm_kills(kills, report_id):
    """Apply LLM-recommended kills. Only HIGH confidence kills are auto-applied."""
    if not kills:
        return 0

    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    applied = 0

    for kill in kills:
        cid = kill.get("candidate_id")
        confidence = kill.get("confidence", "MEDIUM")
        reason = kill.get("reason", "No reason given")
        conn_type = kill.get("connection_type", "?")

        logger.info("LLM KILL [{}] ({}): candidate {} '{}' - {}".format(
            confidence, conn_type, cid,
            kill.get("asset_theme", "?")[:40], reason[:100]
        ))

        if confidence != "HIGH":
            continue

        row = conn.execute(
            "SELECT id, asset_theme FROM candidates "
            "WHERE id = ? AND is_active = 1 AND state NOT IN ('KILLED', 'EXPIRED')",
            (cid,)
        ).fetchone()

        if not row:
            continue

        kill_reason = "LLM {}: {}".format(conn_type, reason[:200])
        conn.execute("""
            UPDATE candidates
            SET state = 'KILLED', killed_at = ?, kill_reason = ?,
                killed_by = 'llm', state_reason = ?,
                state_changed_at = ?
            WHERE id = ?
        """, (now, kill_reason, kill_reason, now, cid))

        applied += 1
        logger.info("LLM KILL APPLIED: {} '{}'".format(cid, row["asset_theme"][:40]))

    if applied > 0:
        conn.commit()
    conn.close()
    return applied


def _call_llm(api_key, system_prompt, user_prompt):
    """Make an OpenAI API call and parse JSON response."""
    try:
        resp = http_requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": "Bearer {}".format(api_key),
                "Content-Type": "application/json",
            },
            json={
                "model": "gpt-4o-mini",
                "max_tokens": 2048,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ]
            },
            timeout=60
        )
        resp.raise_for_status()
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        return json.loads(content)
    except json.JSONDecodeError as e:
        logger.error("LLM returned invalid JSON: {}".format(e))
        return None
    except http_requests.exceptions.Timeout:
        logger.warning("LLM assessment timed out")
        return None
    except Exception as e:
        logger.error("LLM assessment failed: {}".format(e))
        return None


def _is_market_hours():
    from config import MARKET_OPEN_UTC, MARKET_CLOSE_UTC, MARKET_DAYS
    now = datetime.now(timezone.utc)
    if now.weekday() not in MARKET_DAYS:
        return False
    hour_dec = now.hour + now.minute / 60.0
    return MARKET_OPEN_UTC <= hour_dec < MARKET_CLOSE_UTC
