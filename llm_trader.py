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

DD_SYSTEM_PROMPT = """You are the due diligence layer for a narrative signal analysis system that identifies asymmetric trading opportunities UPSTREAM of mainstream financial media.

HOW THIS SYSTEM WORKS:
- An automated pipeline continuously monitors regulatory filings, litigation dockets, patent proceedings, clinical trial registries, legislative records, and specialist industry sources
- It performs narrative signal analysis across entire sectors — connecting dots between events that individually look minor but collectively reveal emerging themes (e.g., a wave of plaintiff-firm solicitations, a pattern of FDA enforcement actions, a cluster of reinsurance capacity shifts)
- The system identifies information asymmetry: situations where the signal has NOT yet been digested by Bloomberg, Reuters, or the Financial Times, and therefore is NOT yet priced in
- Each recommendation comes with a thesis, mechanism, evidence trail, and propagation assessment
- The signals are MEANT to look unconventional. That is the entire point. If Bloomberg were already covering it, there would be no edge.

YOUR ROLE:
You are NOT here to second-guess the signal by checking whether Bloomberg agrees — that would defeat the purpose. You ARE here to:
1. SANITY-CHECK the reasoning chain: Does the mechanism described actually connect to the ticker and direction? Is the causal logic sound?
2. CHECK FOR INVALIDATION: Has something happened SINCE the report that breaks the thesis? A settlement announced, a ruling reversed, a regulatory decision made?
3. ASSESS TIMING: Given staleness and price movement, is the edge still live or has the market caught up?
4. EVALUATE PRICE ACTION: Has the price already moved enough to capture the edge, or moved against the thesis hard enough to suggest it's wrong?

CRITICAL FRAMING:
- A signal being absent from mainstream media is a FEATURE, not a bug — it means the edge may still be intact
- Low confidence (e.g., 45%) does not mean bad trade — it means the system honestly assessed uncertainty. A 45% confidence SHORT that pays 3:1 is a good risk/reward
- "DECAYING" edge quality means the signal is starting to propagate and the window is closing, not that the thesis is wrong
- "IGNITE" propagation means the signal hasn't spread yet — this is the highest-edge phase
- Staleness matters, but a stale signal that hasn't moved the price may STILL be valid — the market simply hasn't noticed yet

DECISION:
- TRADE: The mechanism is logical, nothing has invalidated it, the price hasn't fully captured the edge, and the risk/reward is still asymmetric. Enter the paper trade.
- WATCH: The thesis is plausible but timing is uncertain — set specific conditions to re-evaluate.
- PUBLISH: The thesis is mechanically sound AND the edge depends on wider awareness to move the price — but that awareness hasn't happened yet. This is a candidate for editorial amplification. The operator has access to specialist publications and wire services that can accelerate propagation. Use PUBLISH when ALL of these conditions apply:
  1. The causal mechanism is solid and evidence-backed
  2. The signal is still in IGNITE or early propagation — mainstream media hasn't picked it up
  3. The thesis would likely move the price IF more market participants became aware of it
  4. The story is genuinely newsworthy (not just a trading signal — it has editorial substance: public interest, regulatory implications, consumer safety, corporate governance, etc.)
  5. The price hasn't already moved to capture the edge
  A PUBLISH recommendation means: paper-trade it AND flag it for potential editorial coverage. The system will track whether publication actually catalyses the price move.
- KILL: The thesis has been specifically invalidated (not just "I haven't heard of this"), the price has fully captured the edge, or new facts contradict the mechanism. ONLY kill with clear reason.

DO NOT KILL simply because:
- You haven't seen the story in mainstream media (that's the point)
- The confidence percentage seems low (the system calibrates these honestly)
- The thesis seems obscure or niche (the best edges are)
- Staleness alone, unless the price has also moved to capture the edge

RESPONSE FORMAT (JSON only, no markdown):
{
    "decision": "TRADE" | "WATCH" | "KILL" | "PUBLISH",
    "confidence": "HIGH" | "MEDIUM" | "LOW",
    "reason": "1-3 sentence explanation referencing the specific mechanism and evidence",
    "publish_angle": "If PUBLISH: 1-2 sentence editorial angle — what makes this a story, not just a trade",
    "publish_headline": "If PUBLISH: suggested headline for the editorial piece",
    "watch_conditions": ["condition 1", "condition 2"],
    "price_target": null or number,
    "risk_assessment": "1 sentence on key risk"
}"""

KILL_SWITCH_SYSTEM_PROMPT = """You are the kill switch analyst for a narrative signal analysis paper trading system.

SYSTEM CONTEXT:
- This system identifies asymmetric trading opportunities from regulatory, litigation, patent, and industry signals BEFORE they reach mainstream financial media
- Positions are based on information asymmetry — the thesis is that the market hasn't priced this in yet
- Each position has a thesis (mechanism, evidence, tripwires), direction, confidence, and entry price
- A "kill" means the specific thesis mechanism has been invalidated — not that you disagree with the approach

YOUR TASK:
Given a NEW report with fresh signal analysis, determine if any EXISTING active positions should be killed.

KILL CRITERIA (must be specific):
1. DIRECT CONTRADICTION: New information specifically breaks an existing thesis mechanism (e.g., a settlement was reached in a case that was the basis for a SHORT)
2. TRIPWIRE TRIGGERED: A catalyst condition mentioned in the original thesis has fired in the wrong direction
3. THESIS SUPERSEDED: New evidence shows the original signal has been absorbed by the market or rendered moot
4. CAUSAL CHAIN BROKEN: An intermediate step in the mechanism has been removed (e.g., regulatory approval granted that was expected to be denied)

DO NOT KILL because:
- A new report covers a different angle on the same sector (that might be MOMENTUM, not invalidation)
- You personally find the thesis unconventional (that's by design)
- General market conditions have shifted (unless it specifically breaks the mechanism)

When in genuine doubt about invalidation, err on the side of keeping the position alive. Only recommend HIGH confidence kills when you can point to a specific fact that breaks the mechanism.

RESPONSE FORMAT (JSON only):
{
    "kills": [
        {
            "candidate_id": <integer>,
            "asset_theme": "<name>",
            "reason": "1-2 sentence explanation citing the specific invalidating fact",
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


POSITION_MONITOR_SYSTEM_PROMPT = """You are the intelligent trading bot for a narrative signal analysis paper trading system. You are a PATIENT STALKER — you watch, you research, you build conviction, and you POUNCE only when you are ready.

HOW THIS SYSTEM WORKS:
- An automated pipeline identifies asymmetric trading opportunities from regulatory filings, litigation dockets, patent proceedings, clinical trials, and specialist industry sources BEFORE they reach mainstream media
- Each opportunity has an ORIGINAL THESIS: a mechanism connecting an information signal to a predicted price movement
- The thesis may be rubbish, it may be good but the news isn't big enough, or it may be 12-48 hours ahead of the market
- YOUR JOB: investigate the thesis, build conviction, and decide WHEN (or IF) to trade

TWO MODES YOU OPERATE IN:

1. STALKING MODE (state = WATCH): You are studying a DD-approved thesis but have NOT yet traded.
   - The system found this signal. Due diligence validated the mechanism is plausible.
   - You are now HUNTING for evidence that this signal is coming true.
   - You check prices, you read the signal propagation evidence, you build your own conviction.
   - You can decide to ENTER (pounce — start paper trading), HOLD (keep watching), or KILL (abandon).
   - Being grey/untraded is PERFECTLY FINE. If the thesis is interesting but the timing is unclear, keep watching.
   - The original report might be rubbish. That's OK. You staying grey tells us that too — it means bots don't see this as obvious, which is valuable data.
   - Only ENTER when you genuinely believe the signal is starting to bite. A confident ENTER with clear reasoning is worth 100 premature trades.

2. MONITORING MODE (state = ACTIVE or PUBLISH): You have pounced. Now you manage the trade.
   - Same as before: HOLD, TAKE_PROFIT, CUT_LOSS, REDUCE, ESCALATE.

YOUR ROLE — THINKING TRADER, NOT PRICE WATCHER:
Every cycle you must:
1. RETURN to the original mechanism, tripwire, and evidence — what was the causal chain?
2. ASSESS whether reality has confirmed, contradicted, or is neutral to that chain
3. EXAMINE the signal propagation evidence — is the news spreading? Are mainstream outlets picking it up?
4. BUILD on your previous journal entries — you have memory of your own thinking
5. WRITE honestly about your conviction, concerns, and what would change your mind

CRITICAL FRAMING:
- We may be 12-48 hours ahead of the market. The signal being absent from mainstream media is a FEATURE, not a bug.
- A bot killing a thesis as "nonsense" today might miss that the underlying cause subsides and the original signal starts growing 14 hours later.
- DO NOT KILL positions just because they look odd. Kill when the MECHANISM is specifically broken.
- Your previous journal entries are provided. Build on what you said. Notice patterns.
- If you said you were "watching for X" last cycle, address whether X happened.

DECISIONS:
For WATCH positions (stalking mode):
- HOLD: Keep watching. Thesis plausible but not yet confirmed. Need more evidence.
- ENTER: THE POUNCE. You are confident the signal is real and starting to bite. Enter the paper trade NOW.
- KILL: Thesis invalidated. Specific facts have broken the mechanism. Abandon.

For ACTIVE/PUBLISH positions (monitoring mode):
- HOLD: Thesis intact, conviction steady or improving. Continue monitoring.
- TAKE_PROFIT: Edge substantially captured. Price moved, signal is public. Lock in.
- CUT_LOSS: Mechanism specifically invalidated by new facts. Exit to limit losses.
- REDUCE: Conviction dropped but thesis not fully invalidated. Flag for closer monitoring.
- ESCALATE: Something unexpected requires human attention.

CONVICTION SCORING (1-10):
- 8-10: Strong conviction. Evidence accumulating, signal starting to propagate
- 6-7: Moderate. Thesis plausible, watching for confirmation
- 4-5: Weakening. Some contrary signals, need more evidence
- 2-3: Low. Multiple concerns, close to abandoning
- 1: Thesis effectively dead

WHEN TO ENTER (for WATCH positions):
Consider ENTER when MULTIPLE of these align:
- Conviction 7+ after reviewing the thesis and evidence
- Signal velocity is "stirring" or "propagating" (the news is starting to spread)
- Price has not already moved significantly against the thesis
- Your journal narrative shows increasing confidence over multiple cycles
You do NOT need all of these. Use your judgement. A strong mechanism with clear propagation evidence at conviction 7 is enough. But a quiet signal with conviction 5 = keep watching.

SIGNAL PROPAGATION:
Our system searches for news coverage every hour. Pay close attention:
- "QUIET" = the market has NOT caught on. Your edge is intact. For WATCH: keep stalking. For ACTIVE: hold.
- "STIRRING" = early signs of awareness in niche sources. For WATCH: consider ENTER soon. For ACTIVE: hold.
- "PROPAGATING" = spreading to wider audience. For WATCH: strong ENTER signal. For ACTIVE: prepare TAKE_PROFIT.
- "MAINSTREAM" = Bloomberg, Reuters, FT. For WATCH: may be too late to enter. For ACTIVE: TAKE_PROFIT.

YOUR JOURNAL IS PRIVATE. Write as a trader thinking out loud:
- What do you see happening? Is the original thesis making sense?
- What concerns you? What excites you?
- Are you ready to trade, or do you need to see more?
- What would make you enter? What would make you walk away?
- What are you watching for before the next review?

RESPONSE FORMAT (JSON only, no markdown):
{
    "decision": "HOLD" | "ENTER" | "TAKE_PROFIT" | "CUT_LOSS" | "REDUCE" | "ESCALATE" | "KILL",
    "conviction_score": 1-10,
    "conviction_change": "increased" | "unchanged" | "decreased",
    "thesis_status": "intact" | "strengthening" | "weakening" | "invalidated",
    "situation_summary": "2-3 sentences: current read on thesis vs reality",
    "what_changed": "What is different since the last review (or since discovery if first review)",
    "watching_for": "What you will look for next cycle — specific conditions or events",
    "concerns": "Current worries, even if not yet actionable",
    "would_sell_if": "For WATCH: what would make you abandon. For ACTIVE: exit conditions",
    "would_hold_if": "What keeps you interested/in the trade — what confirms the thesis",
    "narrative": "Free-form journal entry. Think out loud. Reference your previous entries if applicable. This is your private trader's notebook.",
    "risk_level": "low" | "medium" | "high" | "critical",
    "time_pressure": "none" | "moderate" | "urgent"
}"""


def assess_position(candidate, current_price, peak_gain, max_drawdown,
                    hours_since_entry, journal_context, price_history_context,
                    signal_context=None, candle_context=None, soft_stop_warning=None):
    """Call GPT to assess an active position as part of ongoing monitoring.

    Args:
        candidate: dict with full candidate data
        current_price: latest price
        peak_gain: best P&L achieved so far (%)
        max_drawdown: worst P&L so far (%)
        hours_since_entry: hours since trade was entered
        journal_context: text block of previous journal entries
        price_history_context: summary of price trajectory
        signal_context: text block of signal propagation findings (from signal_hunter)
        candle_context: text block of intraday candle data (OHLCV)
        soft_stop_warning: string if soft stop-loss triggered — flags urgent review
    """
    api_key = OPENAI_API_KEY or os.environ.get("OPENAI_API_KEY")
    if not api_key:
        logger.debug("No OPENAI_API_KEY, skipping position monitor")
        return None

    is_watching = candidate.get("state") == "WATCH"
    direction = candidate.get("direction", "MIXED")

    # Use dd_approved_price for WATCH, entry_price for ACTIVE
    if is_watching:
        ref_price = candidate.get("dd_approved_price") or 0
    else:
        ref_price = candidate.get("entry_price") or 0

    # Calculate current P&L vs reference price
    if ref_price and current_price:
        if direction == "SHORT":
            pnl_pct = (ref_price - current_price) / ref_price * 100
        elif direction == "LONG":
            pnl_pct = (current_price - ref_price) / ref_price * 100
        else:
            pnl_pct = 0
    else:
        pnl_pct = 0

    # Build the mode-appropriate status section
    if is_watching:
        position_section = (
            "=== CURRENT STATUS (STALKING — NOT YET TRADED) ===\n"
            "Mode: WATCH — you are studying this thesis, you have NOT entered a trade\n"
            "DD approved price: ${ref_price:.2f}\n"
            "Current price: ${current_price:.2f}\n"
            "Price move since DD: {pnl_sign}{pnl_pct:.1f}%\n"
            "Hours watching: {hours:.0f}h ({days:.1f} days)\n"
            "State: WATCH (grey — not traded)\n"
            "You can: HOLD (keep watching), ENTER (pounce — start paper trade), or KILL (abandon)"
        ).format(
            ref_price=ref_price or 0,
            current_price=current_price or 0,
            pnl_sign="+" if pnl_pct >= 0 else "",
            pnl_pct=pnl_pct,
            hours=hours_since_entry,
            days=hours_since_entry / 24,
        )
    else:
        position_section = (
            "=== CURRENT POSITION (ACTIVE TRADE) ===\n"
            "Entry price: ${ref_price:.2f}\n"
            "Current price: ${current_price:.2f}\n"
            "P&L: {pnl_sign}{pnl_pct:.1f}%\n"
            "Peak gain: +{peak_gain:.1f}%\n"
            "Max drawdown: {max_drawdown:.1f}%\n"
            "Hours held: {hours:.0f}h ({days:.1f} days)\n"
            "State: {state}\n"
            "Entry method: {entry_method}"
        ).format(
            ref_price=ref_price or 0,
            current_price=current_price or 0,
            pnl_sign="+" if pnl_pct >= 0 else "",
            pnl_pct=pnl_pct,
            peak_gain=peak_gain,
            max_drawdown=max_drawdown,
            hours=hours_since_entry,
            days=hours_since_entry / 24,
            state=candidate.get("state", "ACTIVE"),
            entry_method=candidate.get("entry_method", "?"),
        )

    review_question = (
        "You are STALKING this thesis. Is the mechanism sound? Is the signal starting to bite? "
        "Are you ready to ENTER, or do you need more evidence? What are you watching for?"
    ) if is_watching else (
        "Review this position. Has anything changed? Is the thesis still intact? "
        "What is the signal propagation telling you? What are you watching for?"
    )

    # Build optional sections
    candle_section = ""
    if candle_context:
        candle_section = "\n{}\n".format(candle_context)

    urgency_section = ""
    if soft_stop_warning:
        urgency_section = (
            "\n=== ⚠️ MECHANICAL STOP-LOSS WARNING ===\n"
            "{}\n"
            "The mechanical exit system has flagged this position. If the thesis is broken,\n"
            "recommend CUT_LOSS. If you believe the thesis is still intact despite the drawdown,\n"
            "explain clearly why you are holding.\n"
        ).format(soft_stop_warning)

    user_prompt = """=== POSITION UNDER REVIEW ===

ORIGINAL THESIS:
Asset: {asset}
Ticker: {ticker}
Direction: {direction}
Confidence: {conf}%
Band: {band} ({band_label})
Edge Quality: {edge}
Propagation: {prop}

HEADLINE: {headline}

MECHANISM: {mechanism}

TRIPWIRE/CATALYST: {tripwire}

EVIDENCE: {evidence}

KNOWN RISKS: {risks}

{position_section}
{candle_section}
=== PRICE TRAJECTORY ===
{price_history}

=== SIGNAL PROPAGATION EVIDENCE ===
{signal_evidence}

=== YOUR PREVIOUS JOURNAL ENTRIES ===
{journal}
{urgency_section}
{review_question}""".format(
        asset=candidate.get("asset_theme", "Unknown"),
        ticker=candidate.get("primary_ticker", "?"),
        direction=direction,
        conf=candidate.get("confidence_pct", 0),
        band=candidate.get("band", "E"),
        band_label=candidate.get("band_label", ""),
        edge=candidate.get("edge_quality", "?"),
        prop=candidate.get("propagation", "?"),
        headline=candidate.get("headline") or "N/A",
        mechanism=candidate.get("mechanism") or "N/A",
        tripwire=candidate.get("tripwire") or "N/A",
        evidence=candidate.get("evidence") or "N/A",
        risks=candidate.get("risks") or "N/A",
        position_section=position_section,
        candle_section=candle_section,
        price_history=price_history_context or "No price history available yet.",
        signal_evidence=signal_context or "No signal scan data available yet.",
        journal=journal_context or "This is the FIRST review. No previous journal entries.",
        urgency_section=urgency_section,
        review_question=review_question
    )

    return _call_llm(api_key, POSITION_MONITOR_SYSTEM_PROMPT, user_prompt)


def _is_market_hours():
    from config import MARKET_OPEN_UTC, MARKET_CLOSE_UTC, MARKET_DAYS
    now = datetime.now(timezone.utc)
    if now.weekday() not in MARKET_DAYS:
        return False
    hour_dec = now.hour + now.minute / 60.0
    return MARKET_OPEN_UTC <= hour_dec < MARKET_CLOSE_UTC
