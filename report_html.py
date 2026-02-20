"""
Hedge Fund Edge Tracker - HTML Report Generator
Noah Pink design system with three-section trading sheet:
1. Active Positions  2. Pipeline (Qualified Candidates)  3. Research Lab
Plus backtest performance card, confidence clusters, and learning dashboard.
"""
import os
import logging
from datetime import datetime, timezone

from analytics import generate_analytics
from config import REPORTS_DIR, BANDS, KILL_DISPLAY_HOURS, ALPHA_FORMULA_DESC

logger = logging.getLogger("hedgefund.report")


def _band_color(band):
    return BANDS.get(band, BANDS["E"])["color"]

def _band_bg(band):
    return BANDS.get(band, BANDS["E"])["bg"]

def _state_color(state):
    return {
        "ACTIVE": "#2563eb", "WATCH": "#6b7280", "KILLED": "#5b21b6",
        "PUBLISH": "#2563eb", "PENDING": "#9ea2b0", "EXPIRED": "#c4c8d4",
    }.get(state, "#9ea2b0")

def _state_bg(state):
    return {
        "ACTIVE": "#dbeafe", "WATCH": "#f3f4f6", "KILLED": "#ede9fe",
        "PUBLISH": "#dbeafe", "PENDING": "#f1f5f9", "EXPIRED": "#f8f9fa",
    }.get(state, "#f1f5f9")

def _direction_color(d):
    return {"SHORT": "#cc0000", "LONG": "#16a34a", "MIXED": "#92400e"}.get(d, "#9ea2b0")

def _direction_bg(d):
    return {"SHORT": "#fef2f2", "LONG": "#f0fdf4", "MIXED": "#fef3c7"}.get(d, "#f1f5f9")

def _status_dot(status):
    c = {"green": "#16a34a", "orange": "#f59e0b", "red": "#cc0000",
         "purple": "#7c3aed", "killed": "#5b21b6", "grey": "#9ea2b0"}.get(status, "#9ea2b0")
    return '<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:{}"></span>'.format(c)

def _status_text_color(status):
    return {"green": "#16a34a", "orange": "#f59e0b", "red": "#cc0000",
            "purple": "#7c3aed", "killed": "#5b21b6", "grey": "#9ea2b0"}.get(status, "#9ea2b0")


def _build_timeline_cells(m):
    """Build hourly price timeline as colored cells."""
    timeline = m.get("timeline", [])
    if not timeline:
        return '<td class="td-timeline"><span class="tl-empty">awaiting data</span></td>'

    killed = m.get("state") == "KILLED"
    kill_inserted = False
    watched = m.get("state") == "WATCH"
    cells = []

    for pt in timeline:
        is_killed_pt = pt.get("killed", False)
        is_watch_pt = pt.get("watched", False)

        # Kill marker
        if killed and is_killed_pt and not kill_inserted:
            cells.append('<span class="kill-marker" title="Killed at {:.0f}h">K</span>'.format(
                pt.get("hours", 0)
            ))
            kill_inserted = True

        # Color
        if is_killed_pt:
            sc = "#5b21b6"
        elif is_watch_pt and watched:
            sc = "#7c3aed"
        else:
            sc = _status_text_color(pt.get("status", "grey"))

        price = pt.get("price", 0)
        pnl = pt.get("pnl_pct")
        hours = pt.get("hours", 0)

        if pnl is not None:
            sign = "+" if pnl > 0 else ""
            pnl_str = "{}{:.1f}%".format(sign, pnl)
        else:
            pnl_str = ""

        time_str = pt.get("time", "")
        cells.append(
            '<span class="tl-point" style="color:{sc}" title="{t} ({h:.0f}h): ${p:.2f} {pnl}">'
            '<sup class="tl-time">{ts}</sup>${p:.2f}<sub>{pnl}</sub></span>'.format(
                sc=sc, t=time_str, h=hours, p=price, pnl=pnl_str,
                ts=time_str[-5:] if len(time_str) > 5 else time_str
            )
        )

    html = '<span class="tl-arrow">&rarr;</span>'.join(cells)
    return '<td class="td-timeline">{}</td>'.format(html)


# ---------------------------------------------------------------------------
# Section builders for the three-part trading sheet
# ---------------------------------------------------------------------------

def _classify_candidates(candidates):
    """Split candidates into three sections: active, alpha pipeline, research.

    Active: state in (ACTIVE, PUBLISH)
    Alpha Pipeline: state == WATCH AND alpha == True (LONG + Band A/B)
    Research Lab: everything else that is visible (non-alpha WATCH, kills, pending)
    """
    now = datetime.now(timezone.utc)

    active_list = []
    pipeline_list = []
    research_list = []
    hidden_kills = 0

    for m in candidates:
        # Skip deactivated positions (no ticker)
        if not m.get("is_active", 1):
            continue

        state = m["state"]

        # Filter old kills
        if state == "KILLED" and m.get("killed_at"):
            try:
                kill_time = datetime.fromisoformat(m["killed_at"])
                if kill_time.tzinfo is None:
                    kill_time = kill_time.replace(tzinfo=timezone.utc)
                hours_since_kill = (now - kill_time).total_seconds() / 3600
                if hours_since_kill > KILL_DISPLAY_HOURS:
                    hidden_kills += 1
                    continue
            except (ValueError, TypeError):
                pass

        if state in ("ACTIVE", "PUBLISH"):
            active_list.append(m)
        elif state == "WATCH" and m.get("alpha"):
            pipeline_list.append(m)
        else:
            research_list.append(m)

    # Sort each section
    active_list.sort(key=lambda m: -(m.get("confidence_pct") or 0))
    pipeline_list.sort(key=lambda m: -(m.get("confidence_pct") or 0))
    state_order = {"WATCH": 0, "PENDING": 1, "KILLED": 2, "EXPIRED": 3}
    research_list.sort(key=lambda m: (
        state_order.get(m["state"], 9), -(m.get("confidence_pct") or 0)
    ))

    return active_list, pipeline_list, research_list, hidden_kills


def _build_active_section(active_list):
    """Build HTML for Active Positions section."""
    count = len(active_list)
    header = (
        '<div class="section-bar section-bar-active">'
        '<span class="section-bar-icon">&#9679;</span> '
        'Active Positions'
        '<span class="section-bar-count">{} position{}</span>'
        '</div>'
    ).format(count, "s" if count != 1 else "")

    if not active_list:
        return header + '<div class="section-empty">No active positions. The trader is waiting for the right entry.</div>'

    rows = []
    for m in active_list:
        rows.append(_build_active_row(m))

    table = """<div class="table-scroll">
<table class="trading-table">
<thead><tr>
    <th>Ticker</th><th>Band</th><th>Asset / Thesis</th><th>Dir</th>
    <th>Conf</th><th>Entry</th><th>Current</th><th>Trade P&amp;L</th>
    <th>Report P&amp;L</th><th>Timeline</th>
</tr></thead>
<tbody>{rows}</tbody>
</table></div>""".format(rows="\n".join(rows))

    return header + table


def _build_active_row(m):
    """Build a single row for an active/publish position."""
    ticker_raw = m.get("primary_ticker", "?")
    band = m.get("band", "E")
    bc = _band_color(band)
    asset = (m.get("asset_theme") or "Unknown")[:45]
    thesis = (m.get("headline") or m.get("mechanism") or "")[:70]
    direction = m.get("direction", "MIXED")
    dc = _direction_color(direction)
    db = _direction_bg(direction)
    conf = m.get("confidence_pct", 0)

    entry = m.get("entry_price")
    current = m.get("current_price")
    pnl = m.get("current_pnl")
    report_pnl = m.get("report_pnl")

    entry_str = "${:.2f}".format(entry) if entry else "---"
    current_str = "${:.2f}".format(current) if current else "---"

    # Trade P&L
    if pnl is not None:
        tpnl_sign = "+" if pnl >= 0 else ""
        tpnl_color = "#16a34a" if pnl >= 0 else "#cc0000"
        trade_pnl_str = '<span style="color:{};font-weight:700;font-size:1rem">{}{:.1f}%</span>'.format(
            tpnl_color, tpnl_sign, pnl)
    else:
        trade_pnl_str = '<span style="color:var(--grey-400)">---</span>'

    # Report P&L
    if report_pnl is not None:
        rpnl_sign = "+" if report_pnl >= 0 else ""
        report_pnl_str = '<span style="color:#7c3aed;font-weight:700">{}{:.1f}%</span>'.format(
            rpnl_sign, report_pnl)
    else:
        report_pnl_str = '<span style="color:var(--grey-400)">---</span>'

    # Thesis badge
    thesis_st = m.get("latest_thesis_status", "")
    thesis_colors = {
        "intact": ("#166534", "#dcfce7"),
        "strengthening": ("#065f46", "#d1fae5"),
        "weakening": ("#92400e", "#fef3c7"),
        "invalidated": ("#991b1b", "#fef2f2"),
    }
    tc, tbg = thesis_colors.get(thesis_st, ("", ""))
    thesis_micro = ""
    if thesis_st and tc:
        thesis_micro = ' <span class="thesis-badge" style="color:{};background:{}">{}</span>'.format(
            tc, tbg, thesis_st)

    # Notes icon
    cid = m.get("id", 0)
    has_notes = bool(m.get("latest_conviction") or m.get("latest_watching_for")
                     or m.get("latest_narrative_entries") or m.get("dd_entries"))
    if has_notes:
        notes_icon = '<a href="positions/position_{}.html" class="notes-link" title="View trader notes">&#128203;</a>'.format(cid)
    else:
        notes_icon = ''

    timeline_html = _build_timeline_cells(m)

    return """<tr class="active-row">
    <td class="td-ticker-active"><span class="ticker-name">{ticker}</span><span class="trade-badge">B</span>{thesis_micro} {notes_icon}</td>
    <td class="td-band" style="color:{bc}">{band}</td>
    <td class="td-asset"><div class="name">{asset}</div><div class="thesis">{thesis}</div></td>
    <td><span class="td-dir" style="color:{dc};background:{db}">{direction}</span></td>
    <td style="text-align:center;font-weight:700">{conf:.0f}%</td>
    <td class="td-price">{entry_str}</td>
    <td class="td-price">{current_str}</td>
    <td class="td-pnl">{trade_pnl_str}</td>
    <td class="td-pnl">{report_pnl_str}</td>
    {timeline_html}
</tr>""".format(
        ticker=ticker_raw, thesis_micro=thesis_micro, notes_icon=notes_icon,
        bc=bc, band=band, asset=asset, thesis=thesis,
        dc=dc, db=db, direction=direction, conf=conf,
        entry_str=entry_str, current_str=current_str,
        trade_pnl_str=trade_pnl_str, report_pnl_str=report_pnl_str,
        timeline_html=timeline_html)


def _build_pipeline_section(pipeline_list):
    """Build HTML for Alpha Pipeline section."""
    count = len(pipeline_list)
    header = (
        '<div class="section-bar section-bar-pipeline">'
        '<span class="section-bar-icon">&#9670;</span> '
        'Alpha Pipeline &mdash; Trading Formula Candidates'
        '<span class="section-bar-count">{} alpha candidate{} awaiting entry</span>'
        '</div>'
    ).format(count, "s" if count != 1 else "")

    if not pipeline_list:
        return header + '<div class="section-empty">No alpha candidates in the pipeline. Scanning for LONG + Band A/B signals.</div>'

    rows = []
    for m in pipeline_list:
        rows.append(_build_pipeline_row(m))

    table = """<div class="table-scroll">
<table class="trading-table pipeline-table">
<thead><tr>
    <th>Ticker</th><th>Band</th><th>Asset / Thesis</th><th>Dir</th>
    <th>Conf</th><th>Report Price</th><th>Current</th><th>Report P&amp;L</th>
    <th>Signal</th><th>Thesis</th>
</tr></thead>
<tbody>{rows}</tbody>
</table></div>""".format(rows="\n".join(rows))

    return header + table


def _build_pipeline_row(m):
    """Build a single row for a pipeline (qualified WATCH) candidate."""
    ticker_raw = m.get("primary_ticker", "?")
    band = m.get("band", "E")
    bc = _band_color(band)
    asset = (m.get("asset_theme") or "Unknown")[:45]
    thesis = (m.get("headline") or m.get("mechanism") or "")[:70]
    direction = m.get("direction", "MIXED")
    dc = _direction_color(direction)
    db = _direction_bg(direction)
    conf = m.get("confidence_pct", 0)

    dd_price = m.get("dd_approved_price")
    report_price = m.get("report_price", 0)
    ref_price = dd_price or report_price
    current = m.get("current_price")
    report_pnl = m.get("report_pnl")

    ref_str = "${:.2f}".format(ref_price) if ref_price else "---"
    current_str = "${:.2f}".format(current) if current else "---"

    # Report P&L
    if report_pnl is not None:
        rpnl_sign = "+" if report_pnl >= 0 else ""
        rpnl_color = "#7c3aed"
        report_pnl_str = '<span style="color:{};font-weight:700">{}{:.1f}%</span>'.format(
            rpnl_color, rpnl_sign, report_pnl)
    else:
        report_pnl_str = '<span style="color:var(--grey-400)">---</span>'

    # Signal velocity
    sig_velocity = m.get("signal_velocity", "quiet")
    sig_hits = m.get("signal_hits_24h", 0)
    sig_icons = {"quiet": "&#128263;", "stirring": "&#128264;",
                 "propagating": "&#128266;", "mainstream": "&#128680;"}
    sig_icon = sig_icons.get(sig_velocity, "")
    signal_str = '<span style="font-size:0.78rem">{} {}</span>'.format(sig_icon, sig_velocity)

    # Thesis status
    thesis_st = m.get("latest_thesis_status", "")
    thesis_colors = {
        "intact": ("#166534", "#dcfce7"),
        "strengthening": ("#065f46", "#d1fae5"),
        "weakening": ("#92400e", "#fef3c7"),
        "invalidated": ("#991b1b", "#fef2f2"),
    }
    tc, tbg = thesis_colors.get(thesis_st, ("#73788a", "#f1f5f9"))
    if thesis_st:
        thesis_badge = '<span class="thesis-badge" style="color:{};background:{}">{}</span>'.format(
            tc, tbg, thesis_st)
    else:
        thesis_badge = '<span style="color:var(--grey-400);font-size:0.72rem">pending</span>'

    # Notes
    cid = m.get("id", 0)
    has_notes = bool(m.get("latest_conviction") or m.get("latest_watching_for")
                     or m.get("latest_narrative_entries") or m.get("dd_entries"))
    if has_notes:
        notes_icon = '<a href="positions/position_{}.html" class="notes-link" title="View trader notes">&#128203;</a>'.format(cid)
    else:
        notes_icon = ''

    return """<tr class="pipeline-row">
    <td class="td-ticker-pipeline"><span class="ticker-name">{ticker}</span> {notes_icon}</td>
    <td class="td-band" style="color:{bc}">{band}</td>
    <td class="td-asset"><div class="name">{asset}</div><div class="thesis">{thesis}</div></td>
    <td><span class="td-dir" style="color:{dc};background:{db}">{direction}</span></td>
    <td style="text-align:center;font-weight:700">{conf:.0f}%</td>
    <td class="td-price">{ref_str}</td>
    <td class="td-price">{current_str}</td>
    <td class="td-pnl">{report_pnl_str}</td>
    <td>{signal_str}</td>
    <td>{thesis_badge}</td>
</tr>""".format(
        ticker=ticker_raw, notes_icon=notes_icon,
        bc=bc, band=band, asset=asset, thesis=thesis,
        dc=dc, db=db, direction=direction, conf=conf,
        ref_str=ref_str, current_str=current_str,
        report_pnl_str=report_pnl_str,
        signal_str=signal_str, thesis_badge=thesis_badge)


def _build_research_section(research_list, hidden_kills):
    """Build HTML for Research Lab -- Experimental & Dismissed section."""
    count = len(research_list)
    killed_in_list = len([m for m in research_list if m["state"] == "KILLED"])
    watch_in_list = count - killed_in_list

    header = (
        '<div class="section-bar section-bar-research">'
        '<span class="section-bar-icon">&#9881;</span> '
        'Research Lab &mdash; Experimental &amp; Dismissed'
        '<span class="section-bar-count">{killed} killed, {watch} non-alpha watch</span>'
        '</div>'
    ).format(killed=killed_in_list, watch=watch_in_list)

    if not research_list and hidden_kills == 0:
        return header + '<div class="section-empty">No dismissed positions yet.</div>'

    rows = []
    for m in research_list:
        rows.append(_build_research_row(m))

    hidden_note = ""
    if hidden_kills > 0:
        hidden_note = (
            '<div class="research-hidden-note">'
            '{} killed position{} older than {}h removed from view '
            '&mdash; still counted in learning analytics</div>'
        ).format(hidden_kills, "s" if hidden_kills != 1 else "", int(KILL_DISPLAY_HOURS))

    table = """<div class="table-scroll">
<table class="trading-table research-table">
<thead><tr>
    <th>Ticker</th><th>Band</th><th>Asset</th><th>Dir</th>
    <th>Conf</th><th>Report P&amp;L</th><th>State</th><th>Reason</th>
</tr></thead>
<tbody>{rows}</tbody>
</table></div>{hidden_note}""".format(rows="\n".join(rows), hidden_note=hidden_note)

    return header + table


def _build_research_row(m):
    """Build a single row for the research lab (killed + non-qualifying watch)."""
    ticker_raw = m.get("primary_ticker", "?")
    band = m.get("band", "E")
    bc = _band_color(band)
    asset = (m.get("asset_theme") or "Unknown")[:35]
    direction = m.get("direction", "MIXED")
    dc = _direction_color(direction)
    db = _direction_bg(direction)
    conf = m.get("confidence_pct", 0)
    report_pnl = m.get("report_pnl")

    if report_pnl is not None:
        rpnl_sign = "+" if report_pnl >= 0 else ""
        report_pnl_str = '<span style="color:#7c3aed">{}{:.1f}%</span>'.format(rpnl_sign, report_pnl)
    else:
        report_pnl_str = '---'

    state = m["state"]
    sc = _state_color(state)
    sb = _state_bg(state)

    reason = m.get("state_reason") or m.get("kill_reason") or ""
    if not reason and state == "WATCH":
        # Explain why it is not in the alpha group
        if m.get("direction") != "LONG":
            reason = "Non-LONG direction (not alpha)"
        elif m.get("band") not in ("A", "B"):
            reason = "Band {} (below alpha threshold)".format(m.get("band", "?"))
        else:
            reason = "Does not meet alpha criteria"
    reason = reason[:80]

    row_class = "killed-row" if state == "KILLED" else "research-watch-row"

    # Notes
    cid = m.get("id", 0)
    has_notes = bool(m.get("latest_conviction") or m.get("latest_watching_for")
                     or m.get("latest_narrative_entries") or m.get("dd_entries"))
    if has_notes:
        notes_icon = ' <a href="positions/position_{}.html" class="notes-link" title="View notes">&#128203;</a>'.format(cid)
    else:
        notes_icon = ''

    return """<tr class="{row_class}">
    <td class="td-ticker-research">{ticker}{notes_icon}</td>
    <td class="td-band" style="color:{bc}">{band}</td>
    <td class="td-asset-compact">{asset}</td>
    <td><span class="td-dir" style="color:{dc};background:{db}">{direction}</span></td>
    <td style="text-align:center">{conf:.0f}%</td>
    <td class="td-pnl">{report_pnl_str}</td>
    <td><span class="td-state" style="color:{sc};background:{sb}">{state}</span></td>
    <td class="td-reason">{reason}</td>
</tr>""".format(
        row_class=row_class, ticker=ticker_raw, notes_icon=notes_icon,
        bc=bc, band=band, asset=asset,
        dc=dc, db=db, direction=direction, conf=conf,
        report_pnl_str=report_pnl_str,
        sc=sc, sb=sb, state=state, reason=reason)


# ---------------------------------------------------------------------------
# Backtest Performance Card
# ---------------------------------------------------------------------------

def _build_backtest_card(s):
    """Build the Alpha Group performance card — the headline card."""
    alpha_pnl = s.get("alpha_total_pnl", 0)
    alpha_avg = s.get("alpha_avg_pnl", 0)
    alpha_color = "#4ade80" if alpha_pnl >= 0 else "#f87171"
    alpha_sign = "+" if alpha_pnl >= 0 else ""
    avg_color = "#4ade80" if alpha_avg >= 0 else "#f87171"
    avg_sign = "+" if alpha_avg >= 0 else ""

    # Research group comparison
    res_pnl = s.get("research_total_pnl", 0)
    res_wr = s.get("research_win_rate", 0)
    res_measured = s.get("research_measured", 0)
    res_sign = "+" if res_pnl >= 0 else ""

    return """<div class="backtest-card">
    <div class="backtest-header">
        <span class="backtest-icon">&#9733;</span>
        Alpha Group Performance
    </div>
    <div class="backtest-stats">
        <div class="backtest-stat">
            <div class="backtest-num">{alpha_measured}</div>
            <div class="backtest-label">Alpha Signals</div>
        </div>
        <div class="backtest-stat">
            <div class="backtest-num">{alpha_wr:.0f}%</div>
            <div class="backtest-label">Win Rate</div>
        </div>
        <div class="backtest-stat">
            <div class="backtest-num" style="color:{alpha_color}">{alpha_sign}{alpha_pnl:.0f}%</div>
            <div class="backtest-label">Total P&amp;L</div>
        </div>
        <div class="backtest-stat">
            <div class="backtest-num" style="color:{avg_color}">{avg_sign}{alpha_avg:.1f}%</div>
            <div class="backtest-label">Avg per Signal</div>
        </div>
        <div class="backtest-stat">
            <div class="backtest-num" style="color:#2dd4bf">{alpha_pf:.2f}&times;</div>
            <div class="backtest-label">Profit Factor</div>
        </div>
    </div>
    <div class="backtest-rules">
        <span class="rules-label">Formula:</span>
        {formula} &middot; Report P&amp;L from signal price
    </div>
    <div style="margin-top:0.8rem;padding-top:0.8rem;border-top:1px solid rgba(255,255,255,0.08);display:flex;gap:2rem;flex-wrap:wrap;font-size:0.78rem">
        <div style="color:rgba(255,255,255,0.5)">
            <span style="font-weight:700;color:rgba(255,255,255,0.7)">Research:</span>
            {res_measured} signals &middot; {res_wr:.0f}% win rate &middot; {res_sign}{res_pnl:.0f}% total P&amp;L
        </div>
    </div>
</div>""".format(
        alpha_measured=s.get("alpha_measured", 0),
        alpha_wr=s.get("alpha_win_rate", 0),
        alpha_pnl=alpha_pnl,
        alpha_color=alpha_color,
        alpha_sign=alpha_sign,
        alpha_avg=alpha_avg,
        avg_color=avg_color,
        avg_sign=avg_sign,
        alpha_pf=s.get("alpha_profit_factor", 0),
        formula=ALPHA_FORMULA_DESC,
        res_measured=res_measured,
        res_wr=res_wr,
        res_pnl=res_pnl,
        res_sign=res_sign)


# ---------------------------------------------------------------------------
# Exit Timing Analysis
# ---------------------------------------------------------------------------

def _build_exit_timing_card(data):
    """Build exit timing analysis card similar to PolyHunter."""
    ta = data.get("timing_analysis", {})
    if not ta:
        return ""

    # Collect average P&L across all bands for each window
    windows = ["0-6h", "6-12h", "12-24h", "24-48h", "48h+"]
    window_labels = ["6H", "12H", "24H", "48H", "96H+"]
    window_totals = {}
    window_counts = {}

    for w in windows:
        window_totals[w] = 0
        window_counts[w] = 0

    for band_key in ["A", "B", "C", "D", "E"]:
        bt = ta.get(band_key, {})
        for w in windows:
            wd = bt.get("windows", {}).get(w, {})
            dp = wd.get("data_points", 0)
            if dp > 0:
                window_totals[w] += wd.get("avg_pnl", 0) * dp
                window_counts[w] += dp

    # Find best window
    best_window = None
    best_avg = -9999
    for w in windows:
        if window_counts[w] > 0:
            avg = window_totals[w] / window_counts[w]
            if avg > best_avg:
                best_avg = avg
                best_window = w

    # Build header cells
    header_cells = []
    for i, w in enumerate(windows):
        label = window_labels[i]
        cls = ' class="timing-best"' if w == best_window else ''
        header_cells.append("<th{}>{}</th>".format(cls, label))

    # Build band rows
    band_rows = []
    for band_key in ["A", "B", "C", "D", "E"]:
        bt = ta.get(band_key, {})
        if not bt.get("windows"):
            continue
        bc = _band_color(band_key)
        cells = '<td style="color:{};font-weight:700">Band {}</td>'.format(bc, band_key)
        for w in windows:
            wd = bt.get("windows", {}).get(w, {})
            avg_pnl = wd.get("avg_pnl", 0)
            dp = wd.get("data_points", 0)
            if dp > 0:
                pnl_sign = "+" if avg_pnl >= 0 else ""
                pnl_color = "#16a34a" if avg_pnl >= 0 else "#cc0000"
                is_best = (w == best_window)
                cls = ' class="timing-best"' if is_best else ''
                cells += '<td{}><span style="color:{};font-weight:700">{}{:.1f}%</span><br><span style="font-size:0.65rem;color:var(--grey-400)">n={}</span></td>'.format(
                    cls, pnl_color, pnl_sign, avg_pnl, dp)
            else:
                cells += '<td style="color:var(--grey-400)">---</td>'
        band_rows.append("<tr>{}</tr>".format(cells))

    # Aggregate row
    agg_cells = '<td style="font-weight:700">All Bands</td>'
    for w in windows:
        if window_counts[w] > 0:
            avg = window_totals[w] / window_counts[w]
            pnl_sign = "+" if avg >= 0 else ""
            pnl_color = "#16a34a" if avg >= 0 else "#cc0000"
            is_best = (w == best_window)
            cls = ' class="timing-best"' if is_best else ''
            agg_cells += '<td{}><span style="color:{};font-weight:700">{}{:.1f}%</span></td>'.format(
                cls, pnl_color, pnl_sign, avg)
        else:
            agg_cells += '<td style="color:var(--grey-400)">---</td>'

    if not band_rows:
        return ""

    return """<div class="learn-card learn-card-wide">
    <h3>Exit Timing Analysis</h3>
    <p style="font-size:0.82rem;color:var(--ink-mid);margin-bottom:0.8rem">
        Average P&amp;L by holding period. <span style="background:#e0f5f5;padding:1px 6px;border-radius:3px;font-weight:700;color:var(--accent)">Teal</span> highlights the optimal window.
    </p>
    <table style="width:100%">
        <thead><tr><th>Band</th>{headers}</tr></thead>
        <tbody>
            {band_rows}
            <tr style="border-top:2px solid var(--grey-200)">{agg_cells}</tr>
        </tbody>
    </table>
</div>""".format(
        headers="".join(header_cells),
        band_rows="\n".join(band_rows),
        agg_cells=agg_cells)


# ---------------------------------------------------------------------------
# Keep existing functions unchanged
# ---------------------------------------------------------------------------

def _build_band_cards(band_perf):
    """Build confidence band cluster cards."""
    cards = []
    for band_key in ["A", "B", "C", "D", "E"]:
        bp = band_perf.get(band_key, {})
        if not bp.get("count", 0):
            continue

        bc = BANDS[band_key]["color"]
        bg = BANDS[band_key]["bg"]
        label = bp.get("label", "")
        pnl = bp.get("avg_pnl", 0)
        pnl_color = "#16a34a" if pnl >= 0 else "#cc0000"
        pnl_sign = "+" if pnl >= 0 else ""

        members_html = ""
        for mem in bp.get("members", [])[:6]:
            dot = _status_dot(mem.get("status", "grey"))
            mem_pnl = mem.get("report_pnl")
            mem_pnl_str = "{:.1f}%".format(mem_pnl) if mem_pnl is not None else "---"
            members_html += '<div class="member">{dot} {name} ({ticker}) <span style="margin-left:auto;font-weight:700">{pnl}</span></div>'.format(
                dot=dot,
                name=mem.get("asset_theme", "?")[:25],
                ticker=mem.get("primary_ticker", ""),
                pnl=mem_pnl_str
            )

        cards.append("""<div class="band-card" style="border-left-color:{bc}">
    <div class="band-letter" style="color:{bc}">{band}</div>
    <div class="band-label" style="color:{bc}">{label}</div>
    <div class="band-stats">
        <div><span class="num">{count}</span> positions</div>
        <div><span class="num">{signal_count}</span> measured</div>
        <div><span class="num">{wr:.0f}%</span> win rate</div>
        <div><span class="num" style="color:{pc}">{ps}{pnl:.1f}%</span> avg P&amp;L</div>
    </div>
    <div class="members">{members}</div>
</div>""".format(
            bc=bc, band=band_key, label=label,
            count=bp["count"], signal_count=bp.get("signal_count", 0),
            wr=bp.get("win_rate", 0),
            pc=pnl_color, ps=pnl_sign, pnl=pnl,
            members=members_html
        ))

    if not cards:
        return '<p style="color:var(--grey-400);font-style:italic;">No data yet.</p>'
    return "\n".join(cards)


def _build_learning_dashboard(data):
    """Build learning dashboard cards."""
    cards = []

    # Edge Quality card
    ea = data.get("edge_analysis", {})
    if ea:
        rows = ""
        for eq in ["HIGH", "DECAYING"]:
            ed = ea.get(eq, {})
            if ed.get("count", 0) > 0:
                rows += "<tr><td>{}</td><td>{}</td><td>{}%</td><td>{}%</td></tr>".format(
                    eq, ed["count"], ed["win_rate"], ed["avg_pnl"]
                )
        if rows:
            cards.append("""<div class="learn-card">
    <h3>Edge Quality</h3>
    <table><thead><tr><th>Quality</th><th>Count</th><th>Win Rate</th><th>Avg P&amp;L</th></tr></thead>
    <tbody>{}</tbody></table>
</div>""".format(rows))

    # Direction Analysis card
    da = data.get("direction_analysis", {})
    if da:
        rows = ""
        for d in ["SHORT", "LONG", "MIXED"]:
            dd = da.get(d, {})
            if dd.get("count", 0) > 0:
                rows += "<tr><td style='color:{}'>{}</td><td>{}</td><td>{}%</td><td>{}%</td></tr>".format(
                    _direction_color(d), d, dd["count"], dd["win_rate"], dd["avg_pnl"]
                )
        if rows:
            cards.append("""<div class="learn-card">
    <h3>Direction Analysis</h3>
    <table><thead><tr><th>Direction</th><th>Count</th><th>Win Rate</th><th>Avg P&amp;L</th></tr></thead>
    <tbody>{}</tbody></table>
</div>""".format(rows))

    # Propagation Analysis card
    pa = data.get("propagation_analysis", {})
    if pa:
        rows = ""
        for p in ["IGNITE", "CATALYTIC", "SILENT", "FRAGILE"]:
            pd_data = pa.get(p, {})
            if pd_data.get("count", 0) > 0:
                rows += "<tr><td>{}</td><td>{}</td><td>{}%</td><td>{}%</td></tr>".format(
                    p, pd_data["count"], pd_data["win_rate"], pd_data["avg_pnl"]
                )
        if rows:
            cards.append("""<div class="learn-card">
    <h3>Propagation Posture</h3>
    <table><thead><tr><th>Posture</th><th>Count</th><th>Win Rate</th><th>Avg P&amp;L</th></tr></thead>
    <tbody>{}</tbody></table>
</div>""".format(rows))

    # Kill Validation card
    kv = data.get("kill_validation", {})
    if kv.get("total_kills", 0) > 0:
        cards.append("""<div class="learn-card">
    <h3>Kill Validation</h3>
    <table><thead><tr><th>Metric</th><th>Value</th></tr></thead>
    <tbody>
        <tr><td>Total Kills</td><td style="font-weight:700">{total}</td></tr>
        <tr><td>Good Kills (avoided loss)</td><td style="font-weight:700;color:var(--green)">{good}</td></tr>
        <tr><td>Bad Kills (missed profit)</td><td style="font-weight:700;color:var(--red)">{bad}</td></tr>
        <tr><td>Neutral</td><td style="font-weight:700">{neutral}</td></tr>
        <tr><td>Kill Accuracy</td><td style="font-weight:700">{acc:.0f}%</td></tr>
    </tbody></table>
</div>""".format(
            total=kv["total_kills"], good=kv["good_kills"],
            bad=kv["bad_kills"], neutral=kv["neutral_kills"],
            acc=kv["kill_accuracy"]
        ))

    # Staleness Impact card
    si = data.get("staleness_impact", {})
    if si:
        rows = ""
        for window in ["0-6h", "6-24h", "24-48h", "48h+"]:
            sw = si.get(window, {})
            if sw.get("count", 0) > 0:
                rows += "<tr><td>{}</td><td>{}</td><td>{}%</td><td>{}%</td></tr>".format(
                    window, sw["count"], sw["win_rate"], sw["avg_pnl"]
                )
        if rows:
            cards.append("""<div class="learn-card">
    <h3>Staleness Impact</h3>
    <table><thead><tr><th>Window</th><th>Count</th><th>Win Rate</th><th>Avg P&amp;L</th></tr></thead>
    <tbody>{}</tbody></table>
</div>""".format(rows))

    # Optimal Timing card
    ta = data.get("timing_analysis", {})
    if ta:
        rows = ""
        for band_key in ["A", "B", "C", "D", "E"]:
            bt = ta.get(band_key, {})
            best = bt.get("best_window", "N/A")
            if best != "N/A":
                best_data = bt.get("windows", {}).get(best, {})
                rows += "<tr><td style='color:{}'>{} ({})</td><td>{}</td><td>{}%</td><td>{}</td></tr>".format(
                    _band_color(band_key), band_key, BANDS[band_key]["label"],
                    best, best_data.get("avg_pnl", 0), best_data.get("data_points", 0)
                )
        if rows:
            cards.append("""<div class="learn-card">
    <h3>Optimal Holding Period</h3>
    <table><thead><tr><th>Band</th><th>Best Window</th><th>Avg P&amp;L</th><th>Data Points</th></tr></thead>
    <tbody>{}</tbody></table>
</div>""".format(rows))

    if not cards:
        return '<div class="learn-card"><h3>Learning</h3><p style="color:var(--grey-400);font-style:italic;">Insufficient data. The system will learn as more positions are tracked.</p></div>'

    return "\n".join(cards)


# ---------------------------------------------------------------------------
# Dynamic Headline
# ---------------------------------------------------------------------------

def _dynamic_headline(s):
    """Generate dynamic headline based on alpha group performance."""
    alpha_measured = s.get("alpha_measured", 0)
    alpha_wr = s.get("alpha_win_rate", 0)
    alpha_pf = s.get("alpha_profit_factor", 0)

    if alpha_measured > 0:
        parts = []
        parts.append("{} Alpha Signals".format(alpha_measured))
        if alpha_wr > 0:
            parts.append("{:.0f}% Win Rate".format(alpha_wr))
        if alpha_pf > 1:
            parts.append("{:.1f}x Profit Factor".format(alpha_pf))
        if s.get("research_measured", 0) > 0:
            parts.append("{} Research Signals".format(s["research_measured"]))
        return ", ".join(parts)

    if s["total_candidates"] == 0:
        return "No positions yet. Awaiting first report."
    if s["pipeline_count"] > 0:
        return "{} candidates in alpha pipeline, awaiting optimal entry.".format(s["pipeline_count"])
    return "Tracking {} positions across confidence bands.".format(s["total_candidates"])


# ---------------------------------------------------------------------------
# Position detail pages (unchanged)
# ---------------------------------------------------------------------------

def _generate_position_pages(candidates):
    """Generate individual HTML detail pages for each position.

    Each page is a human-readable log of everything the bot has thought and decided
    about this position. Linked from the trading sheet via a clipboard icon.
    Pages live in reports/positions/position_N.html
    """
    positions_dir = os.path.join(REPORTS_DIR, "positions")
    os.makedirs(positions_dir, exist_ok=True)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    for m in candidates:
        if not m.get("is_active", 1):
            continue

        cid = m.get("id", 0)
        ticker = m.get("primary_ticker") or "?"
        asset = (m.get("asset_theme") or "?")[:80]
        state = m["state"]
        direction = m.get("direction", "?")
        conf = m.get("confidence_pct", 0)
        band = m.get("band", "E")
        band_label = m.get("band_label", "")

        # State styling
        if state == "WATCH":
            state_color, state_bg = "#6b7280", "#f1f5f9"
            state_label = "WATCHING"
        elif state in ("ACTIVE", "PUBLISH"):
            state_color, state_bg = "#2563eb", "#dbeafe"
            state_label = "TRADING"
        elif state == "KILLED":
            state_color, state_bg = "#7c3aed", "#f3e8ff"
            state_label = "KILLED"
        else:
            state_color, state_bg = "#73788a", "#f1f5f9"
            state_label = state

        # Conviction
        conv = m.get("latest_conviction")
        conv_html = ""
        if conv:
            conv_pct = min(conv * 10, 100)
            if conv >= 7: conv_color = "#16a34a"
            elif conv >= 5: conv_color = "#f59e0b"
            elif conv >= 3: conv_color = "#ea580c"
            else: conv_color = "#cc0000"
            conv_html = """<div style="margin:1rem 0">
                <div style="font-size:0.75rem;color:#6b7280;font-weight:700;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px">Conviction</div>
                <div style="display:flex;align-items:center;gap:10px">
                    <div style="flex:1;max-width:200px;height:8px;background:#e5e7eb;border-radius:4px;overflow:hidden">
                        <div style="width:{pct}%;height:100%;background:{color};border-radius:4px"></div>
                    </div>
                    <span style="color:{color};font-weight:700;font-size:1.1rem">{conv}/10</span>
                </div>
            </div>""".format(pct=conv_pct, color=conv_color, conv=conv)

        # Thesis status
        thesis_st = m.get("latest_thesis_status", "")
        thesis_html = ""
        if thesis_st:
            thesis_colors = {"intact": ("#166534", "#dcfce7"), "strengthening": ("#065f46", "#d1fae5"),
                "weakening": ("#92400e", "#fef3c7"), "invalidated": ("#991b1b", "#fef2f2")}
            tc, tbg = thesis_colors.get(thesis_st, ("#73788a", "#f1f5f9"))
            thesis_html = '<span style="display:inline-block;padding:3px 10px;border-radius:12px;font-size:0.75rem;font-weight:700;color:{};background:{}">{}</span>'.format(tc, tbg, thesis_st.upper())

        # Signal velocity
        sig_velocity = m.get("signal_velocity", "quiet")
        sig_hits = m.get("signal_hits_24h", 0)
        sig_colors = {"quiet": ("#166534", "#dcfce7", "&#128263;"), "stirring": ("#92400e", "#fef3c7", "&#128264;"),
            "propagating": ("#b45309", "#ffedd5", "&#128266;"), "mainstream": ("#991b1b", "#fef2f2", "&#128680;")}
        sig_c, sig_bg, sig_icon = sig_colors.get(sig_velocity, ("#73788a", "#f1f5f9", ""))
        signal_html = '<span style="display:inline-block;padding:3px 10px;border-radius:12px;font-size:0.75rem;font-weight:700;color:{};background:{}">{} {} ({})</span>'.format(
            sig_c, sig_bg, sig_icon, sig_velocity, sig_hits)

        # Prices section
        entry_price = m.get("entry_price")
        dd_price = m.get("dd_approved_price")
        current_price = m.get("current_price")
        report_pnl = m.get("report_pnl")
        current_pnl = m.get("current_pnl")

        prices_html = '<div style="display:flex;gap:1.5rem;flex-wrap:wrap;margin:1rem 0">'
        if dd_price:
            prices_html += '<div><div style="font-size:0.7rem;color:#6b7280;text-transform:uppercase">DD Price</div><div style="font-weight:700;font-size:1.1rem">${:.2f}</div></div>'.format(dd_price)
        if entry_price:
            prices_html += '<div><div style="font-size:0.7rem;color:#6b7280;text-transform:uppercase">Entry Price</div><div style="font-weight:700;font-size:1.1rem">${:.2f}</div></div>'.format(entry_price)
        if current_price:
            prices_html += '<div><div style="font-size:0.7rem;color:#6b7280;text-transform:uppercase">Current</div><div style="font-weight:700;font-size:1.1rem">${:.2f}</div></div>'.format(current_price)
        if report_pnl is not None:
            rp_sign = "+" if report_pnl >= 0 else ""
            prices_html += '<div><div style="font-size:0.7rem;color:#7c3aed;text-transform:uppercase">Report P&amp;L</div><div style="font-weight:700;font-size:1.1rem;color:#7c3aed">{}{:.1f}%</div></div>'.format(rp_sign, report_pnl)
        if current_pnl is not None:
            tp_sign = "+" if current_pnl >= 0 else ""
            tp_color = "#16a34a" if current_pnl >= 0 else "#cc0000"
            prices_html += '<div><div style="font-size:0.7rem;color:{};text-transform:uppercase">Trade P&amp;L</div><div style="font-weight:700;font-size:1.1rem;color:{}">{}{:.1f}%</div></div>'.format(tp_color, tp_color, tp_sign, current_pnl)
        prices_html += '</div>'

        # State reason
        reason = m.get("state_reason") or m.get("kill_reason") or ""
        reason_html = ""
        if reason:
            reason_html = '<div style="background:#f9fafb;border-radius:6px;padding:0.8rem 1rem;margin:0.8rem 0;font-size:0.85rem;color:#374151;border-left:3px solid {}"><strong>Status:</strong> {}</div>'.format(
                state_color, reason[:300])

        # Report thesis/mechanism
        mechanism = m.get("mechanism") or m.get("headline") or ""
        tripwire = m.get("tripwire") or ""
        evidence = m.get("evidence") or ""
        risks = m.get("risks") or ""
        thesis_block = ""
        if mechanism or tripwire or evidence:
            thesis_block = '<div style="margin:1rem 0;padding:1rem;background:#fefce8;border-radius:6px;border-left:3px solid #eab308">'
            thesis_block += '<div style="font-size:0.75rem;color:#92400e;font-weight:700;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px">Original Report Thesis</div>'
            if mechanism:
                thesis_block += '<div style="font-size:0.85rem;color:#374151;margin-bottom:6px"><strong>Mechanism:</strong> {}</div>'.format(mechanism[:400])
            if tripwire:
                thesis_block += '<div style="font-size:0.85rem;color:#374151;margin-bottom:6px"><strong>Tripwire:</strong> {}</div>'.format(tripwire[:300])
            if evidence:
                thesis_block += '<div style="font-size:0.85rem;color:#374151;margin-bottom:6px"><strong>Evidence:</strong> {}</div>'.format(evidence[:400])
            if risks:
                thesis_block += '<div style="font-size:0.85rem;color:#b45309"><strong>Risks:</strong> {}</div>'.format(risks[:300])
            thesis_block += '</div>'

        # Watching for
        watching = m.get("latest_watching_for", "")
        watching_html = ""
        if watching:
            watching_html = '<div style="background:#f0fdf4;border-radius:6px;padding:0.8rem 1rem;margin:0.8rem 0;font-size:0.85rem;border-left:3px solid #22c55e"><strong>&#128269; Watching For:</strong> {}</div>'.format(watching[:400])

        # Concerns
        concerns = m.get("latest_concerns", "")
        concerns_html = ""
        if concerns:
            concerns_html = '<div style="background:#fff7ed;border-radius:6px;padding:0.8rem 1rem;margin:0.8rem 0;font-size:0.85rem;border-left:3px solid #f97316"><strong>&#9888; Concerns:</strong> {}</div>'.format(concerns[:400])

        # Narrative entries (ALL of them)
        narrative_html = ""
        narratives = m.get("latest_narrative_entries", [])
        if narratives:
            narrative_html = '<div style="margin:1.5rem 0">'
            narrative_html += '<div style="font-size:0.8rem;color:#6b7280;font-weight:700;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:0.8rem;padding-bottom:4px;border-bottom:1px solid #e5e7eb">Private Narrative Log</div>'
            for entry in narratives:
                narrative_html += '<div style="margin-bottom:1rem;padding:0.8rem 1rem;background:white;border-radius:6px;border-left:3px solid #d1d5db;box-shadow:0 1px 2px rgba(0,0,0,0.04)">'
                narrative_html += '<div style="font-size:0.85rem;color:#374151;line-height:1.6">{}</div>'.format(entry.get("narrative", "")[:600])
                narrative_html += '<div style="font-size:0.7rem;color:#9ca3af;margin-top:4px">Cycle {} &middot; {}</div>'.format(
                    entry.get("cycle", "?"), entry.get("timestamp", "?"))
                narrative_html += '</div>'
            narrative_html += '</div>'

        # DD log (ALL entries)
        dd_html = ""
        dd_entries = m.get("dd_entries", [])
        if dd_entries:
            dd_html = '<div style="margin:1.5rem 0">'
            dd_html += '<div style="font-size:0.8rem;color:#6b7280;font-weight:700;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:0.8rem;padding-bottom:4px;border-bottom:1px solid #e5e7eb">Due Diligence Log</div>'
            for dd in dd_entries:
                dd_decision = dd.get("decision", "?")
                dd_reason = dd.get("decision_reason", "")[:400]
                dd_time = (dd.get("checked_at") or "")[:16]
                dd_type = dd.get("dd_type", "")
                dd_stale = dd.get("staleness_hours", 0)
                dd_price_check = dd.get("price_at_check")
                dd_move = dd.get("price_move_since_report", 0)
                dd_color = "#16a34a" if dd_decision in ("TRADE", "PUBLISH") else "#cc0000" if dd_decision == "KILL" else "#f59e0b"
                dd_html += '<div style="margin-bottom:0.8rem;padding:0.6rem 1rem;background:#f9fafb;border-radius:6px;border-left:3px solid {}">' .format(dd_color)
                dd_html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">'
                dd_html += '<span style="font-weight:700;color:{}">{}</span>'.format(dd_color, dd_decision)
                dd_html += '<span style="font-size:0.7rem;color:#9ca3af">{} &middot; {}</span>'.format(dd_type, dd_time)
                dd_html += '</div>'
                dd_html += '<div style="font-size:0.82rem;color:#374151">{}</div>'.format(dd_reason)
                if dd_price_check:
                    dd_html += '<div style="font-size:0.72rem;color:#6b7280;margin-top:3px">Price: ${:.2f} | Move: {:.1f}% | Staleness: {:.0f}h</div>'.format(dd_price_check, dd_move, dd_stale)
                dd_html += '</div>'
            dd_html += '</div>'

        # Timeline
        timeline_html = ""
        timeline = m.get("timeline", [])
        if timeline:
            timeline_html = '<div style="margin:1.5rem 0">'
            timeline_html += '<div style="font-size:0.8rem;color:#6b7280;font-weight:700;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:0.8rem;padding-bottom:4px;border-bottom:1px solid #e5e7eb">Price History</div>'
            timeline_html += '<div style="display:flex;flex-wrap:wrap;gap:6px">'
            for pt in timeline:
                pnl = pt.get("pnl_pct")
                status = pt.get("status", "grey")
                color_map = {"green": "#16a34a", "red": "#cc0000", "orange": "#f59e0b", "purple": "#7c3aed", "grey": "#9ca3af"}
                pt_color = color_map.get(status, "#9ca3af")
                pnl_str = "{:+.1f}%".format(pnl) if pnl is not None else ""
                timeline_html += '<span style="font-size:0.7rem;color:{};padding:2px 6px;background:#f9fafb;border-radius:4px">{} ${:.2f} {}</span>'.format(
                    pt_color, pt.get("time", "")[-5:], pt.get("price", 0), pnl_str)
            timeline_html += '</div></div>'

        # Build the full page
        page_html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{ticker} &mdash; {asset} | Trader Notes</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Lato', -apple-system, sans-serif; background: #FFF1E5; color: #1a1a2e; line-height: 1.6; }}
        .container {{ max-width: 760px; margin: 0 auto; padding: 1.5rem; }}
        a {{ color: #0d7680; }}
    </style>
    <link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700&family=Lato:wght@300;400;700&display=swap" rel="stylesheet">
</head>
<body>
<div class="container">
    <div style="margin-bottom:1rem">
        <a href="../" style="font-size:0.8rem;color:#6b7280;text-decoration:none">&larr; Back to Trading Sheet</a>
    </div>

    <div style="display:flex;align-items:center;gap:12px;margin-bottom:0.5rem;flex-wrap:wrap">
        <span style="font-family:'Playfair Display',serif;font-size:1.8rem;font-weight:700">{ticker}</span>
        <span style="font-size:0.8rem;padding:4px 12px;border-radius:12px;color:{state_color};background:{state_bg};font-weight:700">{state_label}</span>
        <span style="font-size:0.85rem;color:#6b7280">{direction} {conf:.0f}% &middot; Band {band}</span>
    </div>
    <div style="font-size:1rem;color:#4b5563;margin-bottom:0.5rem">{asset}</div>

    {prices_html}
    <div style="margin:0.5rem 0">{thesis_html} {signal_html}</div>
    {conv_html}
    {reason_html}
    {thesis_block}
    {watching_html}
    {concerns_html}
    {narrative_html}
    {dd_html}
    {timeline_html}

    <div style="margin-top:2rem;padding-top:1rem;border-top:1px solid #e5e7eb;font-size:0.72rem;color:#9ca3af;text-align:center">
        Position #{cid} &middot; Updated {now_str} &middot; <a href="../" style="color:#9ca3af">Back to Trading Sheet</a>
    </div>
</div>
</body>
</html>""".format(
            ticker=ticker, asset=asset, state_color=state_color,
            state_bg=state_bg, state_label=state_label,
            direction=direction, conf=conf, band=band,
            prices_html=prices_html, thesis_html=thesis_html,
            signal_html=signal_html, conv_html=conv_html,
            reason_html=reason_html, thesis_block=thesis_block,
            watching_html=watching_html, concerns_html=concerns_html,
            narrative_html=narrative_html, dd_html=dd_html,
            timeline_html=timeline_html, cid=cid, now_str=now_str
        )

        # Write the page
        page_path = os.path.join(positions_dir, "position_{}.html".format(cid))
        with open(page_path, "w") as f:
            f.write(page_html)


# ---------------------------------------------------------------------------
# Main report generator
# ---------------------------------------------------------------------------

def generate_html_report():
    """Generate the full HTML report with Noah Pink design."""
    os.makedirs(REPORTS_DIR, exist_ok=True)

    data = generate_analytics()
    s = data["summary"]
    now = datetime.now(timezone.utc)
    now_str = now.strftime("%Y-%m-%d %H:%M UTC")

    # Classify candidates into three sections
    active_list, pipeline_list, research_list, hidden_kills = _classify_candidates(data["candidates"])

    # Build three trading sheet sections
    active_section = _build_active_section(active_list)
    pipeline_section = _build_pipeline_section(pipeline_list)
    research_section = _build_research_section(research_list, hidden_kills)

    # Build backtest card
    backtest_card = _build_backtest_card(s)

    # Build band cluster cards
    band_cards = _build_band_cards(data["band_performance"])

    # Build exit timing card
    exit_timing = _build_exit_timing_card(data)

    # Build learning dashboard
    learning = _build_learning_dashboard(data)

    # Generate individual position detail pages
    _generate_position_pages(data["candidates"])

    # Hero stats
    headline = _dynamic_headline(s)

    # Pre-compute values that need sign handling for the template
    alpha_total = s.get("alpha_total_pnl", 0)
    alpha_total_sign = "+" if alpha_total >= 0 else ""

    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Hedge Fund Edge Tracker</title>
<!-- Open Graph / Social sharing preview -->
<meta property="og:type" content="website">
<meta property="og:title" content="NOAH Hedge Fund Edge Tracker">
<meta property="og:description" content="Alpha Group: {alpha_measured} signals, {alpha_win_rate:.0f}% win rate, {alpha_total_sign}{alpha_total_pnl:.0f}% P&amp;L, {alpha_profit_factor:.1f}x profit factor.">
<meta property="og:image" content="https://ivanmassow.github.io/hedgefund-tracker/og-image.png?v=2">
<meta property="og:url" content="https://ivanmassow.github.io/hedgefund-tracker/">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="NOAH Hedge Fund Edge Tracker">
<meta name="twitter:description" content="Alpha Group: {alpha_measured} signals, {alpha_win_rate:.0f}% win rate, {alpha_total_sign}{alpha_total_pnl:.0f}% P&amp;L, {alpha_profit_factor:.1f}x profit factor.">
<meta name="twitter:image" content="https://ivanmassow.github.io/hedgefund-tracker/og-image.png?v=2">
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700&family=Lato:wght@300;400;700&family=Montserrat:wght@600;700&display=swap" rel="stylesheet">
<style>
:root {{
    --ink: #262a33;
    --ink-light: #3d424d;
    --ink-mid: #5a5f6b;
    --ink-subtle: #73788a;
    --grey-100: #f3f4f6;
    --grey-200: #e5e7eb;
    --grey-300: #d1d5db;
    --grey-400: #9ea2b0;
    --paper: #FFF1E5;
    --accent: #0d7680;
    --accent-light: #0e8c97;
    --blush: #ffe4d6;
    --blush-dark: #ffd6c2;
    --warm: #c9926b;
    --green: #16a34a;
    --orange: #f59e0b;
    --red: #cc0000;
    --purple: #7c3aed;
    --purple-dark: #5b21b6;
}}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    font-family: 'Lato', sans-serif;
    background: var(--paper);
    color: var(--ink);
    -webkit-font-smoothing: antialiased;
    padding-top: 56px;
}}
.container {{ max-width: 1120px; margin: 0 auto; padding: 0 2rem; }}

/* Header */
.header {{
    position: fixed; top: 0; left: 0; right: 0; z-index: 100;
    background: var(--ink); height: 56px;
    display: flex; align-items: center; padding: 0 2rem;
}}
.header .logo {{
    font-family: 'Montserrat', sans-serif; font-weight: 700;
    color: #fff; font-size: 1.3rem; letter-spacing: 0.08em;
    text-transform: uppercase;
}}
.header .nav {{ display: flex; gap: 1.5rem; margin-left: 3rem; }}
.header .nav a {{
    color: var(--grey-400); text-decoration: none;
    font-size: 0.82rem; letter-spacing: 0.04em;
    transition: color 0.2s;
}}
.header .nav a:hover {{ color: #fff; }}
.header .meta {{
    margin-left: auto; color: var(--grey-400);
    font-size: 0.78rem; letter-spacing: 0.02em;
}}

/* Hero */
.hero {{
    background: var(--ink); color: #fff;
    padding: 3rem 0 2.5rem; margin-top: -56px; padding-top: calc(56px + 3rem);
}}
.hero h1 {{
    font-family: 'Playfair Display', serif;
    font-size: clamp(2rem, 4.5vw, 3rem); font-weight: 700;
    letter-spacing: -0.01em; margin-bottom: 0.5rem;
}}
.hero .subtitle {{
    font-size: 0.72rem; font-weight: 700;
    letter-spacing: 0.14em; text-transform: uppercase;
    color: #FFA089; margin-bottom: 1rem;
}}
.hero .headline {{
    font-family: 'Playfair Display', serif;
    font-size: 1.1rem; font-weight: 400; font-style: italic;
    color: var(--grey-300); max-width: 700px; margin-bottom: 1.5rem;
}}
.stat-grid {{
    display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
    gap: 1.5rem; margin-top: 1rem;
}}
.stat-box .num {{
    font-family: 'Playfair Display', serif;
    font-size: clamp(1.8rem, 4vw, 2.6rem); font-weight: 700;
}}
.stat-box .label {{
    font-size: 0.72rem; letter-spacing: 0.06em;
    text-transform: uppercase; color: var(--grey-400);
}}
.stat-box .num.green {{ color: #4ade80; }}
.stat-box .num.accent {{ color: #2dd4bf; }}

/* Backtest Card */
.backtest-card {{
    background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 8px;
    padding: 1.5rem 2rem;
    margin: 2rem 0;
    color: #fff;
}}
.backtest-header {{
    font-family: 'Montserrat', sans-serif;
    font-size: 0.78rem; font-weight: 700;
    letter-spacing: 0.1em; text-transform: uppercase;
    color: #fbbf24;
    margin-bottom: 1rem;
}}
.backtest-icon {{
    margin-right: 6px;
}}
.backtest-stats {{
    display: flex; flex-wrap: wrap; gap: 2rem;
    margin-bottom: 1rem;
}}
.backtest-stat {{
    text-align: center;
}}
.backtest-num {{
    font-family: 'Playfair Display', serif;
    font-size: 1.8rem; font-weight: 700;
    color: #fff;
}}
.backtest-label {{
    font-size: 0.68rem; letter-spacing: 0.06em;
    text-transform: uppercase; color: rgba(255,255,255,0.5);
    margin-top: 2px;
}}
.backtest-rules {{
    font-size: 0.75rem; color: rgba(255,255,255,0.45);
    border-top: 1px solid rgba(255,255,255,0.08);
    padding-top: 0.8rem;
}}
.backtest-rules .rules-label {{
    font-weight: 700; color: rgba(255,255,255,0.6);
    text-transform: uppercase; letter-spacing: 0.06em;
    font-size: 0.68rem;
}}

/* Sections */
.section {{ padding: 2.5rem 0; scroll-margin-top: 72px; }}
.section-label {{
    font-size: 0.72rem; font-weight: 700;
    letter-spacing: 0.14em; text-transform: uppercase;
    color: var(--accent); margin-bottom: 0.8rem;
}}
.section-title {{
    font-family: 'Playfair Display', serif;
    font-size: clamp(1.5rem, 3.5vw, 2.2rem);
    font-weight: 600; margin-bottom: 0.5rem;
}}
.section-intro {{
    color: var(--ink-mid); font-size: 0.95rem;
    max-width: 680px; margin-bottom: 2rem;
}}

/* Section Bars (separators between Active / Pipeline / Research) */
.section-bar {{
    display: flex; align-items: center; gap: 0.5rem;
    padding: 0.6rem 1rem;
    font-family: 'Montserrat', sans-serif;
    font-size: 0.78rem; font-weight: 700;
    letter-spacing: 0.08em; text-transform: uppercase;
    border-radius: 4px;
    margin-bottom: 0.5rem;
    margin-top: 2rem;
}}
.section-bar:first-child {{ margin-top: 0; }}
.section-bar-icon {{ font-size: 0.9rem; }}
.section-bar-count {{
    margin-left: auto;
    font-size: 0.7rem; font-weight: 400;
    letter-spacing: 0.02em; text-transform: none;
    opacity: 0.7;
}}
.section-bar-active {{
    background: #dbeafe; color: #1d4ed8;
    border-left: 4px solid #2563eb;
}}
.section-bar-pipeline {{
    background: #e0f5f5; color: #0d7680;
    border-left: 4px solid #0d7680;
}}
.section-bar-research {{
    background: #f3f4f6; color: #6b7280;
    border-left: 4px solid #d1d5db;
}}
.section-empty {{
    padding: 1.5rem; text-align: center;
    color: var(--grey-400); font-style: italic;
    font-size: 0.88rem;
}}

/* Trading Sheet Tables */
.table-scroll {{
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
}}
.trading-table {{
    width: 100%; border-collapse: collapse;
    font-size: 0.85rem;
}}
.trading-table thead th {{
    background: var(--paper); padding: 0.6rem 0.5rem;
    text-align: left; font-weight: 700;
    font-size: 0.7rem; letter-spacing: 0.06em;
    text-transform: uppercase; color: var(--ink-subtle);
    border-bottom: 2px solid var(--grey-200);
    white-space: nowrap;
}}
.trading-table tbody tr {{
    border-bottom: 1px solid var(--grey-100);
    transition: background 0.15s;
}}
.trading-table tbody tr:hover {{ background: var(--blush); }}
.trading-table td {{
    padding: 0.5rem 0.5rem; vertical-align: middle;
}}

/* Active rows */
.active-row {{
    border-left: 3px solid #2563eb;
    background: #fafbff;
}}
.active-row:hover {{ background: #eef2ff !important; }}
.td-ticker-active {{
    font-weight: 700; font-size: 0.95rem;
    white-space: nowrap;
}}
.ticker-name {{
    font-family: 'Montserrat', sans-serif;
    letter-spacing: 0.04em;
}}
.trade-badge {{
    display: inline-flex; align-items: center; justify-content: center;
    width: 20px; height: 20px; border-radius: 50%;
    background: #2563eb; color: #fff;
    font-family: 'Montserrat', sans-serif;
    font-weight: 700; font-size: 0.65rem;
    line-height: 1; margin-left: 4px;
    vertical-align: middle;
    box-shadow: 0 1px 3px rgba(37, 99, 235, 0.3);
}}

/* Pipeline rows */
.pipeline-row {{
    border-left: 3px solid #0d7680;
    background: #fafffe;
}}
.pipeline-row:hover {{ background: #e8faf9 !important; }}
.td-ticker-pipeline {{
    font-weight: 700; font-size: 0.9rem;
    white-space: nowrap;
}}

/* Research rows */
.research-table {{
    opacity: 0.75;
    font-size: 0.8rem;
}}
.research-table:hover {{
    opacity: 1;
}}
.killed-row {{ opacity: 0.7; }}
.killed-row .td-ticker-research {{ color: var(--purple-dark); }}
.research-watch-row {{ }}
.td-ticker-research {{
    font-weight: 700; font-size: 0.82rem;
    white-space: nowrap;
}}
.td-asset-compact {{
    max-width: 180px; white-space: nowrap;
    overflow: hidden; text-overflow: ellipsis;
    font-size: 0.82rem;
}}
.td-reason {{
    font-size: 0.72rem; color: var(--ink-subtle);
    max-width: 200px; white-space: nowrap;
    overflow: hidden; text-overflow: ellipsis;
}}
.research-hidden-note {{
    text-align: center; color: var(--grey-400);
    padding: 0.8rem; font-size: 0.75rem;
    font-style: italic;
}}

/* Shared cell styles */
.td-band {{
    font-family: 'Playfair Display', serif;
    font-weight: 700; font-size: 1rem; text-align: center;
    width: 2.5rem;
}}
.td-asset {{ max-width: 220px; }}
.td-asset .name {{ font-weight: 700; font-size: 0.85rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
.td-asset .thesis {{ font-size: 0.68rem; color: var(--ink-mid); font-style: italic; margin-top: 1px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 210px; }}
.td-dir {{
    font-weight: 700; font-size: 0.72rem;
    letter-spacing: 0.04em; text-align: center;
    padding: 2px 8px; border-radius: 3px;
    display: inline-block;
}}
.td-price {{ font-weight: 700; white-space: nowrap; }}
.td-pnl {{ font-weight: 700; white-space: nowrap; text-align: right; }}
.td-state {{
    font-weight: 700; font-size: 0.72rem;
    letter-spacing: 0.04em; text-align: center;
    padding: 2px 8px; border-radius: 3px;
    display: inline-block; white-space: nowrap;
}}
.td-timeline {{
    font-size: 0.75rem; white-space: nowrap;
    max-width: 300px; overflow-x: auto;
}}
.tl-point {{ display: inline-block; margin: 0 1px; }}
.tl-point sup {{ font-size: 0.55rem; color: var(--ink-subtle); }}
.tl-point sub {{ font-size: 0.6rem; }}
.tl-arrow {{ color: var(--grey-300); margin: 0 2px; font-size: 0.7rem; }}
.tl-empty {{ color: var(--grey-400); font-style: italic; }}
.kill-marker {{
    display: inline-block; background: var(--purple-dark);
    color: #fff; font-weight: 700; font-size: 0.65rem;
    width: 16px; height: 16px; line-height: 16px;
    text-align: center; border-radius: 50%; margin: 0 2px;
}}

/* Thesis badges */
.thesis-badge {{
    display: inline-block; font-size: 0.65rem; font-weight: 700;
    letter-spacing: 0.03em; text-transform: uppercase;
    padding: 1px 6px; border-radius: 3px;
    margin-left: 4px;
}}
.notes-link {{
    text-decoration: none; font-size: 0.95rem; opacity: 0.6;
    transition: opacity 0.2s; cursor: pointer; margin-left: 6px;
    vertical-align: middle;
}}
.notes-link:hover {{ opacity: 1; }}

/* Band cluster cards */
.band-grid {{
    display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1.2rem;
}}
.band-card {{
    background: #fff; border: 1px solid var(--grey-200);
    border-radius: 4px; padding: 1.5rem;
    border-left: 5px solid;
}}
.band-card .band-letter {{
    font-family: 'Playfair Display', serif;
    font-size: 1.8rem; font-weight: 700;
}}
.band-card .band-label {{
    font-size: 0.72rem; letter-spacing: 0.08em;
    text-transform: uppercase; margin-bottom: 0.8rem;
}}
.band-card .band-stats {{
    display: grid; grid-template-columns: 1fr 1fr;
    gap: 0.4rem; font-size: 0.82rem; margin-bottom: 0.8rem;
}}
.band-card .band-stats .num {{ font-weight: 700; }}
.band-card .members {{ font-size: 0.78rem; }}
.band-card .members .member {{
    display: flex; align-items: center; gap: 0.4rem;
    padding: 2px 0; border-bottom: 1px solid var(--grey-100);
}}

/* Learning dashboard */
.learn-grid {{
    display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 1.5rem;
}}
.learn-card {{
    background: #fff; border: 1px solid var(--grey-200);
    border-radius: 4px; padding: 1.5rem;
}}
.learn-card-wide {{
    grid-column: 1 / -1;
}}
.learn-card h3 {{
    font-family: 'Playfair Display', serif;
    font-size: 1.1rem; font-weight: 600;
    margin-bottom: 0.8rem;
}}
.learn-card table {{
    width: 100%; font-size: 0.82rem;
    border-collapse: collapse;
}}
.learn-card th {{
    text-align: left; font-weight: 700;
    font-size: 0.72rem; letter-spacing: 0.04em;
    text-transform: uppercase; color: var(--ink-subtle);
    padding: 0.3rem 0.4rem; border-bottom: 1px solid var(--grey-200);
}}
.learn-card td {{
    padding: 0.3rem 0.4rem; border-bottom: 1px solid var(--grey-100);
}}
.timing-best {{
    background: #e0f5f5 !important;
}}

/* Footer */
.footer {{
    background: var(--ink); color: var(--grey-400);
    padding: 2.5rem 0; margin-top: 3rem;
    font-size: 0.82rem;
}}
.footer .logo {{
    font-family: 'Montserrat', sans-serif;
    font-weight: 700; color: #fff; font-size: 1.1rem;
    letter-spacing: 0.08em; text-transform: uppercase;
    margin-bottom: 0.5rem;
}}

/* Responsive */
@media (max-width: 768px) {{
    .container {{ padding: 0 1rem; }}
    .stat-grid {{ grid-template-columns: repeat(2, 1fr); }}
    .trading-table {{ font-size: 0.78rem; }}
    .band-grid {{ grid-template-columns: 1fr; }}
    .learn-grid {{ grid-template-columns: 1fr; }}
    .backtest-stats {{ gap: 1rem; }}
    .backtest-num {{ font-size: 1.4rem; }}
}}
</style>
</head>
<body>

<!-- Header -->
<div class="header">
    <a href="https://ivanmassow.github.io/noah-dashboard/" style="text-decoration:none"><div class="logo">NOAH</div></a>
    <div class="nav">
        <a href="https://ivanmassow.github.io/polyhunter/">Poly Market</a>
        <a href="https://ivanmassow.github.io/hedgefund-tracker/" style="color:#fff">Hedge Fund</a>
        <a href="https://ivanmassow.github.io/company-watch/">Company Watch</a>
        <span style="color:rgba(255,255,255,0.15)">|</span>
        <a href="#sheet">Sheet</a>
        <a href="#clusters">Clusters</a>
        <a href="#learning">Learning</a>
    </div>
    <div class="meta">Hedge Fund &middot; Paper Trading &middot; {now_str}</div>
</div>

<!-- Hero -->
<div class="hero">
    <div class="container">
        <div class="subtitle">Hedge Fund Intelligence &middot; Paper Trading</div>
        <h1>Edge Tracker</h1>
        <div class="headline">{headline}</div>
        <div class="stat-grid">
            <div class="stat-box">
                <div class="num accent">{alpha_measured}</div>
                <div class="label">Alpha Signals</div>
            </div>
            <div class="stat-box">
                <div class="num">{active}</div>
                <div class="label">Active Trade{active_s}</div>
            </div>
            <div class="stat-box">
                <div class="num green">{alpha_total_sign}{alpha_total_pnl:.0f}%</div>
                <div class="label">Alpha P&amp;L</div>
            </div>
            <div class="stat-box">
                <div class="num">{alpha_win_rate:.0f}%</div>
                <div class="label">Alpha Win Rate</div>
            </div>
            <div class="stat-box">
                <div class="num accent">{alpha_profit_factor:.2f}&times;</div>
                <div class="label">Profit Factor</div>
            </div>
            <div class="stat-box">
                <div class="num" style="color:var(--grey-400)">{research_measured}</div>
                <div class="label">Research Signals</div>
            </div>
        </div>
    </div>
</div>

<!-- Backtest Performance Card -->
<div class="section" style="padding-bottom:0">
    <div class="container">
        {backtest_card}
    </div>
</div>

<!-- Trading Sheet -->
<div class="section" id="sheet">
    <div class="container">
        <div class="section-label">Act I</div>
        <div class="section-title">Trading Sheet</div>
        <div class="section-intro">
            <strong style="color:#2563eb">Active trades</strong> at the top,
            <strong style="color:#0d7680">Alpha Pipeline</strong> (LONG + Band A/B signals matching our formula) next,
            and the <strong style="color:#6b7280">Research Lab</strong> below for learning.
            <span style="color:#7c3aed;font-weight:700">Report P&amp;L</span> shows movement since the signal price.
        </div>

        {active_section}
        {pipeline_section}
        {research_section}
    </div>
</div>

<!-- Confidence Clusters -->
<div class="section" id="clusters">
    <div class="container">
        <div class="section-label">Act II</div>
        <div class="section-title">Confidence Clusters</div>
        <div class="section-intro">Performance by confidence band. Which probability tier produces the best results &mdash; the blue chips or the dark horses?</div>
        <div class="band-grid">
            {band_cards}
        </div>
    </div>
</div>

<!-- Learning Dashboard -->
<div class="section" id="learning">
    <div class="container">
        <div class="section-label">Act III</div>
        <div class="section-title">Learning Dashboard</div>
        <div class="section-intro">What is the system learning? Edge quality, direction accuracy, kill validation, optimal timing, and exit analysis by confidence band.</div>
        <div class="learn-grid">
            {exit_timing}
            {learning}
        </div>
    </div>
</div>

<!-- Footer -->
<div class="footer">
    <div class="container">
        <a href="https://ivanmassow.github.io/noah-dashboard/" style="text-decoration:none"><div class="logo">NOAH</div></a>
        <p>Information asymmetry intelligence &mdash; paper trading hedge fund recommendations to learn which signals work.</p>
        <p style="margin-top: 0.8rem; font-size: 0.72rem; color: rgba(255,241,229,0.5);">
            <a href="https://ivanmassow.github.io/polyhunter/" style="color:rgba(255,241,229,0.5);text-decoration:none">Poly Market</a> &middot;
            <a href="https://ivanmassow.github.io/hedgefund-tracker/" style="color:rgba(255,241,229,0.5);text-decoration:none">Hedge Fund</a> &middot;
            <a href="https://ivanmassow.github.io/company-watch/" style="color:rgba(255,241,229,0.5);text-decoration:none">Company Watch</a>
        </p>
        <p style="margin-top: 0.8rem; font-size: 0.75rem;">
            Report generated {now_str}. Tracking {total} positions ({active} active, {pipeline} pipeline, {killed} killed).
        </p>
        <div style="margin-top:1.2rem;max-width:560px;margin-left:auto;margin-right:auto;padding:0.8rem 1rem;border-top:1px solid rgba(255,241,229,0.12)">
            <p style="font-size:0.7rem;color:rgba(255,241,229,0.55);line-height:1.7;text-align:center;margin:0">
                <strong style="color:rgba(255,241,229,0.7);letter-spacing:0.08em;text-transform:uppercase;font-size:0.65rem">Disclaimer</strong><br>
                You are welcome to view these pages. The trading algorithms and analysis presented here are experimental and under active development. Nothing on this site constitutes financial advice. We accept no responsibility for any losses incurred from acting on information found here. These pages are intended for internal research purposes. You are strongly advised to conduct your own due diligence before making any investment decisions.
            </p>
        </div>
    </div>
</div>

</body>
</html>""".format(
        now_str=now_str,
        headline=headline,
        alpha_measured=s.get("alpha_measured", 0),
        active=s["active_count"],
        active_s="s" if s["active_count"] != 1 else "",
        pipeline=s["pipeline_count"],
        alpha_total_pnl=s.get("alpha_total_pnl", 0),
        alpha_win_rate=s.get("alpha_win_rate", 0),
        alpha_profit_factor=s.get("alpha_profit_factor", 0),
        alpha_total_sign=alpha_total_sign,
        research_measured=s.get("research_measured", 0),
        backtest_card=backtest_card,
        active_section=active_section,
        pipeline_section=pipeline_section,
        research_section=research_section,
        band_cards=band_cards,
        exit_timing=exit_timing,
        learning=learning,
        total=s["total_candidates"],
        killed=s["killed_count"],
    )

    # Save
    latest_path = os.path.join(REPORTS_DIR, "latest.html")
    with open(latest_path, "w") as f:
        f.write(html)

    # Also save timestamped version
    ts_name = "hedgefund_report_{}.html".format(now.strftime("%Y-%m-%d_%H%M"))
    ts_path = os.path.join(REPORTS_DIR, ts_name)
    with open(ts_path, "w") as f:
        f.write(html)

    logger.info("Report generated: {}".format(latest_path))
    return latest_path


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    from db import init_db
    init_db()
    path = generate_html_report()
    print("Report generated: {}".format(path))
