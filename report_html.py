"""
Hedge Fund Edge Tracker - HTML Report Generator
Noah Pink design system with three-act structure:
1. Trading Sheet  2. Confidence Clusters  3. Learning Dashboard
"""
import os
import logging
from datetime import datetime, timezone

from analytics import generate_analytics
from config import REPORTS_DIR, BANDS, KILL_DISPLAY_HOURS

logger = logging.getLogger("hedgefund.report")


def _band_color(band):
    return BANDS.get(band, BANDS["E"])["color"]

def _band_bg(band):
    return BANDS.get(band, BANDS["E"])["bg"]

def _state_color(state):
    return {
        "ACTIVE": "#16a34a", "WATCH": "#7c3aed", "KILLED": "#5b21b6",
        "PUBLISH": "#d97706", "PENDING": "#9ea2b0", "EXPIRED": "#c4c8d4",
    }.get(state, "#9ea2b0")

def _state_bg(state):
    return {
        "ACTIVE": "#dcfce7", "WATCH": "#f3e8ff", "KILLED": "#ede9fe",
        "PUBLISH": "#fef3c7", "PENDING": "#f1f5f9", "EXPIRED": "#f8f9fa",
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


def generate_html_report():
    """Generate the full HTML report with Noah Pink design."""
    os.makedirs(REPORTS_DIR, exist_ok=True)

    data = generate_analytics()
    s = data["summary"]
    now = datetime.now(timezone.utc)
    now_str = now.strftime("%Y-%m-%d %H:%M UTC")

    # Build candidate rows for trading sheet
    trading_rows = _build_trading_rows(data["candidates"])

    # Build band cluster cards
    band_cards = _build_band_cards(data["band_performance"])

    # Build learning dashboard
    learning = _build_learning_dashboard(data)

    # Hero stats
    pnl_sign = "+" if s["total_pnl"] >= 0 else ""
    headline = _dynamic_headline(s)

    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Hedge Fund Edge Tracker</title>
<!-- Open Graph / Social sharing preview -->
<meta property="og:type" content="website">
<meta property="og:title" content="NOAH Hedge Fund Edge Tracker">
<meta property="og:description" content="Information asymmetry intelligence. Paper trading hedge fund recommendations to learn which signals work.">
<meta property="og:image" content="https://ivanmassow.github.io/hedgefund-tracker/og-image.png">
<meta property="og:url" content="https://ivanmassow.github.io/hedgefund-tracker/">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="NOAH Hedge Fund Edge Tracker">
<meta name="twitter:description" content="Information asymmetry intelligence. Paper trading hedge fund recommendations to learn which signals work.">
<meta name="twitter:image" content="https://ivanmassow.github.io/hedgefund-tracker/og-image.png">
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
    color: var(--grey-300); max-width: 600px; margin-bottom: 1.5rem;
}}
.stat-grid {{
    display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
    gap: 1.5rem; margin-top: 1rem;
}}
.stat-box .num {{
    font-family: 'Playfair Display', serif;
    font-size: clamp(2rem, 5vw, 3rem); font-weight: 700;
}}
.stat-box .label {{
    font-size: 0.72rem; letter-spacing: 0.06em;
    text-transform: uppercase; color: var(--grey-400);
}}

/* Sections */
.section {{ padding: 3rem 0; scroll-margin-top: 72px; }}
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

/* Trading Sheet Table */
.table-scroll {{
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
}}
.trading-table {{
    width: 100%; border-collapse: collapse;
    font-size: 0.85rem;
}}
.trading-table thead th {{
    background: var(--paper); padding: 0.7rem 0.6rem;
    text-align: left; font-weight: 700;
    font-size: 0.72rem; letter-spacing: 0.06em;
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
    padding: 0.55rem 0.6rem; vertical-align: middle;
}}
.td-discovered {{ font-size: 0.78rem; color: var(--ink-subtle); white-space: nowrap; }}
.td-band {{
    font-family: 'Playfair Display', serif;
    font-weight: 700; font-size: 1rem; text-align: center;
    width: 2.5rem;
}}
.td-asset {{ max-width: 220px; }}
.td-asset .name {{ font-weight: 700; font-size: 0.88rem; }}
.td-asset .ticker {{ font-size: 0.75rem; color: var(--ink-subtle); }}
.td-asset .thesis {{ font-size: 0.72rem; color: var(--ink-mid); font-style: italic; margin-top: 2px; }}
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

/* Journal / Conviction display */
.conviction-gauge {{
    display: inline-flex; align-items: center; gap: 4px;
    font-size: 0.75rem; font-weight: 700;
}}
.conviction-bar {{
    display: inline-block; width: 50px; height: 6px;
    background: var(--grey-200); border-radius: 3px;
    overflow: hidden; vertical-align: middle;
}}
.conviction-fill {{
    height: 100%; border-radius: 3px;
    transition: width 0.3s;
}}
.thesis-badge {{
    display: inline-block; font-size: 0.65rem; font-weight: 700;
    letter-spacing: 0.03em; text-transform: uppercase;
    padding: 1px 6px; border-radius: 3px;
    margin-left: 4px;
}}
.journal-block {{
    margin-top: 6px; padding: 6px 8px;
    background: rgba(255,255,255,0.6); border-radius: 4px;
    border-left: 3px solid var(--grey-300);
    font-size: 0.72rem; color: var(--ink-mid);
}}
.journal-block .journal-watching {{
    color: var(--accent); font-weight: 700; margin-bottom: 2px;
}}
.journal-block .journal-concerns {{
    color: #b45309; font-style: italic; margin-bottom: 2px;
}}
.journal-block .journal-narrative {{
    color: var(--ink-light); line-height: 1.4;
}}
.journal-block .journal-meta {{
    font-size: 0.65rem; color: var(--grey-400); margin-top: 3px;
}}
.watch-marker {{
    display: inline-block; background: var(--purple);
    color: #fff; font-weight: 700; font-size: 0.65rem;
    width: 16px; height: 16px; line-height: 16px;
    text-align: center; border-radius: 50%; margin: 0 2px;
}}

/* Killed/Watch rows */
.killed-row {{ opacity: 0.7; }}
.killed-row .td-asset .name {{ color: var(--purple-dark); }}
.killed-row .td-discovered {{ color: var(--purple-dark); }}
.watch-row {{ }}
.watch-row .td-asset .name {{ color: var(--purple); }}
.publish-row {{ background: #fffbeb; border-left: 3px solid #d97706; }}
.publish-row .td-asset .name {{ color: #92400e; }}
.publish-row .td-state {{ font-weight: 700; }}

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
    padding: 0.3rem 0; border-bottom: 1px solid var(--grey-200);
}}
.learn-card td {{
    padding: 0.3rem 0; border-bottom: 1px solid var(--grey-100);
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
                <div class="num">{total}</div>
                <div class="label">Positions</div>
            </div>
            <div class="stat-box">
                <div class="num">{active}</div>
                <div class="label">Active</div>
            </div>
            <div class="stat-box">
                <div class="num">{watch}</div>
                <div class="label">Watching</div>
            </div>
            <div class="stat-box">
                <div class="num">{pnl_sign}{total_pnl:.1f}%</div>
                <div class="label">Total P&amp;L</div>
            </div>
            <div class="stat-box">
                <div class="num">{win_rate:.0f}%</div>
                <div class="label">Win Rate</div>
            </div>
            <div class="stat-box">
                <div class="num">{killed}</div>
                <div class="label">Killed</div>
            </div>
        </div>
    </div>
</div>

<!-- Trading Sheet -->
<div class="section" id="sheet">
    <div class="container">
        <div class="section-label">Act I</div>
        <div class="section-title">Trading Sheet</div>
        <div class="section-intro">All positions scored, ranked, and colour-coded. Active trades in green, watched positions in purple, kills in dark purple. Scan in 10 seconds.</div>
        <div class="table-scroll">
            <table class="trading-table">
                <thead>
                    <tr>
                        <th>Discovered</th>
                        <th>Band</th>
                        <th>Asset / Theme</th>
                        <th>Dir</th>
                        <th>Conf</th>
                        <th>Entry</th>
                        <th>Current</th>
                        <th>P&amp;L</th>
                        <th>Timeline</th>
                        <th>State</th>
                    </tr>
                </thead>
                <tbody>
                    {trading_rows}
                </tbody>
            </table>
        </div>
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
        <div class="section-intro">What is the system learning? Edge quality, direction accuracy, kill validation, and optimal timing by confidence band.</div>
        <div class="learn-grid">
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
            Report generated {now_str}.
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
        total=s["total_candidates"],
        active=s["active_count"],
        watch=s["watch_count"],
        killed=s["killed_count"],
        pnl_sign=pnl_sign,
        total_pnl=s["total_pnl"],
        win_rate=s["win_rate"],
        trading_rows=trading_rows,
        band_cards=band_cards,
        learning=learning,
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


def _dynamic_headline(s):
    """Generate dynamic headline based on portfolio performance."""
    if s["total_candidates"] == 0:
        return "No positions yet. Awaiting first report."
    if s["active_count"] == 0 and s["pending_count"] > 0:
        return "New recommendations pending due diligence. The trader is assessing."
    if s["win_rate"] >= 70:
        return "Strong performance. {}% win rate across {} traded positions.".format(
            s["win_rate"], s["active_count"] + s["killed_count"]
        )
    if s["total_pnl"] > 0:
        return "Portfolio in the green. {} active positions, {} under watch.".format(
            s["active_count"], s["watch_count"]
        )
    if s["watch_count"] > s["active_count"]:
        return "Cautious posture. More positions under watch than active. The trader is waiting for better entries."
    return "Tracking {} positions across {} confidence bands.".format(
        s["total_candidates"], len([b for b in ["A","B","C","D","E"]
                                    if any(m.get("band") == b for m in [])])  # simplified
    )


def _build_trading_rows(candidates):
    """Build HTML table rows for the trading sheet.
    Killed positions older than KILL_DISPLAY_HOURS are hidden from the sheet
    but still counted in analytics/learning.
    """
    if not candidates:
        return '<tr><td colspan="10" style="text-align:center;color:var(--grey-400);padding:2rem;font-style:italic;">No positions yet. Awaiting reports.</td></tr>'

    # Filter out old kills
    now = datetime.now(timezone.utc)
    visible = []
    hidden_kills = 0
    for m in candidates:
        if m["state"] == "KILLED" and m.get("killed_at"):
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
        visible.append(m)

    # Sort: PUBLISH first, then ACTIVE, then WATCH, then PENDING, then KILLED
    state_order = {"PUBLISH": 0, "ACTIVE": 1, "WATCH": 2, "PENDING": 3, "KILLED": 4, "EXPIRED": 5}
    sorted_c = sorted(visible, key=lambda m: (
        state_order.get(m["state"], 6), -(m.get("confidence_pct") or 0)
    ))

    rows = []
    for m in sorted_c:
        row_class = ""
        if m["state"] == "KILLED":
            row_class = ' class="killed-row"'
        elif m["state"] == "WATCH":
            row_class = ' class="watch-row"'
        elif m["state"] == "PUBLISH":
            row_class = ' class="publish-row"'

        # Discovered
        disc = m.get("discovered_at", "")[:10]

        # Band
        band = m.get("band", "E")
        bc = _band_color(band)

        # Asset name + thesis
        asset = m.get("asset_theme", "Unknown")[:50]
        ticker = m.get("primary_ticker", "")
        thesis = (m.get("headline") or m.get("mechanism") or "")[:80]
        state_reason = m.get("state_reason", "")

        # Direction badge
        direction = m.get("direction", "MIXED")
        dc = _direction_color(direction)
        db = _direction_bg(direction)

        # Confidence
        conf = m.get("confidence_pct", 0)

        # Prices
        entry = m.get("entry_price")
        current = m.get("current_price")
        pnl = m.get("current_pnl")

        entry_str = "${:.2f}".format(entry) if entry else "---"
        current_str = "${:.2f}".format(current) if current else "---"
        if pnl is not None:
            pnl_sign = "+" if pnl >= 0 else ""
            pnl_color = "#16a34a" if pnl >= 0 else "#cc0000"
            pnl_str = '<span style="color:{}">{}{:.1f}%</span>'.format(pnl_color, pnl_sign, pnl)
        else:
            pnl_str = "---"

        # Timeline
        timeline_html = _build_timeline_cells(m)

        # State badge
        state = m["state"]
        sc = _state_color(state)
        sb = _state_bg(state)

        # State-specific note
        note = ""
        if state == "PUBLISH":
            pub_headline = m.get("publish_headline") or ""
            pub_angle = m.get("publish_angle") or ""
            pub_text = pub_headline or pub_angle
            if pub_text:
                note = '<div style="font-size:0.7rem;color:#d97706;font-weight:700;margin-top:2px;">&#9998; {}</div>'.format(
                    pub_text[:80]
                )
            else:
                note = '<div style="font-size:0.7rem;color:#d97706;font-weight:700;margin-top:2px;">&#9998; Editorial candidate</div>'
        elif state == "KILLED" and m.get("kill_reason"):
            note = '<div style="font-size:0.7rem;color:var(--purple-dark);font-style:italic;opacity:0.85;margin-top:2px;">{}</div>'.format(
                m["kill_reason"][:60]
            )
        elif state == "WATCH" and state_reason:
            note = '<div style="font-size:0.7rem;color:var(--purple);font-style:italic;margin-top:2px;">{}</div>'.format(
                state_reason[:60]
            )

        # Journal / conviction display for active positions
        journal_html = ""
        if state in ("ACTIVE", "PUBLISH") and m.get("latest_conviction"):
            conv = m["latest_conviction"]
            conv_pct = min(conv * 10, 100)
            if conv >= 7:
                conv_color = "#16a34a"
            elif conv >= 5:
                conv_color = "#f59e0b"
            elif conv >= 3:
                conv_color = "#ea580c"
            else:
                conv_color = "#cc0000"

            thesis_st = m.get("latest_thesis_status", "")
            thesis_colors = {
                "intact": ("#166534", "#dcfce7"),
                "strengthening": ("#065f46", "#d1fae5"),
                "weakening": ("#92400e", "#fef3c7"),
                "invalidated": ("#991b1b", "#fef2f2"),
            }
            tc, tbg = thesis_colors.get(thesis_st, ("#73788a", "#f1f5f9"))
            thesis_badge = ""
            if thesis_st:
                thesis_badge = '<span class="thesis-badge" style="color:{};background:{}">{}</span>'.format(
                    tc, tbg, thesis_st
                )

            journal_html = '<div style="margin-top:4px;">'
            journal_html += '<span class="conviction-gauge">'
            journal_html += '<span class="conviction-bar"><span class="conviction-fill" style="width:{}%;background:{}"></span></span>'.format(
                conv_pct, conv_color)
            journal_html += ' <span style="color:{}">{}/10</span>'.format(conv_color, conv)
            journal_html += '</span>'
            journal_html += thesis_badge
            journal_html += '</div>'

            # Journal details block
            watching = m.get("latest_watching_for", "")
            concerns = m.get("latest_concerns", "")
            narratives = m.get("latest_narrative_entries", [])

            if watching or concerns or narratives:
                journal_html += '<div class="journal-block">'
                if watching:
                    journal_html += '<div class="journal-watching">&#128269; {}</div>'.format(
                        watching[:120])
                if concerns:
                    journal_html += '<div class="journal-concerns">&#9888; {}</div>'.format(
                        concerns[:120])
                if narratives:
                    latest = narratives[0]
                    journal_html += '<div class="journal-narrative">{}</div>'.format(
                        latest["narrative"][:200])
                    journal_html += '<div class="journal-meta">Cycle {} &middot; {}</div>'.format(
                        latest["cycle"], latest["timestamp"])
                journal_html += '</div>'

        rows.append("""<tr{row_class}>
    <td class="td-discovered">{disc}</td>
    <td class="td-band" style="color:{bc}">{band}</td>
    <td class="td-asset">
        <div class="name">{asset}</div>
        <div class="ticker">{ticker}</div>
        <div class="thesis">{thesis}</div>
        {note}
        {journal_html}
    </td>
    <td><span class="td-dir" style="color:{dc};background:{db}">{direction}</span></td>
    <td style="text-align:center;font-weight:700">{conf:.0f}%</td>
    <td class="td-price">{entry_str}</td>
    <td class="td-price">{current_str}</td>
    <td class="td-pnl">{pnl_str}</td>
    {timeline_html}
    <td><span class="td-state" style="color:{sc};background:{sb}">{state}</span></td>
</tr>""".format(
            row_class=row_class, disc=disc, bc=bc, band=band,
            asset=asset, ticker=ticker, thesis=thesis, note=note,
            journal_html=journal_html,
            dc=dc, db=db, direction=direction, conf=conf,
            entry_str=entry_str, current_str=current_str,
            pnl_str=pnl_str, timeline_html=timeline_html,
            sc=sc, sb=sb, state=state
        ))

    # Add a note about hidden kills if any
    if hidden_kills > 0:
        rows.append(
            '<tr><td colspan="10" style="text-align:center;color:var(--grey-400);'
            'padding:0.8rem;font-size:0.78rem;font-style:italic;border:none;">'
            '{} killed position{} older than {}h removed from view '
            '&mdash; still counted in learning analytics</td></tr>'.format(
                hidden_kills,
                "s" if hidden_kills != 1 else "",
                int(KILL_DISPLAY_HOURS)
            )
        )

    return "\n".join(rows)


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
            mem_pnl = mem.get("current_pnl")
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
        <div><span class="num">{traded}</span> traded</div>
        <div><span class="num">{wr:.0f}%</span> win rate</div>
        <div><span class="num" style="color:{pc}">{ps}{pnl:.1f}%</span> avg P&amp;L</div>
    </div>
    <div class="members">{members}</div>
</div>""".format(
            bc=bc, band=band_key, label=label,
            count=bp["count"], traded=bp.get("traded_count", 0),
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


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    from db import init_db
    init_db()
    path = generate_html_report()
    print("Report generated: {}".format(path))
