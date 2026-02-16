"""
Hedge Fund Edge Tracker - Runner
Main daemon that orchestrates scanning, trading, tracking, and reporting.
"""
import os
import sys
import time
import logging
import signal
import shutil
import subprocess
from datetime import datetime, timezone, timedelta

from db import init_db
from scanner import scan
from tracker import track_prices
from trader import process_pending_candidates, recheck_watched, is_market_open
from position_monitor import run_position_monitoring
from signal_hunter import run_signal_scan
from report_html import generate_html_report
from analytics import generate_claude_briefing
import llm_trader
from config import (
    SCAN_INTERVAL, TRACK_INTERVAL, DD_INTERVAL, MONITOR_INTERVAL,
    SIGNAL_SCAN_INTERVAL, REPORT_INTERVAL, REPORTS_DIR, LOGS_DIR
)

# Configure logging
os.makedirs(LOGS_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "hedgefund.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("hedgefund.runner")

# State
running = True


def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received. Stopping gracefully...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def export_dashboard_json():
    """Export a lightweight JSON summary for the Noah Dashboard."""
    import json
    from analytics import generate_analytics
    try:
        data = generate_analytics()
        s = data["summary"]
        now = datetime.now(timezone.utc)

        # All recent positions for the dashboard (up to 10, prioritised)
        # Show PUBLISH first, then ACTIVE, WATCH, PENDING, recent KILLED
        recent_positions = []
        state_order = {"PUBLISH": 0, "ACTIVE": 1, "WATCH": 2, "PENDING": 3, "KILLED": 4, "EXPIRED": 5}
        sorted_candidates = sorted(
            data["candidates"],
            key=lambda m: (state_order.get(m.get("state", "EXPIRED"), 5), m.get("discovered_at", ""))
        )
        for m in sorted_candidates:
            if m.get("state") == "EXPIRED":
                continue
            recent_positions.append({
                "asset": (m.get("asset_theme") or "?")[:50],
                "ticker": m.get("primary_ticker", "?"),
                "direction": m.get("direction", "?"),
                "confidence": m.get("confidence_pct", 0),
                "band": m.get("band", "E"),
                "entry_price": m.get("entry_price"),
                "current_pnl": m.get("current_pnl"),
                "state": m.get("state", "PENDING"),
                "headline": (m.get("headline") or m.get("mechanism") or "")[:80],
                "publish_headline": (m.get("publish_headline") or "")[:80],
                "kill_reason": (m.get("kill_reason") or "")[:60],
                "state_reason": (m.get("state_reason") or "")[:60],
                # Journal metadata
                "conviction": m.get("latest_conviction"),
                "thesis_status": m.get("latest_thesis_status"),
                "watching_for": (m.get("latest_watching_for") or "")[:80],
                "journal_count": m.get("journal_count", 0),
                # Signal hunting metadata
                "signal_velocity": m.get("signal_velocity", "quiet"),
                "signal_hits_24h": m.get("signal_hits_24h", 0),
            })
            if len(recent_positions) >= 10:
                break

        summary_json = {
            "system": "hedgefund",
            "title": "Hedge Fund Edge Tracker",
            "updated_at": now.isoformat(),
            "total_positions": s.get("total_candidates", 0),
            "active_count": s.get("active_count", 0),
            "publish_count": s.get("publish_count", 0),
            "watch_count": s.get("watch_count", 0),
            "killed_count": s.get("killed_count", 0),
            "pending_count": s.get("pending_count", 0),
            "total_pnl": s.get("total_pnl", 0),
            "win_rate": s.get("win_rate", 0),
            "best_trade": s.get("best_trade", 0),
            "worst_trade": s.get("worst_trade", 0),
            "short_count": s.get("short_count", 0),
            "long_count": s.get("long_count", 0),
            "recent_positions": recent_positions,
            "report_url": "https://ivanmassow.github.io/hedgefund-tracker/",
        }

        project_root = os.path.dirname(REPORTS_DIR)
        json_path = os.path.join(project_root, "summary.json")
        with open(json_path, "w") as f:
            json.dump(summary_json, f, indent=2)
        logger.info("Dashboard JSON exported to {}".format(json_path))
        return json_path
    except Exception as e:
        logger.warning("Failed to export dashboard JSON: {}".format(e))
        return None


def push_to_github():
    """Copy latest.html to index.html at project root and push to GitHub Pages."""
    try:
        latest = os.path.join(REPORTS_DIR, "latest.html")
        project_root = os.path.dirname(REPORTS_DIR)
        index = os.path.join(project_root, "index.html")
        if not os.path.exists(latest):
            logger.warning("No latest.html to push")
            return False
        shutil.copy2(latest, index)

        # Also export dashboard JSON
        export_dashboard_json()

        # Check if we're in a git repo
        result = subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            cwd=project_root, capture_output=True, timeout=10
        )
        if result.returncode != 0:
            logger.debug("Not in a git repo, skipping push")
            return False
        git_cwd = project_root

        subprocess.run(
            ["git", "add", "index.html", "reports/latest.html", "summary.json"],
            cwd=git_cwd, capture_output=True, check=True
        )
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        subprocess.run(
            ["git", "commit", "-m", "Update report " + now_str],
            cwd=git_cwd, capture_output=True, check=True
        )
        result = subprocess.run(
            ["git", "push"],
            cwd=git_cwd, capture_output=True, timeout=30
        )
        if result.returncode == 0:
            logger.info("Report pushed to GitHub Pages")
            return True
        else:
            logger.warning("Git push failed: " + result.stderr.decode()[:200])
            return False
    except subprocess.CalledProcessError as e:
        if "nothing to commit" in (e.stderr or b"").decode():
            logger.debug("No changes to push")
            return True
        logger.warning("Git push error: " + str(e))
        return False
    except Exception as e:
        logger.warning("Push to GitHub failed: " + str(e))
        return False


def run():
    """Main run loop."""
    logger.info("=" * 60)
    logger.info("Hedge Fund Edge Tracker starting up")
    logger.info("=" * 60)

    init_db()
    logger.info("Database initialised")

    last_scan = 0
    last_track = 0
    last_dd = 0
    last_signal_scan = 0
    last_monitor = 0
    last_report = 0

    # Initial scan
    logger.info("Running initial scan...")
    try:
        new = scan()
        last_scan = time.time()
        logger.info("Initial scan: {} new reports".format(new))
    except Exception as e:
        logger.error("Initial scan failed: {}".format(e), exc_info=True)

    # Process pending candidates
    logger.info("Processing pending candidates...")
    try:
        processed = process_pending_candidates(llm_trader)
        last_dd = time.time()
        logger.info("Processed {} candidates".format(processed))
    except Exception as e:
        logger.error("Initial DD failed: {}".format(e), exc_info=True)

    # Initial price track
    logger.info("Running initial price track...")
    try:
        tracked = track_prices()
        last_track = time.time()
        logger.info("Initial track: {} prices".format(tracked))
    except Exception as e:
        logger.error("Initial track failed: {}".format(e), exc_info=True)

    # Generate initial report
    logger.info("Generating initial report...")
    try:
        path = generate_html_report()
        last_report = time.time()
        logger.info("Initial report: {}".format(path))
        push_to_github()
    except Exception as e:
        logger.error("Initial report failed: {}".format(e), exc_info=True)

    logger.info("Entering main loop. Ctrl+C to stop.")
    logger.info("  Scan: {}min | Track: {}min | DD: {}h | Signal: {}h | Monitor: {}h | Report: {}h".format(
        SCAN_INTERVAL // 60, TRACK_INTERVAL // 60,
        DD_INTERVAL // 3600, SIGNAL_SCAN_INTERVAL // 3600,
        MONITOR_INTERVAL // 3600, REPORT_INTERVAL // 3600
    ))

    while running:
        now = time.time()

        # Scan for new reports
        if now - last_scan >= SCAN_INTERVAL:
            try:
                new = scan()
                last_scan = now
                if new > 0:
                    logger.info("Found {} new report(s)".format(new))
                    # Process new candidates immediately
                    process_pending_candidates(llm_trader)
                    last_dd = now
                    # Track prices and regenerate report
                    track_prices()
                    last_track = now
                    try:
                        path = generate_html_report()
                        last_report = now
                        push_to_github()
                    except Exception as e:
                        logger.error("Post-scan report error: {}".format(e), exc_info=True)
            except Exception as e:
                logger.error("Scan error: {}".format(e), exc_info=True)

        # Track prices
        if now - last_track >= TRACK_INTERVAL:
            try:
                tracked = track_prices()
                last_track = now
                if tracked > 0:
                    try:
                        path = generate_html_report()
                        last_report = now
                        push_to_github()
                    except Exception as e:
                        logger.error("Post-track report error: {}".format(e), exc_info=True)
            except Exception as e:
                logger.error("Track error: {}".format(e), exc_info=True)

        # Process pending candidates + re-check watched (during market hours only)
        if now - last_dd >= DD_INTERVAL and is_market_open():
            try:
                # First: process any pending candidates waiting for DD
                processed = process_pending_candidates(llm_trader)
                # Then: re-check watched positions
                rechecked = recheck_watched(llm_trader)
                last_dd = now
                if processed > 0 or rechecked > 0:
                    try:
                        path = generate_html_report()
                        last_report = now
                        push_to_github()
                    except Exception as e:
                        logger.error("Post-DD report error: {}".format(e), exc_info=True)
            except Exception as e:
                logger.error("DD recheck error: {}".format(e), exc_info=True)

        # Signal hunting — search for thesis propagation evidence
        if now - last_signal_scan >= SIGNAL_SCAN_INTERVAL and is_market_open():
            try:
                scanned = run_signal_scan()
                last_signal_scan = now
                if scanned > 0:
                    logger.info("Signal scan complete: {} positions scanned".format(scanned))
            except Exception as e:
                logger.error("Signal scan error: {}".format(e), exc_info=True)

        # Position monitoring — intelligent ongoing thesis review
        if now - last_monitor >= MONITOR_INTERVAL and is_market_open():
            try:
                monitored = run_position_monitoring(llm_trader)
                last_monitor = now
                if monitored > 0:
                    try:
                        path = generate_html_report()
                        last_report = now
                        logger.info("Report regenerated after position monitoring: {}".format(path))
                        push_to_github()
                    except Exception as e:
                        logger.error("Post-monitor report error: {}".format(e), exc_info=True)
            except Exception as e:
                logger.error("Position monitoring error: {}".format(e), exc_info=True)

        # Heartbeat report
        if now - last_report >= REPORT_INTERVAL:
            try:
                path = generate_html_report()
                last_report = now
                push_to_github()
            except Exception as e:
                logger.error("Report error: {}".format(e), exc_info=True)

        # Sleep in small increments for signal responsiveness
        for _ in range(60):
            if not running:
                break
            time.sleep(1)

    logger.info("Hedge Fund Edge Tracker stopped.")


def run_once():
    """Run a single cycle (testing / manual invocation)."""
    init_db()
    logger.info("Running single cycle...")

    new = scan()
    logger.info("Scan: {} new reports".format(new))

    processed = process_pending_candidates(llm_trader)
    logger.info("DD: {} candidates processed".format(processed))

    tracked = track_prices()
    logger.info("Track: {} prices".format(tracked))

    scanned = run_signal_scan()
    logger.info("Signal scan: {} positions scanned".format(scanned))

    monitored = run_position_monitoring(llm_trader)
    logger.info("Monitor: {} positions reviewed".format(monitored))

    path = generate_html_report()
    logger.info("Report: {}".format(path))
    push_to_github()

    briefing = generate_claude_briefing()
    print("\n" + briefing)

    return path


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        run_once()
    else:
        run()
