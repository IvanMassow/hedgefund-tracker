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
from report_html import generate_html_report
from analytics import generate_claude_briefing
import llm_trader
from config import (
    SCAN_INTERVAL, TRACK_INTERVAL, DD_INTERVAL, REPORT_INTERVAL,
    REPORTS_DIR, LOGS_DIR
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


def push_to_github():
    """Copy latest.html to index.html and push to GitHub Pages."""
    try:
        latest = os.path.join(REPORTS_DIR, "latest.html")
        index = os.path.join(REPORTS_DIR, "index.html")
        if not os.path.exists(latest):
            logger.warning("No latest.html to push")
            return False
        shutil.copy2(latest, index)

        # Check if we're in a git repo
        result = subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            cwd=REPORTS_DIR, capture_output=True, timeout=10
        )
        if result.returncode != 0:
            # Not a git repo in reports — try project root
            project_root = os.path.dirname(REPORTS_DIR)
            result = subprocess.run(
                ["git", "rev-parse", "--is-inside-work-tree"],
                cwd=project_root, capture_output=True, timeout=10
            )
            if result.returncode != 0:
                logger.debug("Not in a git repo, skipping push")
                return False
            git_cwd = project_root
        else:
            git_cwd = REPORTS_DIR

        subprocess.run(
            ["git", "add", "reports/latest.html", "reports/index.html"],
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
    logger.info("  Scan: {}min | Track: {}min | DD: {}h | Report: {}h".format(
        SCAN_INTERVAL // 60, TRACK_INTERVAL // 60,
        DD_INTERVAL // 3600, REPORT_INTERVAL // 3600
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

        # Re-check watched positions (during market hours only)
        if now - last_dd >= DD_INTERVAL and is_market_open():
            try:
                rechecked = recheck_watched(llm_trader)
                last_dd = now
                if rechecked > 0:
                    try:
                        path = generate_html_report()
                        last_report = now
                        push_to_github()
                    except Exception as e:
                        logger.error("Post-DD report error: {}".format(e), exc_info=True)
            except Exception as e:
                logger.error("DD recheck error: {}".format(e), exc_info=True)

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
