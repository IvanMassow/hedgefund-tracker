"""
Hedge Fund Edge Tracker - Configuration
"""
import os

# RSS feeds for Information Asymmetry reports
# Add new feeds here as new publications come online
RSS_FEEDS = [
    "https://pharmarisk.makes.news/gb/en/section/697b9f8fd35a8f8b7090b851/rss.xml",
    "https://semiconductors.makes.news/rss.xml",
]
# Legacy single-URL (kept for backwards compatibility)
RSS_URL = RSS_FEEDS[0]

# Only process reports whose title starts with this prefix
REPORT_TITLE_PREFIX = "Information Asymmetry"

# Alpha Vantage
ALPHA_VANTAGE_KEY = os.environ.get("ALPHA_VANTAGE_KEY", "")
ALPHA_VANTAGE_BASE = "https://www.alphavantage.co/query"
AV_RATE_LIMIT = 12  # seconds between calls (5/min on free tier)

# OpenAI
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

# Intervals (seconds)
SCAN_INTERVAL = 30 * 60        # Check RSS every 30 minutes
TRACK_INTERVAL = 60 * 60       # Fetch prices every 60 minutes
DD_INTERVAL = 2 * 60 * 60      # Re-check WATCH positions + process PENDING every 2 hours
MONITOR_INTERVAL = 60 * 60     # Position monitor reviews every 1 hour (think like a trader)
SIGNAL_SCAN_INTERVAL = 60 * 60 # Signal propagation scan every 1 hour
REPORT_INTERVAL = 6 * 60 * 60  # Heartbeat report every 6 hours

# Trading windows (UTC hours) — NYSE opens 14:30 UTC (9:30 ET)
MARKET_OPEN_UTC = 14.5   # 14:30
MARKET_CLOSE_UTC = 21.0  # 21:00
MARKET_DAYS = [0, 1, 2, 3, 4]  # Monday-Friday

# Tracking
TRACKING_WINDOW_HOURS = 96  # 4 days
MAX_WATCH_CHECKS = 5        # auto-kill after this many failed watch checks
KILL_DISPLAY_HOURS = 48     # Show killed positions on trading sheet for 48h, then fade out

# Confidence bands
BANDS = {
    "A": {"min": 65, "max": 100, "label": "Blue Chip", "color": "#166534", "bg": "#dcfce7"},
    "B": {"min": 55, "max": 64, "label": "Bread & Butter", "color": "#92400e", "bg": "#fef3c7"},
    "C": {"min": 45, "max": 54, "label": "Contested", "color": "#6b5b00", "bg": "#fef9e7"},
    "D": {"min": 35, "max": 44, "label": "Dark Horse", "color": "#7a5c99", "bg": "#f3e8ff"},
    "E": {"min": 0, "max": 34, "label": "Frontier Scout", "color": "#73788a", "bg": "#f1f5f9"},
}

# Alpha Group formula — signals that conform to our trading rules
# These are the grading criteria that back-testing showed produce consistent profits
ALPHA_DIRECTIONS = ["LONG"]       # Only LONG positions (SHORTs lose systematically)
ALPHA_BANDS = ["A", "B"]          # Band A (65%+) and B (55-64%) — high confidence only
ALPHA_FORMULA_DESC = "LONG direction + Band A/B (55%+ confidence)"

# Staleness thresholds (hours)
STALENESS_LOW = 6       # Still fresh
STALENESS_MEDIUM = 24   # Moderate concern
STALENESS_HIGH = 48     # Significant decay risk
STALENESS_CRITICAL = 72 # Very likely stale

# Price movement thresholds for DD (percentage)
PRICE_MOVE_SMALL = 2.0   # Within noise
PRICE_MOVE_MEDIUM = 5.0  # Noteworthy
PRICE_MOVE_LARGE = 10.0  # Edge may be captured or thesis wrong

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
DB_PATH = os.path.join(DATA_DIR, "hedgefund.db")
