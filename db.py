"""
Hedge Fund Edge Tracker - Database Layer
SQLite database for tracking positions, prices, and due diligence.
"""
import sqlite3
import os
from datetime import datetime, timezone

from config import DB_PATH, DATA_DIR


def get_conn():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS reports (
        report_id TEXT PRIMARY KEY,
        title TEXT,
        report_url TEXT,
        generated_date TEXT,
        published_date TEXT,
        cycle_id TEXT,
        market_regime TEXT,
        bull_wind INTEGER,
        bear_wind INTEGER,
        crosswind_risk INTEGER,
        spy_price REAL,
        spy_change REAL,
        total_positions INTEGER,
        trade_count INTEGER,
        avoid_count INTEGER,
        avg_confidence REAL,
        rss_guid TEXT UNIQUE,
        ingested_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS candidates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_id TEXT,
        rank INTEGER,

        asset_theme TEXT,
        tickers TEXT,
        primary_ticker TEXT,
        prices_at_report TEXT,
        price_changes_at_report TEXT,

        direction TEXT,
        confidence_pct REAL,
        trade_confidence_score REAL,
        edge_quality TEXT,
        freshness_score REAL,
        propagation TEXT,
        action TEXT,

        headline TEXT,
        mechanism TEXT,
        tripwire TEXT,
        evidence TEXT,
        risks TEXT,

        band TEXT,
        band_label TEXT,

        state TEXT DEFAULT 'PENDING',
        state_reason TEXT,
        state_changed_at TEXT,

        entry_price REAL,
        entry_time TEXT,
        entry_method TEXT,

        watch_price_target REAL,
        watch_conditions TEXT,
        watch_checks INTEGER DEFAULT 0,

        killed_at TEXT,
        kill_reason TEXT,
        killed_by TEXT,

        discovered_at TEXT DEFAULT (datetime('now')),
        tracking_until TEXT,
        is_active INTEGER DEFAULT 1,

        last_dd_at TEXT,
        dd_count INTEGER DEFAULT 0,

        confirmations INTEGER DEFAULT 1,
        last_confirmed_by TEXT,
        last_confirmed_at TEXT,
        momentum_notes TEXT,

        FOREIGN KEY (report_id) REFERENCES reports(report_id)
    );

    CREATE TABLE IF NOT EXISTS price_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        candidate_id INTEGER NOT NULL,
        timestamp TEXT NOT NULL,
        price REAL,
        open_price REAL,
        high REAL,
        low REAL,
        volume REAL,
        change_pct REAL,
        hours_since_discovery REAL,
        hours_since_entry REAL,
        pnl_pct REAL,
        FOREIGN KEY (candidate_id) REFERENCES candidates(id)
    );

    CREATE TABLE IF NOT EXISTS dd_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        candidate_id INTEGER,
        timestamp TEXT DEFAULT (datetime('now')),
        dd_type TEXT,
        staleness_hours REAL,
        price_at_check REAL,
        price_move_since_report REAL,
        news_found TEXT,
        thesis_still_valid INTEGER,
        decision TEXT,
        decision_reason TEXT,
        llm_analysis TEXT,
        FOREIGN KEY (candidate_id) REFERENCES candidates(id)
    );

    CREATE TABLE IF NOT EXISTS trader_journal (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        candidate_id INTEGER NOT NULL,
        cycle_number INTEGER NOT NULL,
        timestamp TEXT DEFAULT (datetime('now')),
        hours_since_entry REAL,
        price_at_review REAL,
        pnl_pct REAL,
        peak_gain_pct REAL,
        max_drawdown_pct REAL,

        decision TEXT,
        conviction_score INTEGER,
        conviction_change TEXT,
        thesis_status TEXT,

        situation_summary TEXT,
        what_changed TEXT,
        watching_for TEXT,
        concerns TEXT,
        would_sell_if TEXT,
        would_hold_if TEXT,
        narrative TEXT,

        risk_level TEXT,
        time_pressure TEXT,
        llm_raw TEXT,

        FOREIGN KEY (candidate_id) REFERENCES candidates(id)
    );

    CREATE TABLE IF NOT EXISTS signal_scans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        candidate_id INTEGER NOT NULL,
        timestamp TEXT DEFAULT (datetime('now')),
        source TEXT,
        article_title TEXT,
        article_url TEXT UNIQUE,
        article_source TEXT,
        published_at TEXT,
        sentiment_score REAL,
        relevance_score REAL,
        is_signal_hit INTEGER DEFAULT 0,
        FOREIGN KEY (candidate_id) REFERENCES candidates(id)
    );

    CREATE INDEX IF NOT EXISTS idx_snapshots_candidate
        ON price_snapshots(candidate_id, timestamp);
    CREATE INDEX IF NOT EXISTS idx_candidates_active
        ON candidates(is_active);
    CREATE INDEX IF NOT EXISTS idx_candidates_state
        ON candidates(state);
    CREATE INDEX IF NOT EXISTS idx_candidates_report
        ON candidates(report_id);
    CREATE INDEX IF NOT EXISTS idx_journal_candidate
        ON trader_journal(candidate_id, cycle_number);
    CREATE INDEX IF NOT EXISTS idx_signal_scans_candidate
        ON signal_scans(candidate_id, timestamp);
    """)

    # Add PUBLISH columns if they don't exist yet (safe migration)
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(candidates)").fetchall()}
    new_cols = [
        ("publish_angle", "TEXT"),
        ("publish_headline", "TEXT"),
        ("published_at", "TEXT"),
        ("published_url", "TEXT"),
        ("pre_publish_price", "REAL"),
        ("post_publish_price", "REAL"),
        ("publish_impact_pct", "REAL"),
        # Position monitor columns
        ("last_monitor_at", "TEXT"),
        ("monitor_count", "INTEGER DEFAULT 0"),
        ("current_conviction", "INTEGER"),
        ("exit_price", "REAL"),
        ("exit_time", "TEXT"),
        ("exit_reason", "TEXT"),
        ("exit_pnl_pct", "REAL"),
        ("total_held_hours", "REAL"),
        # Signal hunting columns
        ("signal_query", "TEXT"),
        ("last_signal_scan_at", "TEXT"),
        ("signal_hits_24h", "INTEGER DEFAULT 0"),
        ("signal_velocity", "TEXT DEFAULT 'quiet'"),
        # Stalking mode: DD approves but bot watches before entering
        ("dd_approved_price", "REAL"),
        ("dd_approved_at", "TEXT"),
    ]
    for col_name, col_type in new_cols:
        if col_name not in existing_cols:
            conn.execute("ALTER TABLE candidates ADD COLUMN {} {}".format(col_name, col_type))

    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()
    print("Database initialised at {}".format(DB_PATH))
