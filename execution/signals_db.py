"""
Signals Database — SQLite storage for all signal history and backtest results.

Schema:
  volume_signals    — Module 1: volume anomaly signals
  insider_signals   — Module 2: insider trading cluster signals
  bulk_deal_signals — Module 3: bulk/block deal signals
  fii_dii_signals   — Module 5: FII/DII regime signals
  seasonality_facts — Module 6: validated seasonality patterns (static)
  composite_scores  — Final composite scores per stock per day

Usage:
    from signals_db import SignalsDB
    db = SignalsDB()
    db.insert_volume_signal(date='2026-04-04', symbol='RELIANCE', ...)
    scores = db.get_composite_scores('2026-04-04')
"""

import sqlite3
import json
import logging
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(".tmp/antigravity_signals.db")


class SignalsDB:
    """
    Thread-safe SQLite interface for all Antigravity signal storage.
    Creates schema on first use.
    """

    def __init__(self, db_path: str = None):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_schema(self):
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS volume_signals (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    date        TEXT NOT NULL,
                    symbol      TEXT NOT NULL,
                    signal_type TEXT NOT NULL,   -- STEALTH_ACCUMULATION, BREAKOUT_BUYING, etc.
                    vol_ratio   REAL,            -- today_vol / 20d_avg_vol
                    delivery_pct REAL,           -- delivery as % of traded qty
                    price_change_pct REAL,       -- day's price change %
                    close_price  REAL,
                    score       INTEGER,         -- contribution to composite (-25 to +30)
                    inserted_at TEXT DEFAULT (datetime('now'))
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_vol_date_sym ON volume_signals(date, symbol);

                CREATE TABLE IF NOT EXISTS insider_signals (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    date            TEXT NOT NULL,
                    symbol          TEXT NOT NULL,
                    signal_type     TEXT NOT NULL,  -- BUY_CLUSTER, SELL_CLUSTER, etc.
                    insider_count   INTEGER,         -- number of distinct insiders
                    insider_categories TEXT,         -- JSON list of categories
                    total_value_lakhs REAL,
                    days_window     INTEGER,         -- detection window in days
                    score           INTEGER,
                    inserted_at     TEXT DEFAULT (datetime('now'))
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_ins_date_sym ON insider_signals(date, symbol);

                CREATE TABLE IF NOT EXISTS bulk_deal_signals (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    date            TEXT NOT NULL,
                    symbol          TEXT NOT NULL,
                    signal_type     TEXT NOT NULL,   -- SYSTEMATIC_ACCUMULATION, etc.
                    client_name     TEXT,
                    deal_count      INTEGER,         -- occurrences in 30d window
                    total_qty       REAL,
                    total_value_cr  REAL,            -- value in ₹ crore
                    deal_type       TEXT,            -- BULK/BLOCK/MIXED
                    score           INTEGER,
                    inserted_at     TEXT DEFAULT (datetime('now'))
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_bulk_date_sym_client ON bulk_deal_signals(date, symbol, client_name);

                CREATE TABLE IF NOT EXISTS fii_dii_signals (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    date            TEXT NOT NULL UNIQUE,
                    fii_cash_net    REAL,
                    fii_fut_net     REAL,
                    fii_opt_net     REAL,
                    dii_cash_net    REAL,
                    composite_score REAL,           -- fii composite score for the day
                    rolling_5d_score REAL,          -- 5-day rolling sum
                    regime          TEXT,           -- BULLISH/BEARISH/NEUTRAL
                    signal_type     TEXT,
                    score           INTEGER,        -- contribution to composite (-25 to +15)
                    inserted_at     TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS seasonality_facts (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    pattern_name TEXT NOT NULL UNIQUE,
                    description TEXT,
                    pattern_type TEXT,  -- DOW / MONTH / EXPIRY_WEEK / etc.
                    day_of_week  INTEGER,  -- 0=Mon, 6=Sun (-1 if not applicable)
                    month        INTEGER,  -- 1-12 (-1 if not applicable)
                    avg_return   REAL,
                    p_value      REAL,
                    occurrences  INTEGER,
                    t_statistic  REAL,
                    score_when_active INTEGER,  -- points to add when this pattern is active
                    is_valid     INTEGER DEFAULT 1,  -- 0 if p > 0.05 or n < 50
                    validated_on TEXT,
                    inserted_at  TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS composite_scores (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    date            TEXT NOT NULL,
                    symbol          TEXT NOT NULL,
                    composite_score INTEGER,
                    vol_score       INTEGER DEFAULT 0,
                    insider_score   INTEGER DEFAULT 0,
                    bulk_score      INTEGER DEFAULT 0,
                    pairs_score     INTEGER DEFAULT 0,
                    fii_score       INTEGER DEFAULT 0,
                    seasonality_score INTEGER DEFAULT 0,
                    sentiment_score INTEGER DEFAULT 0,
                    signal_type     TEXT,    -- STRONG_BUY/BUY/WATCHLIST/NO_SIGNAL/SHORT/STRONG_SHORT
                    active_signals  TEXT,    -- JSON list of signal descriptions
                    inserted_at     TEXT DEFAULT (datetime('now'))
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_comp_date_sym ON composite_scores(date, symbol);

                CREATE TABLE IF NOT EXISTS backtest_results (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    module      TEXT NOT NULL,   -- MODULE1/MODULE2/.../COMPOSITE
                    signal_type TEXT,
                    backtest_date TEXT,
                    trade_date  TEXT,
                    symbol      TEXT,
                    entry_score INTEGER,
                    return_5d   REAL,
                    return_10d  REAL,
                    return_30d  REAL,
                    was_correct INTEGER,   -- 1 if signal direction matched return
                    inserted_at TEXT DEFAULT (datetime('now'))
                );
            """)
        logger.info(f"SignalsDB ready: {self.db_path}")

    # ─── VOLUME SIGNALS ───────────────────────────────────────────────────────

    def upsert_volume_signal(self, date: str, symbol: str, signal_type: str,
                              vol_ratio: float, delivery_pct: float,
                              price_change_pct: float, close_price: float, score: int):
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO volume_signals (date, symbol, signal_type, vol_ratio, delivery_pct,
                    price_change_pct, close_price, score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, symbol) DO UPDATE SET
                    signal_type=excluded.signal_type,
                    vol_ratio=excluded.vol_ratio,
                    delivery_pct=excluded.delivery_pct,
                    price_change_pct=excluded.price_change_pct,
                    score=excluded.score
            """, (date, symbol, signal_type, vol_ratio, delivery_pct,
                  price_change_pct, close_price, score))

    def get_volume_signals(self, date: str = None, min_score: int = None):
        with self._conn() as conn:
            q = "SELECT * FROM volume_signals WHERE 1=1"
            params = []
            if date:
                q += " AND date = ?"
                params.append(date)
            if min_score is not None:
                q += " AND score >= ?"
                params.append(min_score)
            q += " ORDER BY score DESC"
            return [dict(r) for r in conn.execute(q, params).fetchall()]

    # ─── INSIDER SIGNALS ──────────────────────────────────────────────────────

    def upsert_insider_signal(self, date: str, symbol: str, signal_type: str,
                               insider_count: int, insider_categories: list,
                               total_value_lakhs: float, days_window: int, score: int):
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO insider_signals (date, symbol, signal_type, insider_count,
                    insider_categories, total_value_lakhs, days_window, score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, symbol) DO UPDATE SET
                    signal_type=excluded.signal_type,
                    insider_count=excluded.insider_count,
                    insider_categories=excluded.insider_categories,
                    total_value_lakhs=excluded.total_value_lakhs,
                    score=excluded.score
            """, (date, symbol, signal_type, insider_count,
                  json.dumps(insider_categories), total_value_lakhs, days_window, score))

    def get_insider_signals(self, date: str = None, days_back: int = 7):
        with self._conn() as conn:
            if date:
                cutoff = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=days_back)).strftime('%Y-%m-%d')
                rows = conn.execute(
                    "SELECT * FROM insider_signals WHERE date >= ? ORDER BY score DESC", (cutoff,)
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM insider_signals ORDER BY date DESC, score DESC").fetchall()
            results = []
            for r in rows:
                d = dict(r)
                if d.get('insider_categories'):
                    try:
                        d['insider_categories'] = json.loads(d['insider_categories'])
                    except Exception:
                        pass
                results.append(d)
            return results

    # ─── BULK DEAL SIGNALS ────────────────────────────────────────────────────

    def upsert_bulk_signal(self, date: str, symbol: str, client_name: str,
                            signal_type: str, deal_count: int,
                            total_qty: float, total_value_cr: float,
                            deal_type: str, score: int):
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO bulk_deal_signals (date, symbol, signal_type, client_name,
                    deal_count, total_qty, total_value_cr, deal_type, score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, symbol, client_name) DO UPDATE SET
                    signal_type=excluded.signal_type,
                    deal_count=excluded.deal_count,
                    total_qty=excluded.total_qty,
                    total_value_cr=excluded.total_value_cr,
                    score=excluded.score
            """, (date, symbol, client_name, signal_type, deal_count,
                  total_qty, total_value_cr, deal_type, score))

    def get_bulk_signals(self, date: str = None, days_back: int = 7):
        with self._conn() as conn:
            if date:
                cutoff = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=days_back)).strftime('%Y-%m-%d')
                rows = conn.execute(
                    "SELECT * FROM bulk_deal_signals WHERE date >= ? ORDER BY score DESC", (cutoff,)
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM bulk_deal_signals ORDER BY date DESC, score DESC").fetchall()
            return [dict(r) for r in rows]

    # ─── FII/DII SIGNALS ──────────────────────────────────────────────────────

    def upsert_fii_signal(self, date: str, fii_cash_net: float, fii_fut_net: float,
                           fii_opt_net: float, dii_cash_net: float,
                           composite_score: float, rolling_5d_score: float,
                           regime: str, signal_type: str, score: int):
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO fii_dii_signals (date, fii_cash_net, fii_fut_net, fii_opt_net,
                    dii_cash_net, composite_score, rolling_5d_score, regime, signal_type, score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    fii_cash_net=excluded.fii_cash_net,
                    fii_fut_net=excluded.fii_fut_net,
                    rolling_5d_score=excluded.rolling_5d_score,
                    regime=excluded.regime,
                    signal_type=excluded.signal_type,
                    score=excluded.score
            """, (date, fii_cash_net, fii_fut_net, fii_opt_net, dii_cash_net,
                  composite_score, rolling_5d_score, regime, signal_type, score))

    def get_fii_regime(self, date: str = None):
        with self._conn() as conn:
            if date:
                row = conn.execute(
                    "SELECT * FROM fii_dii_signals WHERE date <= ? ORDER BY date DESC LIMIT 1", (date,)
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM fii_dii_signals ORDER BY date DESC LIMIT 1"
                ).fetchone()
            return dict(row) if row else None

    def get_fii_history(self, days: int = 10):
        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM fii_dii_signals WHERE date >= ? ORDER BY date", (cutoff,)
            ).fetchall()
        return [dict(r) for r in rows]

    # ─── SEASONALITY ──────────────────────────────────────────────────────────

    def upsert_seasonality_fact(self, pattern_name: str, description: str, pattern_type: str,
                                 day_of_week: int, month: int, avg_return: float,
                                 p_value: float, occurrences: int, t_statistic: float,
                                 score_when_active: int, is_valid: bool):
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO seasonality_facts (pattern_name, description, pattern_type, day_of_week,
                    month, avg_return, p_value, occurrences, t_statistic, score_when_active,
                    is_valid, validated_on)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, date('now'))
                ON CONFLICT(pattern_name) DO UPDATE SET
                    avg_return=excluded.avg_return,
                    p_value=excluded.p_value,
                    occurrences=excluded.occurrences,
                    t_statistic=excluded.t_statistic,
                    score_when_active=excluded.score_when_active,
                    is_valid=excluded.is_valid,
                    validated_on=date('now')
            """, (pattern_name, description, pattern_type, day_of_week, month,
                  avg_return, p_value, occurrences, t_statistic,
                  score_when_active, 1 if is_valid else 0))

    def get_active_seasonality_score(self, date: str):
        """Return (total_score, [active_pattern_names]) for the given date."""
        dt = datetime.strptime(date, '%Y-%m-%d')
        day_of_week = dt.weekday()  # 0=Mon
        month = dt.month

        with self._conn() as conn:
            rows = conn.execute("""
                SELECT * FROM seasonality_facts
                WHERE is_valid = 1
                AND (day_of_week = ? OR day_of_week = -1)
                AND (month = ? OR month = -1)
                AND (pattern_type NOT IN ('DOW') OR day_of_week = ?)
            """, (day_of_week, month, day_of_week)).fetchall()

        total = 0
        active = []
        for row in rows:
            row = dict(row)
            # Double-check DOW match
            if row['pattern_type'] == 'DOW' and row['day_of_week'] != day_of_week:
                continue
            if row['pattern_type'] == 'MONTH' and row['month'] != month:
                continue
            total += row['score_when_active']
            active.append(row['pattern_name'])

        return total, active

    # ─── COMPOSITE SCORES ─────────────────────────────────────────────────────

    def upsert_composite_score(self, date: str, symbol: str, composite_score: int,
                                vol_score: int = 0, insider_score: int = 0,
                                bulk_score: int = 0, pairs_score: int = 0,
                                fii_score: int = 0, seasonality_score: int = 0,
                                sentiment_score: int = 0, signal_type: str = 'NO_SIGNAL',
                                active_signals: list = None):
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO composite_scores (date, symbol, composite_score, vol_score, insider_score,
                    bulk_score, pairs_score, fii_score, seasonality_score, sentiment_score,
                    signal_type, active_signals)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, symbol) DO UPDATE SET
                    composite_score=excluded.composite_score,
                    vol_score=excluded.vol_score,
                    insider_score=excluded.insider_score,
                    bulk_score=excluded.bulk_score,
                    pairs_score=excluded.pairs_score,
                    fii_score=excluded.fii_score,
                    seasonality_score=excluded.seasonality_score,
                    sentiment_score=excluded.sentiment_score,
                    signal_type=excluded.signal_type,
                    active_signals=excluded.active_signals
            """, (date, symbol, composite_score, vol_score, insider_score,
                  bulk_score, pairs_score, fii_score, seasonality_score, sentiment_score,
                  signal_type, json.dumps(active_signals or [])))

    def get_composite_scores(self, date: str, min_score: int = None,
                              top_n: int = None):
        with self._conn() as conn:
            q = "SELECT * FROM composite_scores WHERE date = ?"
            params = [date]
            if min_score is not None:
                q += " AND composite_score >= ?"
                params.append(min_score)
            q += " ORDER BY composite_score DESC"
            if top_n:
                q += " LIMIT ?"
                params.append(top_n)
            rows = conn.execute(q, params).fetchall()
            results = []
            for r in rows:
                d = dict(r)
                if d.get('active_signals'):
                    try:
                        d['active_signals'] = json.loads(d['active_signals'])
                    except Exception:
                        pass
                results.append(d)
            return results

    def get_signal_type_for_score(self, score: int) -> str:
        """Map composite score to trading action."""
        if score >= 60:
            return 'STRONG_BUY'
        elif score >= 40:
            return 'BUY'
        elif score >= 20:
            return 'WATCHLIST'
        elif score >= -20:
            return 'NO_SIGNAL'
        elif score >= -40:
            return 'SHORT'
        else:
            return 'STRONG_SHORT'

    # ─── BACKTEST TRACKING ────────────────────────────────────────────────────

    def insert_backtest_result(self, module: str, signal_type: str, trade_date: str,
                                symbol: str, entry_score: int, return_5d: float,
                                return_10d: float, return_30d: float, was_correct: bool):
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO backtest_results (module, signal_type, backtest_date, trade_date, symbol,
                    entry_score, return_5d, return_10d, return_30d, was_correct)
                VALUES (?, ?, date('now'), ?, ?, ?, ?, ?, ?, ?)
            """, (module, signal_type, trade_date, symbol, entry_score,
                  return_5d, return_10d, return_30d, 1 if was_correct else 0))

    def get_backtest_stats(self, module: str = None, days_back: int = 90) -> dict:
        """Rolling hit rate and avg return for a module."""
        cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        with self._conn() as conn:
            q = "SELECT * FROM backtest_results WHERE trade_date >= ?"
            params = [cutoff]
            if module:
                q += " AND module = ?"
                params.append(module)
            rows = conn.execute(q, params).fetchall()
        if not rows:
            return {'module': module, 'trades': 0}
        rets_5d = [r['return_5d'] for r in rows if r['return_5d'] is not None]
        correct = sum(1 for r in rows if r['was_correct'])
        return {
            'module': module or 'ALL',
            'trades': len(rows),
            'win_rate': correct / len(rows) if rows else 0,
            'avg_return_5d': float(np.mean(rets_5d)) if rets_5d else 0,
            'days_back': days_back,
        }

    def summary(self) -> dict:
        """Quick DB summary."""
        with self._conn() as conn:
            counts = {}
            for table in ['volume_signals', 'insider_signals', 'bulk_deal_signals',
                           'fii_dii_signals', 'composite_scores', 'backtest_results']:
                row = conn.execute(f"SELECT COUNT(*) as n FROM {table}").fetchone()
                counts[table] = row['n']
        return counts


if __name__ == "__main__":
    db = SignalsDB()
    print("Schema created. DB summary:", db.summary())
