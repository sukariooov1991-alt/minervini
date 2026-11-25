"""SQLite OHLCV and event storage."""
from __future__ import annotations

import json
import math
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable

import pandas as pd


class StorageManager:
    MAX_CANDLES = 600
    MAX_EVENTS = 50

    def __init__(self, db_path: str = "stock_data.db"):
        self.db_path = str(db_path)
        self._memory_uri = None
        self._memory_anchor = None
        if self.db_path == ":memory:":
            # Keep a shared in-memory database alive across short-lived connections.
            self._memory_uri = f"file:minervini_{uuid.uuid4().hex}?mode=memory&cache=shared"
            self._memory_anchor = sqlite3.connect(
                self._memory_uri, uri=True, check_same_thread=False
            )
        else:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _get_conn(self):
        conn = sqlite3.connect(self._memory_uri, uri=True) if self._memory_uri else sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        with self._get_conn() as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS candles (
                symbol TEXT NOT NULL, date TEXT NOT NULL,
                open REAL, high REAL, low REAL, close REAL, volume REAL,
                PRIMARY KEY(symbol, date)
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                type TEXT NOT NULL CHECK(type IN ('news','earnings')),
                timestamp TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_candles_sym_date ON candles(symbol, date DESC);
            CREATE INDEX IF NOT EXISTS idx_events_sym_type_ts ON events(symbol, type, timestamp DESC);
            """)

    @staticmethod
    def _clean_candle_row(
        symbol: str, date: str,
        open_: float, high: float, low: float, close: float, volume: float,
    ) -> tuple[str, str, float, float, float, float, float] | None:
        vals = [open_, high, low, close, volume]
        try:
            fvals = [float(v) for v in vals]
        except (TypeError, ValueError):
            return None
        if not all(math.isfinite(v) for v in fvals) or min(fvals[:4]) <= 0 or fvals[4] < 0:
            return None
        open_, high, low, close, volume = fvals
        # OHLC integrity: the day's high/low must contain every traded OHLC price.
        if high < max(open_, low, close) or low > min(open_, high, close):
            return None
        date_text = str(date)[:10]
        if len(date_text) != 10:
            return None
        try:
            pd.Timestamp(date_text)
        except (TypeError, ValueError):
            return None
        return (symbol.upper().strip(), date_text, open_, high, low, close, volume)

    def upsert_candle(
        self, symbol: str, date: str,
        open_: float, high: float, low: float, close: float, volume: float,
    ) -> None:
        row = self._clean_candle_row(symbol, date, open_, high, low, close, volume)
        if row is None:
            return
        sym = row[0]
        with self._get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO candles VALUES (?,?,?,?,?,?,?)",
                row,
            )
            conn.execute(
                """DELETE FROM candles WHERE symbol=? AND date NOT IN (
                   SELECT date FROM candles WHERE symbol=? ORDER BY date DESC LIMIT ?)""",
                (sym, sym, self.MAX_CANDLES),
            )

    def upsert_candles_bulk(
        self,
        symbol: str,
        rows: Iterable[tuple[str, float, float, float, float, float]],
    ) -> int:
        sym = symbol.upper().strip()
        cleaned: list[tuple[str, str, float, float, float, float, float]] = []
        for item in rows:
            if len(item) != 6:
                continue
            dt_str, o, h, l, c, v = item
            row = self._clean_candle_row(sym, dt_str, o, h, l, c, v)
            if row is not None:
                cleaned.append(row)

        if not cleaned:
            return 0

        with self._get_conn() as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO candles VALUES (?,?,?,?,?,?,?)",
                cleaned,
            )
            conn.execute(
                """DELETE FROM candles WHERE symbol=? AND date NOT IN (
                   SELECT date FROM candles WHERE symbol=? ORDER BY date DESC LIMIT ?)""",
                (sym, sym, self.MAX_CANDLES),
            )
        return len(cleaned)

    def replace_candles_bulk(
        self,
        symbol: str,
        rows: Iterable[tuple[str, float, float, float, float, float]],
    ) -> int:
        """Atomically replace one symbol's history with a newly validated provider series."""
        sym = symbol.upper().strip()
        cleaned: list[tuple[str, str, float, float, float, float, float]] = []
        for item in rows:
            if len(item) != 6:
                continue
            dt_str, o, h, l, close_, volume = item
            row = self._clean_candle_row(sym, dt_str, o, h, l, close_, volume)
            if row is not None:
                cleaned.append(row)

        if not cleaned:
            return 0

        with self._get_conn() as conn:
            conn.execute("DELETE FROM candles WHERE symbol=?", (sym,))
            conn.executemany(
                "INSERT OR REPLACE INTO candles VALUES (?,?,?,?,?,?,?)",
                cleaned,
            )
            conn.execute(
                """DELETE FROM candles WHERE symbol=? AND date NOT IN (
                   SELECT date FROM candles WHERE symbol=? ORDER BY date DESC LIMIT ?)""",
                (sym, sym, self.MAX_CANDLES),
            )
        return len(cleaned)

    def get_candles_df(self, symbol: str) -> pd.DataFrame:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT date,open,high,low,close,volume FROM candles "
                "WHERE symbol=? ORDER BY date ASC LIMIT ?",
                (symbol.upper(), self.MAX_CANDLES),
            ).fetchall()

        cols = ["date", "open", "high", "low", "close", "volume"]
        df = pd.DataFrame([dict(r) for r in rows], columns=cols)
        if df.empty:
            return df
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for c in cols[1:]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return (
            df.dropna(subset=cols)
            .drop_duplicates("date")
            .sort_values("date")
            .reset_index(drop=True)
        )

    def add_event(
        self, symbol: str, event_type: str, timestamp: str, payload: Dict[str, Any],
    ) -> None:
        if event_type not in {"news", "earnings"}:
            raise ValueError("event_type must be news or earnings")
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO events(symbol,type,timestamp,payload) VALUES(?,?,?,?)",
                (symbol.upper(), event_type, timestamp, json.dumps(payload, default=str)),
            )
            conn.execute(
                """DELETE FROM events WHERE symbol=? AND type=? AND id NOT IN (
                   SELECT id FROM events WHERE symbol=? AND type=?
                   ORDER BY timestamp DESC LIMIT ?)""",
                (symbol.upper(), event_type, symbol.upper(), event_type, self.MAX_EVENTS),
            )

    def get_events(self, symbol: str, event_type: str, limit: int = 10) -> list[dict[str, Any]]:
        if event_type not in {"news", "earnings"}:
            raise ValueError("event_type must be news or earnings")
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT symbol, type, timestamp, payload FROM events "
                "WHERE symbol=? AND type=? ORDER BY timestamp DESC LIMIT ?",
                (symbol.upper(), event_type, limit),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            item = dict(r)
            try:
                item["payload"] = json.loads(item["payload"])
            except Exception:
                pass
            out.append(item)
        return out
