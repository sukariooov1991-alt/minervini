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
    MAX_CANDLES = 300

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
        conn = (
            sqlite3.connect(self._memory_uri, uri=True, timeout=30)
            if self._memory_uri
            else sqlite3.connect(self.db_path, timeout=30)
        )
        conn.execute("PRAGMA busy_timeout=30000")
        if self._memory_uri is None:
            conn.execute("PRAGMA synchronous=NORMAL")
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
            # WAL lets scan, realtime-card, and SSE-backed readers coexist with
            # short writes on Render's persistent SQLite database.
            if self._memory_uri is None:
                conn.execute("PRAGMA journal_mode=WAL")
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
            CREATE TABLE IF NOT EXISTS scan_checkpoints (
                scan_name TEXT PRIMARY KEY,
                symbols_json TEXT NOT NULL,
                completed_json TEXT NOT NULL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS data_cache (
                cache_key TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS watchlist_cards (
                symbol TEXT PRIMARY KEY,
                card_json TEXT NOT NULL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """)
            # One-time-safe migration: trim any legacy 600-bar histories to the
            # latest 300 rows per symbol without touching cards or event records.
            conn.execute("""
                DELETE FROM candles
                WHERE rowid IN (
                    SELECT rowid FROM (
                        SELECT rowid,
                               ROW_NUMBER() OVER (
                                   PARTITION BY symbol ORDER BY date DESC
                               ) AS position
                        FROM candles
                    )
                    WHERE position > ?
                )
            """, (self.MAX_CANDLES,))


    def get_cache(self, cache_key: str, max_age_seconds: int | None = None) -> Any | None:
        """Read a durable JSON cache entry, optionally enforcing its freshness."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT payload, fetched_at FROM data_cache WHERE cache_key=?",
                (str(cache_key),),
            ).fetchone()
        if row is None:
            return None
        if max_age_seconds is not None:
            try:
                fetched = pd.Timestamp(row["fetched_at"])
                if fetched.tzinfo is None:
                    fetched = fetched.tz_localize("UTC")
                age = (pd.Timestamp.now(tz="UTC") - fetched.tz_convert("UTC")).total_seconds()
                if age > max_age_seconds:
                    return None
            except Exception:
                return None
        try:
            return json.loads(row["payload"])
        except (TypeError, ValueError):
            return None

    def cache_age_seconds(self, cache_key: str) -> float | None:
        """Return cache age in seconds, or None when the entry is absent/invalid."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT fetched_at FROM data_cache WHERE cache_key=?",
                (str(cache_key),),
            ).fetchone()
        if row is None:
            return None
        try:
            fetched = pd.Timestamp(row["fetched_at"])
            if fetched.tzinfo is None:
                fetched = fetched.tz_localize("UTC")
            return max(
                0.0,
                (pd.Timestamp.now(tz="UTC") - fetched.tz_convert("UTC")).total_seconds(),
            )
        except Exception:
            return None

    def set_cache(self, cache_key: str, payload: Any) -> None:
        """Persist JSON data and its UTC refresh timestamp."""
        encoded = json.dumps(payload, ensure_ascii=False, default=str, allow_nan=False)
        with self._get_conn() as conn:
            conn.execute(
                """INSERT INTO data_cache(cache_key,payload,fetched_at)
                   VALUES(?,?,?)
                   ON CONFLICT(cache_key) DO UPDATE SET
                     payload=excluded.payload, fetched_at=excluded.fetched_at""",
                (str(cache_key), encoded, pd.Timestamp.now(tz="UTC").isoformat()),
            )

    def save_watchlist_card(self, card: Dict[str, Any]) -> None:
        """Persist one complete watchlist card so it survives process restarts."""
        if not isinstance(card, dict):
            raise TypeError("card must be a dictionary")
        symbol = str(card.get("symbol") or "").upper().strip()
        if not symbol:
            raise ValueError("watchlist card requires a symbol")
        def make_json_safe(value: Any) -> Any:
            if isinstance(value, float) and not math.isfinite(value):
                return None
            if isinstance(value, dict):
                return {str(key): make_json_safe(item) for key, item in value.items()}
            if isinstance(value, (list, tuple)):
                return [make_json_safe(item) for item in value]
            return value

        payload = json.dumps(
            make_json_safe(card), ensure_ascii=False, default=str, allow_nan=False
        )
        with self._get_conn() as conn:
            conn.execute(
                """INSERT INTO watchlist_cards(symbol, card_json, updated_at)
                   VALUES (?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(symbol) DO UPDATE SET
                     card_json=excluded.card_json,
                     updated_at=CURRENT_TIMESTAMP""",
                (symbol, payload),
            )

    def load_watchlist_cards(self) -> list[dict[str, Any]]:
        """Load valid persisted cards in stable symbol order."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT symbol, card_json FROM watchlist_cards ORDER BY symbol"
            ).fetchall()
        cards: list[dict[str, Any]] = []
        for row in rows:
            try:
                card = json.loads(row["card_json"])
            except (TypeError, ValueError):
                continue
            if isinstance(card, dict):
                card["symbol"] = str(card.get("symbol") or row["symbol"]).upper()
                cards.append(card)
        return cards

    def delete_watchlist_card(self, symbol: str) -> None:
        clean = str(symbol or "").upper().strip()
        if not clean:
            return
        with self._get_conn() as conn:
            conn.execute("DELETE FROM watchlist_cards WHERE symbol=?", (clean,))

    def get_scan_checkpoint(self, scan_name: str, symbols: list[str]) -> set[str]:
        """Return completed symbols for a matching resumable scan; reset on universe changes."""
        clean_symbols = [str(s).upper().strip() for s in symbols]
        symbols_json = json.dumps(clean_symbols, separators=(",", ":"))
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT symbols_json, completed_json FROM scan_checkpoints WHERE scan_name=?",
                (scan_name,),
            ).fetchone()
            if row is None or row["symbols_json"] != symbols_json:
                conn.execute(
                    "INSERT OR REPLACE INTO scan_checkpoints "
                    "(scan_name, symbols_json, completed_json, updated_at) VALUES (?,?,?,CURRENT_TIMESTAMP)",
                    (scan_name, symbols_json, "[]"),
                )
                return set()
            try:
                completed = json.loads(row["completed_json"])
            except (TypeError, ValueError):
                completed = []
            return {str(s).upper().strip() for s in completed}

    def has_scan_checkpoint(self, scan_name: str, symbols: list[str]) -> bool:
        """Check whether an interrupted scan exists for this exact symbol universe."""
        symbols_json = json.dumps(
            [str(s).upper().strip() for s in symbols], separators=(",", ":")
        )
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT symbols_json FROM scan_checkpoints WHERE scan_name=?",
                (scan_name,),
            ).fetchone()
        return row is not None and row["symbols_json"] == symbols_json

    def mark_scan_symbol_completed(self, scan_name: str, symbol: str) -> None:
        """Persist one successful symbol immediately so process restarts do not lose progress."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT completed_json FROM scan_checkpoints WHERE scan_name=?",
                (scan_name,),
            ).fetchone()
            if row is None:
                return
            try:
                completed = set(json.loads(row["completed_json"]))
            except (TypeError, ValueError):
                completed = set()
            completed.add(str(symbol).upper().strip())
            conn.execute(
                "UPDATE scan_checkpoints SET completed_json=?, updated_at=CURRENT_TIMESTAMP WHERE scan_name=?",
                (json.dumps(sorted(completed), separators=(",", ":")), scan_name),
            )

    def clear_scan_checkpoint(self, scan_name: str) -> None:
        with self._get_conn() as conn:
            conn.execute("DELETE FROM scan_checkpoints WHERE scan_name=?", (scan_name,))

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
                "WHERE symbol=? ORDER BY date DESC LIMIT ?",
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
            encoded = json.dumps(payload, ensure_ascii=False, default=str, sort_keys=True)
            conn.execute(
                """INSERT INTO events(symbol,type,timestamp,payload)
                   SELECT ?,?,?,? WHERE NOT EXISTS (
                     SELECT 1 FROM events
                     WHERE symbol=? AND type=? AND timestamp=? AND payload=?
                   )""",
                (symbol.upper(), event_type, timestamp, encoded,
                 symbol.upper(), event_type, timestamp, encoded),
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
