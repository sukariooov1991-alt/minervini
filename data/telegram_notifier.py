"""Independent Telegram notification module for MINERVINI.

Strict rules per specification:
- Send ONLY when a stock passes all required filters AND appears on the main page.
- Include only: symbol, 5 entry zones, 5 targets, and 1 shared stop loss.
- Never send uncalculated prices/levels, and never send price-change,
  target-hit, or stop-loss notifications.
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
from pathlib import Path
from typing import Any
from urllib import parse, request


class TelegramNotifier:
    def __init__(
        self,
        bot_token: str | None = None,
        chat_id: str | None = None,
        timeout: int = 10,
        state_db_path: str | None = None,
    ):
        self.bot_token = (bot_token or os.getenv("TELEGRAM_BOT_TOKEN", "")).strip()
        self.chat_id = (chat_id or os.getenv("TELEGRAM_CHAT_ID", "")).strip()
        self.timeout = timeout
        db_target = state_db_path or os.getenv("DB_PATH", "stock_data.db")
        self.state_db_path = str(db_target)
        self._sent_symbols: set[str] = set()
        self._init_state_table()
        self._load_sent_symbols()

    def _init_state_table(self) -> None:
        if self.state_db_path == ":memory:":
            return
        Path(self.state_db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.state_db_path)
        try:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS telegram_sent_symbols (
                    symbol TEXT PRIMARY KEY,
                    sent_at TEXT DEFAULT CURRENT_TIMESTAMP
                )"""
            )
            conn.commit()
        finally:
            conn.close()

    def _load_sent_symbols(self) -> None:
        if self.state_db_path == ":memory:" or not Path(self.state_db_path).exists():
            return
        conn = sqlite3.connect(self.state_db_path)
        try:
            rows = conn.execute("SELECT symbol FROM telegram_sent_symbols").fetchall()
            self._sent_symbols = {str(r[0]).upper().strip() for r in rows if r and r[0]}
        finally:
            conn.close()

    def _mark_symbol_sent(self, symbol: str) -> None:
        sym = symbol.upper().strip()
        self._sent_symbols.add(sym)
        if self.state_db_path == ":memory:":
            return
        conn = sqlite3.connect(self.state_db_path)
        try:
            conn.execute(
                "INSERT OR IGNORE INTO telegram_sent_symbols(symbol) VALUES (?)",
                (sym,),
            )
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def is_eligible_for_notification(stock_item: dict[str, Any]) -> bool:
        passed_all = bool(stock_item.get("passed_all_filters", False))
        shown_on_main = bool(stock_item.get("shown_on_main_page", False))
        entries = stock_item.get("entry_zones") or []
        targets = stock_item.get("targets") or []
        stop_loss = stock_item.get("shared_stop_loss")

        try:
            entry_values = [float(v) for v in entries]
            target_values = [float(v) for v in targets]
            stop_value = float(stop_loss)
            has_calculated_levels = (
                len(entry_values) == 5
                and len(target_values) == 5
                and all(math.isfinite(v) and v > 0 for v in entry_values)
                and all(a < b for a, b in zip(entry_values, entry_values[1:]))
                and all(math.isfinite(t) and t > e for t, e in zip(target_values, entry_values))
                and math.isfinite(stop_value)
                and 0 < stop_value < entry_values[0]
            )
        except (TypeError, ValueError):
            has_calculated_levels = False
        return passed_all and shown_on_main and has_calculated_levels

    @staticmethod
    def format_message(stock_item: dict[str, Any]) -> str:
        symbol = str(stock_item.get("symbol", "")).upper().strip()
        entries = stock_item["entry_zones"]
        targets = stock_item["targets"]
        stop_loss = stock_item["shared_stop_loss"]

        lines = [
            f"رمز السهم: {symbol}",
            "مناطق الدخول الخمس:",
        ]
        for idx, val in enumerate(entries, 1):
            lines.append(f"  {idx}. {val}")

        lines.append("الأهداف الخمسة:")
        for idx, val in enumerate(targets, 1):
            lines.append(f"  {idx}. {val}")

        lines.append(f"وقف الخسارة المشترك: {stop_loss}")
        return "\n".join(lines)

    def send_stock_appearance_alert(self, stock_item: dict[str, Any], force_resend: bool = False) -> bool:
        if not self.is_eligible_for_notification(stock_item):
            return False

        symbol = str(stock_item.get("symbol", "")).upper().strip()
        if not symbol:
            return False

        if not force_resend and symbol in self._sent_symbols:
            return False

        if not self.bot_token or not self.chat_id:
            raise RuntimeError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be configured.")

        text = self.format_message(stock_item)
        url = f"https://api.telegram.org/bot{parse.quote(self.bot_token)}/sendMessage"
        payload = json.dumps({
            "chat_id": self.chat_id,
            "text": text,
        }).encode("utf-8")

        req = request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(req, timeout=self.timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            ok = bool(body.get("ok", False))
            if ok:
                self._mark_symbol_sent(symbol)
            return ok
