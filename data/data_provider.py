"""Yahoo Finance market-data adapter (daily candles, earnings, news)."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
import time
import pandas as pd

from data.storage_manager import StorageManager


def _find_nearest_prior_value(
    series: pd.Series,
    target_date: pd.Timestamp,
    max_window_days: int = 45,
) -> float:
    if series is None or series.empty:
        return float("nan")
    best_val = float("nan")
    best_diff_days = max_window_days + 1
    for idx_val, raw_val in series.items():
        try:
            idx_ts = pd.Timestamp(idx_val)
            num_val = float(raw_val)
        except (TypeError, ValueError):
            continue
        if pd.isna(num_val):
            continue
        # Only use a report dated on/before the year-ago target; never borrow future data.
        if idx_ts > target_date:
            continue
        diff_days = (target_date - idx_ts).days
        if 0 <= diff_days <= max_window_days and diff_days < best_diff_days:
            best_diff_days = diff_days
            best_val = num_val
    return best_val


class DataProvider:
    def __init__(self, symbol: str, storage: StorageManager | None = None):
        self.symbol = symbol.upper().strip()
        try:
            import yfinance as yf
        except ImportError as exc:
            raise RuntimeError(
                "Install dependencies with: pip install -r requirements.txt"
            ) from exc
        # Yahoo Finance uses hyphens for class-share tickers (e.g. BRK-B),
        # while the screener/database keep the canonical S&P symbol (BRK.B).
        yahoo_symbol = self.symbol.replace(".", "-")
        self.ticker = yf.Ticker(yahoo_symbol)
        self.storage = storage or StorageManager()

    def fetch_and_store_daily_candles(self, period: str = "2y") -> int:
        # Yahoo occasionally returns an empty response during transient crumb/session
        # failures. Retry the same request with bounded backoff before reporting failure.
        df = None
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                df = self.ticker.history(
                    period=period, interval="1d", auto_adjust=True
                )
                if df is not None and not df.empty:
                    break
            except Exception as exc:
                last_error = exc
            if attempt < 2:
                time.sleep(2 ** attempt)

        if df is None or df.empty:
            detail = f": {last_error}" if last_error else ""
            raise ValueError(f"No daily data for {self.symbol} after 3 attempts{detail}")

        rename = {"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}
        df = df.rename(columns=rename)
        required = ["open", "high", "low", "close", "volume"]
        if not set(required).issubset(df.columns):
            raise ValueError(f"OHLCV columns missing for {self.symbol}")

        bulk_rows: list[tuple[str, float, float, float, float, float]] = []
        for row in df[required].itertuples(index=True, name=None):
            dt = row[0]
            vals = [pd.to_numeric(v, errors="coerce") for v in row[1:]]
            if any(pd.isna(v) for v in vals):
                continue
            dt_str = pd.Timestamp(dt).strftime("%Y-%m-%d")
            fvals = tuple(float(v) for v in vals)
            bulk_rows.append((dt_str, fvals[0], fvals[1], fvals[2], fvals[3], fvals[4]))

        count = self.storage.upsert_candles_bulk(self.symbol, bulk_rows)
        if count == 0:
            raise ValueError(f"No valid OHLCV rows for {self.symbol}")
        return count

    def get_ohlcv_dataframe(self) -> pd.DataFrame:
        return self.storage.get_candles_df(self.symbol)

    def get_fundamentals_dataframe(self) -> pd.DataFrame:
        """Return quarterly fundamentals; absent fields remain NaN, never fabricated."""
        try:
            q = self.ticker.quarterly_financials
        except Exception:
            return pd.DataFrame()
        # ROE is supplementary metadata; Yahoo may deny the quoteSummary/info
        # endpoint even when quarterly financial statements are available.
        # Do not discard usable EPS/revenue data because this optional request fails.
        try:
            info = self.ticker.info or {}
        except Exception:
            info = {}
        if q is None or q.empty:
            return pd.DataFrame()

        eps = q.loc["Diluted EPS"].dropna() if "Diluted EPS" in q.index else pd.Series(dtype=float)
        rev = q.loc["Total Revenue"].dropna() if "Total Revenue" in q.index else pd.Series(dtype=float)

        dates = sorted(set(eps.index) | set(rev.index), reverse=True)
        rows = []
        for d in dates:
            d_ts = pd.Timestamp(d)
            e = pd.to_numeric(eps.get(d, float("nan")), errors="coerce")
            r = pd.to_numeric(rev.get(d, float("nan")), errors="coerce")

            prior_target = d_ts - pd.DateOffset(months=12)
            prior_e = _find_nearest_prior_value(eps, prior_target, max_window_days=45)
            prior_r = _find_nearest_prior_value(rev, prior_target, max_window_days=45)

            def growth(cur, prior):
                try:
                    cur, prior = float(cur), float(prior)
                    return (cur / prior - 1) * 100 if prior > 0 else float("nan")
                except (TypeError, ValueError):
                    return float("nan")

            rows.append({
                "period": d_ts,
                "eps_current": e,
                "eps_year_ago": prior_e,
                "revenue_current": r,
                "revenue_year_ago": prior_r,
                "eps_growth_pct": growth(e, prior_e),
                "revenue_growth_pct": growth(r, prior_r),
                "roe_pct": (
                    float(info["returnOnEquity"]) * 100
                    if info.get("returnOnEquity") is not None
                    else float("nan")
                ),
            })

        return pd.DataFrame(rows).sort_values("period", ascending=False).reset_index(drop=True)

    def fetch_and_store_news(self) -> list[dict[str, Any]]:
        """Fetch news from Yahoo Finance and store in SQLite events table."""
        raw_items = getattr(self.ticker, "news", None) or []
        stored: list[dict[str, Any]] = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            content = item.get("content") if isinstance(item.get("content"), dict) else item
            title = content.get("title") or item.get("title") or ""
            pub_date = (
                content.get("pubDate")
                or item.get("providerPublishTime")
                or datetime.now(timezone.utc).isoformat()
            )
            if isinstance(pub_date, (int, float)):
                ts = datetime.fromtimestamp(pub_date, tz=timezone.utc).isoformat()
            else:
                ts = str(pub_date)

            provider_info = content.get("provider") if isinstance(content.get("provider"), dict) else {}
            payload = {
                "title": title,
                "publisher": provider_info.get("displayName") or item.get("publisher") or "",
                "url": (
                    (content.get("canonicalUrl") or {}).get("url")
                    if isinstance(content.get("canonicalUrl"), dict)
                    else item.get("link", "")
                ),
                "source": "yahoo_finance",
            }
            self.storage.add_event(self.symbol, "news", ts, payload)
            stored.append({"symbol": self.symbol, "timestamp": ts, **payload})
        return stored
