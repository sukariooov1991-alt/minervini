"""Multi-source daily market-data adapter with automatic failover."""
from __future__ import annotations

from datetime import datetime, timezone, date
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json
import logging
import os
import time
import threading

import pandas as pd

from data.storage_manager import StorageManager

log = logging.getLogger(__name__)

# Serialize public Yahoo requests to reduce throttling.
_YAHOO_REQUEST_LOCK = threading.Lock()
_YAHOO_NEXT_REQUEST_AT = 0.0
_YAHOO_MIN_REQUEST_INTERVAL = 1.25
_LONGBRIDGE_LOCK = threading.Lock()
_LONGBRIDGE_CONTEXT = None


class YahooRateLimitError(RuntimeError):
    """Legacy compatibility exception; market-data failover now handles Yahoo failures."""


def _wait_for_yahoo_request_slot() -> None:
    global _YAHOO_NEXT_REQUEST_AT
    with _YAHOO_REQUEST_LOCK:
        wait = max(0.0, _YAHOO_NEXT_REQUEST_AT - time.monotonic())
        if wait:
            time.sleep(wait)
        _YAHOO_NEXT_REQUEST_AT = time.monotonic() + _YAHOO_MIN_REQUEST_INTERVAL


def _period_start(period: str) -> date:
    now = pd.Timestamp.now().normalize()
    value = str(period or "2y").lower().strip()
    try:
        if value.endswith("y"):
            return (now - pd.DateOffset(years=int(value[:-1]))).date()
        if value.endswith("mo"):
            return (now - pd.DateOffset(months=int(value[:-2]))).date()
        if value.endswith("d"):
            return (now - pd.Timedelta(days=int(value[:-1]))).date()
    except (TypeError, ValueError):
        pass
    return (now - pd.DateOffset(years=2)).date()


def _normalise_ohlcv(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    frame = df.copy()
    frame.columns = [str(c).strip().lower().replace(" ", "_") for c in frame.columns]
    if "date" in frame.columns:
        frame = frame.set_index("date")
    if "datetime" in frame.columns:
        frame = frame.set_index("datetime")
    if "timestamp" in frame.columns:
        frame = frame.set_index("timestamp")
    rename = {"adj_close": "close", "vol": "volume"}
    frame = frame.rename(columns=rename)
    required = ["open", "high", "low", "close", "volume"]
    if not set(required).issubset(frame.columns):
        return pd.DataFrame()
    frame = frame[required].copy()
    frame.index = pd.to_datetime(frame.index, errors="coerce", utc=True)
    frame = frame[~frame.index.isna()]
    # Store exchange dates, not UTC-shifted calendar dates.
    frame.index = frame.index.tz_convert(None).normalize()
    for col in required:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.replace([float("inf"), float("-inf")], pd.NA).dropna(subset=required)
    frame = frame[(frame[["open", "high", "low", "close"]] > 0).all(axis=1)]
    frame = frame[frame["volume"] >= 0]
    frame = frame[(frame["high"] >= frame[["open", "low", "close"]].max(axis=1)) &
                  (frame["low"] <= frame[["open", "high", "close"]].min(axis=1))]
    return frame[~frame.index.duplicated(keep="last")].sort_index()


def _get_longbridge_context():
    global _LONGBRIDGE_CONTEXT
    with _LONGBRIDGE_LOCK:
        if _LONGBRIDGE_CONTEXT is not None:
            return _LONGBRIDGE_CONTEXT
        app_key = os.getenv("LONGBRIDGE_APP_KEY", "").strip()
        app_secret = os.getenv("LONGBRIDGE_APP_SECRET", "").strip()
        access_token = os.getenv("LONGBRIDGE_ACCESS_TOKEN", "").strip()
        if not (app_key and app_secret and access_token):
            raise RuntimeError("Longbridge credentials are not configured")
        from longbridge.openapi import Config, QuoteContext
        _LONGBRIDGE_CONTEXT = QuoteContext(Config(
            app_key=app_key, app_secret=app_secret, access_token=access_token
        ))
        return _LONGBRIDGE_CONTEXT


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
        self.storage = storage or StorageManager()
        self.last_source: str | None = None
        try:
            import yfinance as yf
            self.ticker = yf.Ticker(self.symbol.replace(".", "-"))
        except ImportError:
            self.ticker = None

    def _fetch_yahoo(self, period: str) -> pd.DataFrame:
        if self.ticker is None:
            raise RuntimeError("yfinance is not installed")
        last_error = None
        for attempt in range(3):
            try:
                _wait_for_yahoo_request_slot()
                frame = _normalise_ohlcv(self.ticker.history(
                    period=period, interval="1d", auto_adjust=True
                ))
                if not frame.empty:
                    return frame
            except Exception as exc:
                last_error = exc
                log.warning("Yahoo failed for %s (attempt %s/3): %s", self.symbol, attempt + 1, exc)
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
                try:
                    import yfinance as yf
                    self.ticker = yf.Ticker(self.symbol.replace(".", "-"))
                except Exception:
                    pass
        raise RuntimeError(f"Yahoo returned no usable daily candles: {last_error or 'empty response'}")

    def _fetch_nasdaq(self, period: str) -> pd.DataFrame:
        """Fetch public delayed daily OHLCV from Nasdaq's no-key historical endpoint."""
        start = _period_start(period)
        end = pd.Timestamp.now().date()
        params = urlencode({
            "assetclass": "stocks",
            "fromdate": start.strftime("%Y-%m-%d"),
            "todate": end.strftime("%Y-%m-%d"),
            "limit": "999999",
        })
        url = f"https://api.nasdaq.com/api/quote/{self.symbol}/historical?{params}"
        request = Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; MinerviniDataBot/1.0)",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://www.nasdaq.com",
            "Referer": f"https://www.nasdaq.com/market-activity/stocks/{self.symbol.lower()}",
            "Accept-Language": "en-US,en;q=0.9",
        })
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
        rows = (((payload.get("data") or {}).get("tradesTable") or {}).get("rows")) or []
        if not rows:
            message = (payload.get("message") or payload.get("status") or "no historical rows returned")
            raise RuntimeError(f"Nasdaq returned no historical data: {message}")

        def number(value):
            if value is None:
                return None
            cleaned = str(value).strip().replace("$", "").replace(",", "")
            if cleaned in {"", "-", "N/A", "NA"}:
                return None
            return float(cleaned)

        parsed = []
        for row in rows:
            try:
                parsed.append({
                    "date": pd.to_datetime(row.get("date"), format="%m/%d/%Y", errors="raise"),
                    "open": number(row.get("open")),
                    "high": number(row.get("high")),
                    "low": number(row.get("low")),
                    "close": number(row.get("close")),
                    "volume": number(row.get("volume")),
                })
            except (TypeError, ValueError):
                continue
        return _normalise_ohlcv(pd.DataFrame(parsed))

    def _fetch_longbridge(self, period: str) -> pd.DataFrame:
        from longbridge.openapi import Period, AdjustType
        ctx = _get_longbridge_context()
        lb_symbol = self.symbol if self.symbol.endswith(".US") else f"{self.symbol}.US"
        candles = ctx.history_candlesticks_by_date(
            lb_symbol, Period.Day, AdjustType.ForwardAdjust,
            _period_start(period), pd.Timestamp.now().date()
        )
        rows = []
        for candle in candles or []:
            stamp = getattr(candle, "timestamp", None)
            if stamp is None:
                continue
            if isinstance(stamp, (int, float)):
                stamp = pd.to_datetime(stamp, unit="s", utc=True)
            rows.append({
                "timestamp": stamp,
                "open": getattr(candle, "open", None),
                "high": getattr(candle, "high", None),
                "low": getattr(candle, "low", None),
                "close": getattr(candle, "close", None),
                "volume": getattr(candle, "volume", None),
            })
        return _normalise_ohlcv(pd.DataFrame(rows))

    def _fetch_stooq(self, period: str) -> pd.DataFrame:
        symbol = self.symbol.lower().replace(".", "-")
        if not symbol.endswith(".us"):
            symbol += ".us"
        params = urlencode({
            "s": symbol, "i": "d",
            "d1": _period_start(period).strftime("%Y%m%d"),
            "d2": pd.Timestamp.now().strftime("%Y%m%d"),
        })
        url = f"https://stooq.com/q/d/l/?{params}"
        with urlopen(url, timeout=25) as response:
            frame = pd.read_csv(response)
        return _normalise_ohlcv(frame)

    def _fetch_alpha_vantage(self, period: str) -> pd.DataFrame:
        api_key = os.getenv("ALPHAVANTAGE_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("ALPHAVANTAGE_API_KEY is not configured")
        params = urlencode({
            "function": "TIME_SERIES_DAILY",
            "symbol": self.symbol.replace(".", "-"),
            "outputsize": "full",
            "apikey": api_key,
        })
        url = f"https://www.alphavantage.co/query?{params}"
        with urlopen(url, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
        series_key = "Time Series (Daily)"
        if series_key not in payload:
            message = payload.get("Note") or payload.get("Information") or payload.get("Error Message") or "no daily series returned"
            raise RuntimeError(f"Alpha Vantage: {message}")
        frame = pd.DataFrame.from_dict(payload[series_key], orient="index").rename(columns={
            "1. open": "open", "2. high": "high", "3. low": "low",
            "4. close": "close", "5. volume": "volume", "6. volume": "volume",
        })
        frame.index = pd.to_datetime(frame.index, errors="coerce")
        frame = frame[frame.index >= pd.Timestamp(_period_start(period))]
        return _normalise_ohlcv(frame)

    def fetch_and_store_daily_candles(self, period: str = "2y") -> int:
        # Yahoo is primary; Nasdaq is a no-key public fallback. Keep the existing
        # Longbridge integration intact, followed by optional backup sources.
        providers = (
            ("Yahoo Finance", lambda: self._fetch_yahoo(period)),
            ("Nasdaq public data", lambda: self._fetch_nasdaq(period)),
            ("Longbridge", lambda: self._fetch_longbridge(period)),
            ("Stooq", lambda: self._fetch_stooq(period)),
            ("Alpha Vantage", lambda: self._fetch_alpha_vantage(period)),
        )
        errors = []
        for source, fetch in providers:
            try:
                frame = _normalise_ohlcv(fetch())
                if frame.empty:
                    raise RuntimeError("source returned no valid OHLCV candles")
                # The Minervini trend template needs at least 252 daily bars.
                # A short/partial response is not accepted as a successful source.
                if len(frame) < 252:
                    raise RuntimeError(
                        f"source returned only {len(frame)} daily candles; 252 are required"
                    )
                rows = [
                    (idx.strftime("%Y-%m-%d"), float(row.open), float(row.high),
                     float(row.low), float(row.close), float(row.volume))
                    for idx, row in frame.iterrows()
                ]
                count = self.storage.upsert_candles_bulk(self.symbol, rows)
                if count <= 0:
                    raise RuntimeError("no valid candles remained after storage validation")
                self.last_source = source
                log.info("%s: fetched %s daily candles from %s", self.symbol, count, source)
                return count
            except Exception as exc:
                errors.append(f"{source}: {exc}")
                log.warning("%s unavailable for %s; trying next provider: %s", source, self.symbol, exc)
        raise ValueError(
            f"No daily data for {self.symbol}; all 5 providers failed. " + " | ".join(errors)
        )

    def get_ohlcv_dataframe(self) -> pd.DataFrame:
        return self.storage.get_candles_df(self.symbol)

    def get_fundamentals_dataframe(self) -> pd.DataFrame:
        """Return quarterly fundamentals; absent fields remain NaN, never fabricated."""
        try:
            _wait_for_yahoo_request_slot()
            q = self.ticker.quarterly_financials
        except Exception:
            log.exception("Quarterly fundamentals request failed for %s", self.symbol)
            return pd.DataFrame()
        # Do not call ticker.info here: Yahoo's quoteSummary endpoint frequently
        # returns HTTP 401 (Invalid Crumb / Unauthorized). ROE is supplementary;
        # EPS and revenue growth remain available from quarterly_financials.
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
                "roe_pct": float("nan"),
            })

        return pd.DataFrame(rows).sort_values("period", ascending=False).reset_index(drop=True)

    def fetch_and_store_news(self) -> list[dict[str, Any]]:
        """Fetch news from Yahoo Finance and store in SQLite events table."""
        try:
            _wait_for_yahoo_request_slot()
            raw_items = getattr(self.ticker, "news", None) or []
        except Exception:
            log.exception("Yahoo news request failed for %s", self.symbol)
            return []
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
