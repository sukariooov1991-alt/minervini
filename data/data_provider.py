"""Multi-source daily market-data adapter with automatic failover."""
from __future__ import annotations

from datetime import datetime, timezone, date, timedelta
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json
import math
import logging
import os
import time
import threading

import pandas as pd

from data.storage_manager import StorageManager
from data.market_time import MarketTime, get_us_equity_market_close

log = logging.getLogger(__name__)

# Serialize public Yahoo requests to reduce throttling.
_YAHOO_REQUEST_LOCK = threading.Lock()
_YAHOO_NEXT_REQUEST_AT = 0.0
_YAHOO_MIN_REQUEST_INTERVAL = 1.25
_DAILY_HISTORY_LOCKS_GUARD = threading.Lock()
_DAILY_HISTORY_LOCKS: dict[str, threading.Lock] = {}


def _daily_history_lock(symbol: str) -> threading.Lock:
    clean = str(symbol).upper().strip()
    with _DAILY_HISTORY_LOCKS_GUARD:
        return _DAILY_HISTORY_LOCKS.setdefault(clean, threading.Lock())



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
    # Prefer traded OHLC when both Yahoo Close and Adj Close are present.
    # Adj Close is dividend-adjusted and must not be mixed with raw Longbridge
    # live prices; use it only for providers that expose no ordinary Close.
    if "close" not in frame.columns and "adj_close" in frame.columns:
        frame = frame.rename(columns={"adj_close": "close"})
    else:
        frame = frame.drop(columns=["adj_close"], errors="ignore")
    if "volume" not in frame.columns and "vol" in frame.columns:
        frame = frame.rename(columns={"vol": "volume"})
    else:
        frame = frame.drop(columns=["vol"], errors="ignore")
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


def _find_nearest_prior_period(
    eps: pd.Series,
    revenue: pd.Series,
    target_date: pd.Timestamp,
    max_window_days: int = 60,
) -> tuple[float, float]:
    """Return EPS and revenue from the same prior fiscal reporting period.

    Match a common reported period at/before the year-ago target. This supports
    52/53-week fiscal calendars without combining EPS from one quarter with
    revenue from another or introducing future-report look-ahead.
    """
    def values_by_period(series: pd.Series) -> dict[pd.Timestamp, float]:
        result: dict[pd.Timestamp, float] = {}
        if series is None or series.empty:
            return result
        for raw_date, raw_value in series.items():
            try:
                period = pd.Timestamp(raw_date)
                value = float(raw_value)
                if period.tzinfo is not None:
                    period = period.tz_localize(None)
            except (TypeError, ValueError, OverflowError):
                continue
            if not pd.isna(period) and math.isfinite(value):
                result[period] = value
        return result

    eps_values = values_by_period(eps)
    revenue_values = values_by_period(revenue)
    target = pd.Timestamp(target_date)
    if target.tzinfo is not None:
        target = target.tz_localize(None)

    common_periods = []
    for period in eps_values.keys() & revenue_values.keys():
        if period <= target:
            distance = (target - period).days
            if 0 <= distance <= max_window_days:
                common_periods.append((distance, period))

    if not common_periods:
        return float("nan"), float("nan")

    _, matched_period = min(common_periods, key=lambda item: (item[0], -item[1].value))
    return eps_values[matched_period], revenue_values[matched_period]


def _latest_completed_session(now_et: datetime) -> date | None:
    """Return the newest US equity session whose daily bar should be complete."""
    local_now = MarketTime.to_new_york(now_et)
    candidate = local_now.date()
    for _ in range(14):
        market_close = get_us_equity_market_close(candidate)
        if market_close is not None:
            completed_at = datetime.combine(
                candidate, market_close, tzinfo=local_now.tzinfo
            ) + timedelta(hours=1)
            if local_now >= completed_at:
                return candidate
        candidate -= timedelta(days=1)
    return None


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
                    period=period, interval="1d", auto_adjust=False
                ))
                if not frame.empty:
                    return frame
            except Exception as exc:
                last_error = exc
                error_text = str(exc).lower()
                log.warning("Yahoo failed for %s (attempt %s/3): %s", self.symbol, attempt + 1, exc)
                # Authentication, permission, and rate-limit failures will not
                # be repaired by retrying the same request; fail over promptly.
                if any(marker in error_text for marker in (
                    "401", "403", "429", "unauthorized", "forbidden",
                    "invalid crumb", "too many requests", "rate limit",
                )):
                    break
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
        index_symbols = {
            "^GSPC": "SPX",
            "^IXIC": "COMP",
            "^DJI": "DJI",
        }
        is_index = self.symbol.startswith("^")
        nasdaq_symbol = index_symbols.get(self.symbol, self.symbol.lstrip("^"))
        asset_class = "index" if is_index else "stocks"
        market_path = "index" if is_index else "stocks"
        params = urlencode({
            "assetclass": asset_class,
            "fromdate": start.strftime("%Y-%m-%d"),
            "todate": end.strftime("%Y-%m-%d"),
            "limit": "999999",
        })
        url = f"https://api.nasdaq.com/api/quote/{nasdaq_symbol}/historical?{params}"
        request = Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; MinerviniDataBot/1.0)",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://www.nasdaq.com",
            "Referer": f"https://www.nasdaq.com/market-activity/{market_path}/{nasdaq_symbol.lower()}",
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

    def _fetch_stooq(self, period: str) -> pd.DataFrame:
        stooq_index_symbols = {
            "^GSPC": "spx",
            "^IXIC": "ndq",
            "^DJI": "dji",
        }
        symbol = stooq_index_symbols.get(
            self.symbol, self.symbol.lower().replace(".", "-")
        )
        if not self.symbol.startswith("^") and not symbol.endswith(".us"):
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
        if self.symbol.startswith("^"):
            raise RuntimeError("Alpha Vantage daily stock endpoint does not support index symbols")
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

    def fetch_and_store_daily_candles(self, period: str = "2y", append: bool = False) -> int:
        # Daily-history fallback order: Yahoo -> Stooq -> Nasdaq -> Alpha Vantage.
        # Longbridge is intentionally reserved for realtime WebSocket quotes only.
        providers = (
            ("Yahoo Finance (raw OHLC)", self._fetch_yahoo),
            ("Stooq", self._fetch_stooq),
            ("Nasdaq public data", self._fetch_nasdaq),
            ("Alpha Vantage", self._fetch_alpha_vantage),
        )
        source_key = f"daily_source:{self.symbol}"
        previous_source = self.storage.get_cache(source_key)
        existing_count = len(self.storage.get_candles_df(self.symbol))
        errors = []

        def completed_frame(raw_frame: pd.DataFrame) -> pd.DataFrame:
            frame = _normalise_ohlcv(raw_frame)
            if frame.empty:
                raise RuntimeError("source returned no valid OHLCV candles")
            # The current session's daily bar is incomplete until the regular
            # US session closes and the provider publishes its final OHLCV.
            now_et = MarketTime.now()
            regular_close = get_us_equity_market_close(now_et.date())
            completed_after = (
                datetime.combine(now_et.date(), regular_close) + timedelta(hours=1)
                if regular_close is not None else None
            )
            if completed_after is None or now_et.time() < completed_after.time():
                frame = frame[frame.index.date < now_et.date()]
            if frame.empty:
                raise RuntimeError("source returned no completed daily candles")
            return frame

        for source, fetch in providers:
            try:
                frame = completed_frame(fetch(period))
                # Validate the number of COMPLETED bars, not the raw response:
                # the current unfinished session may be removed above.
                min_required = 1 if append else 252
                if len(frame) < min_required:
                    raise RuntimeError(
                        f"source returned only {len(frame)} completed daily candles; "
                        f"{min_required} are required"
                    )

                # Never merge adjusted prices from different vendors. When the
                # source changes (or legacy cache has no source tag), fetch a
                # full replacement from the new source before writing anything.
                source_changed = bool(
                    append
                    and existing_count > 0
                    and (not previous_source or previous_source != source)
                )
                if source_changed:
                    reason = "unknown legacy source" if not previous_source else (
                        f"source changed from {previous_source} to {source}"
                    )
                    log.warning("%s: %s; rebasing the full daily history", self.symbol, reason)
                    if str(period).lower() != "2y":
                        frame = completed_frame(fetch("2y"))
                    if len(frame) < existing_count:
                        raise RuntimeError(
                            f"full source rebase returned only {len(frame)} completed candles; "
                            f"{existing_count} are required to preserve existing history"
                        )

                rows = [
                    (idx.strftime("%Y-%m-%d"), float(row.open), float(row.high),
                     float(row.low), float(row.close), float(row.volume))
                    for idx, row in frame.iterrows()
                ]
                if source_changed or not append:
                    count = self.storage.replace_candles_bulk(self.symbol, rows)
                else:
                    count = self.storage.upsert_candles_bulk(self.symbol, rows)
                if count <= 0:
                    raise RuntimeError("no valid candles remained after storage validation")
                self.storage.set_cache(source_key, source)
                self.last_source = source
                log.info("%s: fetched %s completed daily candles from %s", self.symbol, count, source)
                return count
            except Exception as exc:
                errors.append(f"{source}: {exc}")
                log.warning("%s unavailable for %s; trying next provider: %s", source, self.symbol, exc)
        raise ValueError(
            f"No daily data for {self.symbol}; all 4 daily-data providers failed. " + " | ".join(errors)
        )

    def ensure_daily_history(self, initial_period: str = "2y") -> int:
        """Serialize per-symbol refreshes shared by scans and the background scheduler."""
        with _daily_history_lock(self.symbol):
            return self._ensure_daily_history_locked(initial_period)

    def _ensure_daily_history_locked(self, initial_period: str = "2y") -> int:
        """Keep a permanent rolling 300-session cache; refresh only when a new session completes."""
        cached = self.storage.get_candles_df(self.symbol)
        known_source = self.storage.get_cache(f"daily_source:{self.symbol}")
        failure_key = f"daily_refresh_failure:{self.symbol}"
        failure_state = self.storage.get_cache(failure_key)
        failure_age = self.storage.cache_age_seconds(failure_key)

        now_et = MarketTime.now()
        expected_session = _latest_completed_session(now_et)
        latest_bar_date = (
            cached["date"].iloc[-1].date()
            if not cached.empty and "date" in cached.columns else None
        )

        # The candle rows themselves are permanent SQLite history. Their age in
        # hours is irrelevant: only a newly completed exchange session can make
        # this cache stale. Weekends and holidays therefore cause no provider call.
        # Older Yahoo caches were dividend-adjusted while Longbridge live
        # quotes and fallback providers use traded prices. Rebase those histories
        # once to raw OHLC before allowing live quotes to share the same scale.
        needs_price_basis_rebase = known_source == "Yahoo Finance"
        if len(cached) > 0 and known_source and not needs_price_basis_rebase and (
            expected_session is None
            or (latest_bar_date is not None and latest_bar_date >= expected_session)
        ):
            self.last_source = "SQLite daily cache"
            return 0

        # Keep the last good history if a provider is temporarily unavailable.
        # Retry a stale/missing session after 15 minutes without re-downloading
        # history on every scan or every scheduler wake-up.
        if (
            isinstance(failure_state, dict)
            and failure_state.get("last_failed_attempt")
            and failure_age is not None
            and failure_age < 15 * 60
        ):
            self.last_source = "SQLite daily cache (stale; refresh failed)" if len(cached) else None
            log.warning(
                "%s: retaining %s stored candles, but reporting stale history to caller; "
                "retrying daily refresh after cooldown",
                self.symbol, len(cached),
            )
            raise RuntimeError(
                f"Daily history for {self.symbol} is stale; provider retry is in the 15-minute failure cooldown"
            )

        # Seed missing/legacy data from the full history once. Thereafter fetch
        # only a short overlapping window so a missed session can be recovered.
        period = "2y" if needs_price_basis_rebase else ("10d" if len(cached) > 0 and known_source else initial_period)
        try:
            count = self.fetch_and_store_daily_candles(period, append=True)
            refreshed = self.storage.get_candles_df(self.symbol)
            latest_refreshed_date = (
                refreshed["date"].iloc[-1].date()
                if not refreshed.empty and "date" in refreshed.columns else None
            )

            # Some vendors return HTTP success before publishing the completed
            # daily bar. Do not mark the day refreshed until its date is stored.
            if (
                expected_session is not None
                and (latest_refreshed_date is None or latest_refreshed_date < expected_session)
            ):
                log.warning(
                    "%s: provider has not published completed session %s yet; "
                    "keeping %s stored candles but reporting the refresh as failed",
                    self.symbol, expected_session, len(refreshed),
                )
                raise RuntimeError(
                    f"Provider has not published completed session {expected_session} for {self.symbol}"
                )

            self.storage.set_cache(failure_key, None)
            self.last_source = self.last_source or "SQLite daily cache"
            log.info(
                "%s: daily history current through %s; stored_bars=%s",
                self.symbol, latest_refreshed_date, len(refreshed),
            )
            return count
        except Exception as exc:
            self.storage.set_cache(
                failure_key,
                {"last_failed_attempt": datetime.now(timezone.utc).isoformat()},
            )
            log.warning(
                "%s: daily refresh failed; preserving %s stored candles: %s",
                self.symbol, len(cached), exc,
            )
            if len(cached):
                self.last_source = "SQLite daily cache (stale; last good history preserved)"
            # A preserved cache is not evidence of a successful refresh. Let
            # the screener exclude this symbol from RS/breadth for this run,
            # while leaving its durable candles and existing watchlist card intact.
            raise

    def get_ohlcv_dataframe(self) -> pd.DataFrame:
        return self.storage.get_candles_df(self.symbol)

    def _persist_earnings_events(self, frame: pd.DataFrame) -> None:
        """Backfill durable earnings events without rewriting unchanged quarters."""
        if frame is None or frame.empty or "period" not in frame.columns:
            return
        existing = {
            str(event.get("timestamp")): event.get("payload")
            for event in self.storage.get_events(self.symbol, "earnings", limit=1000)
        }

        def clean_value(value):
            try:
                if pd.isna(value):
                    return None
            except (TypeError, ValueError):
                pass
            return value

        for item in frame.to_dict(orient="records"):
            period = str(item.get("period") or "")
            if not period or period.lower() == "nat":
                continue
            payload = {
                "period": period,
                "eps_current": clean_value(item.get("eps_current")),
                "eps_year_ago": clean_value(item.get("eps_year_ago")),
                "revenue_current": clean_value(item.get("revenue_current")),
                "revenue_year_ago": clean_value(item.get("revenue_year_ago")),
                "source": "yahoo_finance_quarterly",
            }
            event_date = period[:10]
            if existing.get(event_date) == payload:
                continue
            self.storage.add_event(self.symbol, "earnings", event_date, payload)

    def get_fundamentals_dataframe(self) -> pd.DataFrame:
        """Return cached quarterly fundamentals; cache empty responses briefly to avoid request storms."""
        cache_key = f"fundamentals:{self.symbol}"
        failure_key = f"fundamentals_failure:{self.symbol}"
        cached = self.storage.get_cache(cache_key)
        cache_age = self.storage.cache_age_seconds(cache_key)
        failure_state = self.storage.get_cache(failure_key)
        failure_age = self.storage.cache_age_seconds(failure_key)
        if (
            isinstance(cached, list)
            and cached
            and isinstance(failure_state, dict)
            and failure_state.get("last_failed_attempt")
            and failure_age is not None
            and failure_age < 6 * 3600
        ):
            # Do not hammer Yahoo every 15-minute scheduler pass after a
            # failed refresh. Retain the last good data but mark it UNKNOWN.
            frame = pd.DataFrame(cached)
            if "period" in frame.columns:
                frame["period"] = pd.to_datetime(frame["period"], errors="coerce")
            frame = frame.sort_values("period", ascending=False).reset_index(drop=True) if not frame.empty else frame
            frame.attrs["stale"] = True
            self._persist_earnings_events(frame)
            return frame

        if isinstance(cached, list):
            # Quarterly statements are checked daily so newly reported results
            # enter the screener promptly. Empty/error responses retry after six hours.
            max_age = 24 * 3600 if cached else 6 * 3600
            if cache_age is not None and cache_age < max_age:
                frame = pd.DataFrame(cached)
                if "period" in frame.columns:
                    frame["period"] = pd.to_datetime(frame["period"], errors="coerce")
                frame = frame.sort_values("period", ascending=False).reset_index(drop=True) if not frame.empty else frame
                # The cache may have been committed immediately before a
                # process restart interrupted event persistence. Replaying the
                # idempotent upsert on cache hits repairs that partial write.
                self._persist_earnings_events(frame)
                return frame

        try:
            frame = self._fetch_fundamentals_dataframe()
        except Exception as exc:
            if isinstance(cached, list) and cached:
                # A transient provider failure must not overwrite the last
                # good quarterly statements with an empty cache entry. Return
                # the retained data as explicitly stale so screening treats
                # eligibility as unknown until a successful refresh.
                frame = pd.DataFrame(cached)
                if "period" in frame.columns:
                    frame["period"] = pd.to_datetime(frame["period"], errors="coerce")
                frame = frame.sort_values("period", ascending=False).reset_index(drop=True) if not frame.empty else frame
                frame.attrs["stale"] = True
                self.storage.set_cache(
                    failure_key,
                    {"last_failed_attempt": datetime.now(timezone.utc).isoformat()},
                )
                self._persist_earnings_events(frame)
                log.warning(
                    "%s: quarterly fundamentals refresh failed; retaining stale cached statements: %s",
                    self.symbol, exc,
                )
                return frame
            # Cache provider failures as an empty response so the normal
            # six-hour retry policy applies instead of retrying every scan.
            self.storage.set_cache(cache_key, [])
            raise

        records = frame.copy()
        if "period" in records.columns:
            records["period"] = records["period"].astype(str)
        payload = records.astype(object).where(pd.notna(records), None).to_dict(orient="records")
        # Persist empty results too, with a shorter retry window above.
        self.storage.set_cache(cache_key, payload)
        self.storage.set_cache(failure_key, None)
        self._persist_earnings_events(frame)
        return frame

    def _fetch_fundamentals_dataframe(self) -> pd.DataFrame:
        """Fetch quarterly fundamentals; absent fields remain NaN, never fabricated."""

        try:
            _wait_for_yahoo_request_slot()
            q = self.ticker.quarterly_financials
        except Exception as exc:
            log.exception("Quarterly fundamentals request failed for %s", self.symbol)
            raise RuntimeError(
                f"Quarterly fundamentals request failed for {self.symbol}"
            ) from exc
        # Do not call ticker.info here: Yahoo's quoteSummary endpoint frequently
        # returns HTTP 401 (Invalid Crumb / Unauthorized). ROE is supplementary;
        # EPS and revenue growth remain available from quarterly_financials.
        if q is None or q.empty:
            return pd.DataFrame()

        eps = q.loc["Diluted EPS"].dropna() if "Diluted EPS" in q.index else pd.Series(dtype=float)
        rev = q.loc["Total Revenue"].dropna() if "Total Revenue" in q.index else pd.Series(dtype=float)

        # Keep the newest reported period even when Yahoo omits one metric.
        # An intersection silently drops that quarter and makes an older
        # quarter look current; the screener must classify missing data UNKNOWN.
        dates = sorted(set(eps.index) | set(rev.index), reverse=True)
        if not dates:
            return pd.DataFrame()
        rows = []
        for d in dates:
            d_ts = pd.Timestamp(d)
            e = pd.to_numeric(eps.get(d, float("nan")), errors="coerce")
            r = pd.to_numeric(rev.get(d, float("nan")), errors="coerce")

            prior_target = d_ts - pd.DateOffset(months=12)
            prior_e, prior_r = _find_nearest_prior_period(
                eps, rev, prior_target, max_window_days=60
            )

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
        """Return durable news; refresh from Yahoo at most every four hours."""
        cache_key = f"news:{self.symbol}"
        cached = self.storage.get_cache(cache_key)
        cache_age = self.storage.cache_age_seconds(cache_key)
        if isinstance(cached, list):
            # Non-empty news is refreshed every four hours; empty/error
            # responses retry hourly instead of hammering the provider.
            max_age = 4 * 3600 if cached else 3600
            if cache_age is not None and cache_age < max_age:
                return cached
        try:
            _wait_for_yahoo_request_slot()
            raw_items = getattr(self.ticker, "news", None) or []
        except Exception:
            log.exception("Yahoo news request failed for %s", self.symbol)
            self.storage.set_cache(cache_key, [])
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
        # Cache successful empty responses too, avoiding repeated provider calls.
        self.storage.set_cache(cache_key, stored)
        return stored
