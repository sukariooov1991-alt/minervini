"""Daily SEPA candidate scanner for minervini_v2 structure."""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from analysis.relative_strength import rank_relative_strength
from analysis.trend_template import check_trend_template
from analysis.vcp import check_vcp, check_breakout
from analysis.pullback import check_pullback
from analysis.accumulation_distribution import accumulation_distribution_summary
from analysis.earnings import check_earnings
from analysis.market_filter import check_market_regime
from data.data_provider import DataProvider
from data.trade_levels import build_trade_levels
from data.market_time import MarketTime
from data.storage_manager import StorageManager
from data.sp500_symbols import SP500_SYMBOLS

log = logging.getLogger("minervini_v2")
MIN_UNIVERSE_DATA_COVERAGE = 0.80


FUNDAMENTAL_FIELDS = (
    "eps_current", "eps_year_ago", "revenue_current", "revenue_year_ago",
)


def _missing_fundamental_fields(values: dict) -> list[str]:
    """Return required quarterly fields that are absent, nonnumeric, or nonfinite."""
    missing = []
    for field in FUNDAMENTAL_FIELDS:
        try:
            if not math.isfinite(float(values.get(field))):
                missing.append(field)
        except (TypeError, ValueError):
            missing.append(field)
    return missing


def _entry_ready_from_plan(
    trend_template_passed: bool,
    accumulation_passed: bool,
    earnings_passed: bool,
    entry_confirmation_passed: bool,
    market_favorable: bool,
    plan: dict,
) -> bool:
    """Apply every stock, setup, market, and trade-plan gate to final entry readiness."""
    return bool(
        trend_template_passed
        and accumulation_passed
        and earnings_passed
        and entry_confirmation_passed
        and market_favorable
        and plan.get("status") == "جاهز"
        and plan.get("within_entry_range") is True
    )


class DailyScreener:
    def __init__(
        self,
        db_path: str = "stock_data.db",
        output_path: str = "watchlist.json",
        symbols: list[str] | None = None,
    ):
        self.storage = StorageManager(db_path)
        self.output_path = Path(output_path)
        if symbols is None:
            selected_symbols = SP500_SYMBOLS
        elif isinstance(symbols, str):
            selected_symbols = [symbols]
        else:
            selected_symbols = symbols
        self.symbols = [
            str(sym).upper().strip()
            for sym in selected_symbols
            if str(sym).strip()
        ]
        self._providers: dict[str, DataProvider] = {}

    def _get_provider(self, symbol: str) -> DataProvider:
        if symbol not in self._providers:
            self._providers[symbol] = DataProvider(symbol, self.storage)
        return self._providers[symbol]

    def update_universe_data(self, period: str = "2y", progress_callback=None) -> list[dict]:
        failures: list[dict] = []
        total = len(self.symbols)
        for i, sym in enumerate(self.symbols, 1):
            try:
                refreshed = self._get_provider(sym).ensure_daily_history(initial_period=period)
                cached_count = len(self.storage.get_candles_df(sym))
                log.info(
                    "[%s/%s] %s: refresh_rows=%s cache_bars=%s",
                    i, total, sym, refreshed, cached_count,
                )
            except Exception as exc:
                failures.append({"symbol": sym, "error": str(exc)})
                log.warning("%s: %s", sym, exc)
            if progress_callback:
                progress_callback({
                    "phase": "تحميل البيانات",
                    "completed": i,
                    "total": total,
                    "current_symbol": sym,
                })
        return failures

    def refresh_universe_research_data(self) -> dict[str, list[dict]]:
        """Refresh cached quarterly fundamentals daily and news every four hours."""
        failures: dict[str, list[dict]] = {"fundamentals": [], "news": []}
        total = len(self.symbols)
        for index, symbol in enumerate(self.symbols, 1):
            provider = self._get_provider(symbol)
            try:
                fundamentals = provider.get_fundamentals_dataframe()
                log.debug(
                    "[research %s/%s] %s quarterly periods=%s",
                    index, total, symbol, len(fundamentals),
                )
            except Exception as exc:
                failures["fundamentals"].append({"symbol": symbol, "error": str(exc)})
                log.warning("%s quarterly fundamentals refresh failed: %s", symbol, exc)
            try:
                news = provider.fetch_and_store_news()
                log.debug(
                    "[research %s/%s] %s news_items=%s",
                    index, total, symbol, len(news),
                )
            except Exception as exc:
                failures["news"].append({"symbol": symbol, "error": str(exc)})
                log.warning("%s news refresh failed: %s", symbol, exc)
        log.info(
            "Background research refresh: symbols=%s fundamentals_failures=%s news_failures=%s",
            total, len(failures["fundamentals"]), len(failures["news"]),
        )
        return failures

    def compute_rs_ratings(self, exclude_symbols: set[str] | None = None) -> tuple[dict, dict]:
        prices: dict[str, pd.Series] = {}
        frames: dict[str, pd.DataFrame] = {}
        excluded = {str(sym).upper().strip() for sym in (exclude_symbols or set())}

        for sym in self.symbols:
            if sym.upper().strip() in excluded:
                continue
            df = self.storage.get_candles_df(sym)
            # RS needs 253 closes to compare the latest close with the close 252 sessions earlier.
            if len(df) >= 253:
                frames[sym] = df
                prices[sym] = df.set_index("date")["close"]

        eligible_universe = max(0, len(self.symbols) - len(excluded))
        required_coverage = max(1, math.ceil(eligible_universe * MIN_UNIVERSE_DATA_COVERAGE))
        if len(prices) < required_coverage:
            log.error(
                "Relative-strength universe coverage is insufficient: valid=%s required=%s universe=%s; "
                "refusing to rank a small biased subset",
                len(prices), required_coverage, eligible_universe,
            )
            return {}, frames
        if not prices:
            return {}, frames

        rs = rank_relative_strength(prices)
        return dict(zip(rs.symbol, rs.rs_rating)), frames

    def _fetch_market_index(self, market_symbol: str, period: str) -> pd.DataFrame:
        try:
            dp = DataProvider(market_symbol, self.storage)
            dp.ensure_daily_history(initial_period=period)
            return self.storage.get_candles_df(market_symbol)
        except Exception as exc:
            log.warning("Market index %s: %s", market_symbol, exc)
            return pd.DataFrame()

    def compute_market_breadth(self, exclude_symbols: set[str] | None = None) -> float | None:
        """Percent above 50-day SMA, excluding symbols with failed current downloads."""
        participating = 0
        above = 0
        excluded = {str(symbol).upper().strip() for symbol in (exclude_symbols or set())}
        eligible_universe = max(0, len(self.symbols) - len(excluded))
        self._last_breadth_universe_count = eligible_universe
        for symbol in self.symbols:
            if symbol.upper().strip() in excluded:
                continue
            try:
                frame = self.storage.get_candles_df(symbol)
                if frame is None or len(frame) < 50 or "close" not in frame.columns:
                    continue
                close = pd.to_numeric(frame["close"], errors="coerce").dropna()
                if len(close) < 50:
                    continue
                sma50 = float(close.tail(50).mean())
                last = float(close.iloc[-1])
                if not (pd.notna(sma50) and sma50 > 0 and pd.notna(last)):
                    continue
                participating += 1
                above += int(last > sma50)
            except Exception as exc:
                log.debug("Breadth skipped %s: %s", symbol, exc)
        self._last_breadth_participating = participating
        required_coverage = max(1, math.ceil(eligible_universe * MIN_UNIVERSE_DATA_COVERAGE))
        if participating < required_coverage:
            log.error(
                "Market-breadth coverage is insufficient: valid=%s required=%s universe=%s; "
                "breadth remains UNKNOWN",
                participating, required_coverage, eligible_universe,
            )
            return None
        return above / participating * 100.0 if participating else None

    def check_market(
        self, market_symbol: str = "^GSPC", period: str = "2y",
        breadth_pct: float | None = None,
    ) -> dict:
        index_df = self._fetch_market_index(market_symbol, period)
        status = check_market_regime(index_df, breadth_pct=breadth_pct)
        status["breadth_pct"] = breadth_pct
        status["breadth_universe_count"] = getattr(self, "_last_breadth_universe_count", len(self.symbols))
        status["breadth_participating_count"] = getattr(self, "_last_breadth_participating", 0)
        status["breadth_coverage_pct"] = (
            status["breadth_participating_count"] / status["breadth_universe_count"] * 100.0
            if status["breadth_universe_count"] else 0.0
        )
        log.info(
            "Market regime: %s | favorable=%s | distribution_days=%s",
            status.get("regime"), status.get("market_favorable"),
            status.get("distribution_days"),
        )
        return status

    def run_screen(
        self, update: bool = True, period: str = "2y", market_symbol: str = "^GSPC",
        skip_market_check: bool = False, progress_callback=None, batch_callback=None,
        stop_callback=None, batch_size: int = 5,
    ) -> tuple[pd.DataFrame, list[dict], dict]:
        failures: list[dict] = []
        rows: list[dict] = []
        # Symbols are added only after their eligibility decision is complete.
        # This is separate from result rows, which intentionally omit TT failures.
        evaluated_symbols: set[str] = set()
        trend_template_failures: list[dict] = []
        total = len(self.symbols)
        processed = 0
        analysis_processed = 0
        progress_total = total * 2
        stopped = False

        if progress_callback:
            progress_callback({
                "phase": "فحص حالة السوق",
                "completed": 0,
                "total": progress_total,
                "batch": 0,
                "batch_total": (total + batch_size - 1) // batch_size,
                "current_symbol": market_symbol,
            })

        # Complete the download phase before calculating cross-sectional RS.
        # Otherwise early batches would be ranked against stale rows from symbols
        # that have not yet been refreshed in this scan.
        download_failed_symbols: set[str] = set()
        scan_checkpoint_name = "daily_universe_update"
        if update:
            # Record per-symbol scan progress in SQLite for restart diagnostics.
            # The durable candle cache independently decides whether a provider
            # refresh is due, so completed symbols are still checked cheaply.
            self.storage.get_scan_checkpoint(scan_checkpoint_name, self.symbols)
            for i, sym in enumerate(self.symbols, 1):
                if stop_callback and stop_callback():
                    stopped = True
                    break
                # Checkpoints preserve restart visibility, but the cache itself
                # decides whether this symbol needs a provider refresh. Always
                # run ensure_daily_history so a stale cache cannot be skipped.
                try:
                    refreshed = self._get_provider(sym).ensure_daily_history(initial_period=period)
                    self.storage.mark_scan_symbol_completed(scan_checkpoint_name, sym)
                    cached_df = self.storage.get_candles_df(sym)
                    cached_count = len(cached_df)
                    latest_date = (
                        cached_df["date"].iloc[-1].date().isoformat()
                        if cached_count and "date" in cached_df.columns else "UNKNOWN"
                    )
                    source = self._get_provider(sym).last_source or "UNKNOWN"
                    log.info(
                        "STEP=DAILY_CANDLES STATUS=SUCCESS symbol=%s progress=%s/%s "
                        "new_rows=%s stored_candles=%s latest_date=%s source=%s",
                        sym, i, total, refreshed, cached_count, latest_date, source,
                    )
                except Exception as exc:
                    download_failed_symbols.add(sym)
                    failures.append({"symbol": sym, "error": str(exc), "phase": "download"})
                    log.exception(
                        "STEP=DAILY_CANDLES STATUS=FAILED symbol=%s progress=%s/%s reason=%s",
                        sym, i, total, exc,
                    )
                    if progress_callback:
                        progress_callback({
                            "phase": "خطأ أثناء تحميل البيانات",
                            "completed": processed,
                            "total": progress_total,
                            "current_symbol": sym,
                            "error": str(exc),
                        })
                processed = i
                if progress_callback:
                    progress_callback({
                        "phase": "تحميل البيانات",
                        "completed": processed,
                        "total": progress_total,
                        "batch": (i - 1) // batch_size + 1,
                        "batch_total": (total + batch_size - 1) // batch_size,
                        "current_symbol": sym,
                    })
        else:
            processed = total
            if progress_callback:
                progress_callback({
                    "phase": "تحميل البيانات",
                    "completed": total,
                    "total": progress_total,
                    "current_symbol": "",
                })

        # Breadth is computed after the universe refresh, so it reflects the
        # same stored dataset used by the cross-sectional stock analysis.
        breadth_pct = self.compute_market_breadth(
            exclude_symbols=download_failed_symbols if update else None
        )
        market_status = (
            {"market_favorable": True, "regime": "SKIPPED", "breadth_pct": breadth_pct,
             "reasons": ["market check disabled"]}
            if skip_market_check
            else self.check_market(market_symbol, period, breadth_pct=breadth_pct)
        )
        market_favorable = bool(market_status.get("market_favorable", False))

        # A stopped or failed download is never silently replaced by old stored
        # candles. Failed symbols are excluded from both RS ranking and analysis.
        if stopped or (stop_callback and stop_callback()):
            stopped = True
            rs, frames = {}, {}
        else:
            rs, frames = self.compute_rs_ratings(
                exclude_symbols=download_failed_symbols if update else None
            )

        batches = [
            self.symbols[i:i + batch_size]
            for i in range(0, total, batch_size)
        ]
        if not stopped:
            for batch_no, batch in enumerate(batches, 1):
                if stop_callback and stop_callback():
                    stopped = True
                    break
                if progress_callback:
                    progress_callback({
                        "phase": "تحميل الدفعة",
                        "completed": total + analysis_processed,
                        "total": progress_total,
                        "batch": batch_no,
                        "batch_total": len(batches),
                        "batch_size": len(batch),
                        "current_symbol": batch[0] if batch else "",
                    })

                for sym in batch:
                    if stop_callback and stop_callback():
                        stopped = True
                        break
                    analysis_processed += 1
                    if progress_callback:
                        progress_callback({
                            "phase": "تحليل الدفعة",
                            "completed": total + analysis_processed,
                            "total": progress_total,
                            "batch": batch_no,
                            "current_symbol": sym,
                        })

                    if update and sym in download_failed_symbols:
                        continue
                    df = frames.get(sym)
                    if df is None:
                        df = self.storage.get_candles_df(sym)
                    # A 252-session return requires 253 closing prices. Do not
                    # treat a missing RS rating as a genuine Trend Template failure.
                    if len(df) < 253:
                        available = len(df)
                        log.warning(
                            "Skipping %s during analysis: insufficient daily candles (%s; need 253)",
                            sym, available,
                        )
                        failures.append({
                            "symbol": sym,
                            "error": f"Insufficient daily candles: {available}; 253 required for RS",
                            "phase": "history",
                        })
                        continue

                    if sym not in rs or not math.isfinite(float(rs.get(sym, float("nan")))):
                        failures.append({
                            "symbol": sym,
                            "error": "Relative-strength rating unavailable or universe coverage below threshold",
                            "phase": "relative_strength",
                        })
                        continue

                    try:
                        rating = rs[sym]
                        tt = check_trend_template(df, rating)
                        if not tt["passed"]:
                            # Preserve the exact failed conditions for watchlist
                            # cards; a failed scan gate changes eligibility but
                            # must not silently delete a user's saved card.
                            evaluated_symbols.add(sym)
                            trend_template_failures.append({
                                "symbol": sym,
                                "rs_rating": rating,
                                "trend_conditions": tt.get("conditions", {}),
                            })
                            failed_conditions = [
                                name for name, passed in tt.get("conditions", {}).items()
                                if not passed
                            ]
                            log.info(
                                "STEP=STOCK_ANALYSIS STATUS=COMPLETED symbol=%s eligible=false "
                                "reason=trend_template_failed failed_conditions=%s",
                                sym, ",".join(failed_conditions) or "not_reported",
                            )
                            continue

                        # Exclude the current candle from pattern construction;
                        # breakout validation is performed against the latest candle.
                        base_df = df.iloc[:-1].copy()
                        vcp = check_vcp(base_df)
                        pivot_val = vcp.get("pivot_price")
                        br = check_breakout(df, pivot=pivot_val) if vcp.get("passed") else False
                        pb = check_pullback(df)
                        entry_confirmation_passed = bool(
                            (vcp.get("passed", False) and br)
                            or pb.get("passed", False)
                        )
                        ad = accumulation_distribution_summary(df)

                        quarterly_eps_growth_history: list[float | None] = []
                        quarterly_revenue_growth_history: list[float | None] = []
                        prior_eps_growth_pct = None
                        try:
                            fdf = self._get_provider(sym).get_fundamentals_dataframe()
                            latest = fdf.iloc[0].to_dict() if not fdf.empty else {}
                            if not fdf.empty and "eps_growth_pct" in fdf.columns:
                                for value in fdf["eps_growth_pct"].head(4):
                                    try:
                                        number = float(value)
                                        quarterly_eps_growth_history.append(
                                            number if math.isfinite(number) else None
                                        )
                                    except (TypeError, ValueError):
                                        quarterly_eps_growth_history.append(None)
                                if len(fdf) > 1:
                                    prior_eps_growth_pct = fdf.iloc[1].get("eps_growth_pct")
                            if not fdf.empty and "revenue_growth_pct" in fdf.columns:
                                for value in fdf["revenue_growth_pct"].head(4):
                                    try:
                                        number = float(value)
                                        quarterly_revenue_growth_history.append(
                                            number if math.isfinite(number) else None
                                        )
                                    except (TypeError, ValueError):
                                        quarterly_revenue_growth_history.append(None)
                            if fdf.empty:
                                failures.append({
                                    "symbol": sym,
                                    "error": "Fundamental data unavailable; eligibility cannot be confirmed",
                                    "phase": "fundamentals",
                                })
                            missing_fields = _missing_fundamental_fields(latest)
                            stale_fundamentals = bool(fdf.attrs.get("stale", False))

                            if fdf.empty or stale_fundamentals or missing_fields:
                                earnings = {"passed": False}
                                fundamental_status = "UNKNOWN"
                                if stale_fundamentals:
                                    failures.append({
                                        "symbol": sym,
                                        "error": "Quarterly fundamentals refresh failed; retained statements are stale",
                                        "phase": "fundamentals",
                                    })
                                elif not fdf.empty and missing_fields:
                                    failures.append({
                                        "symbol": sym,
                                        "error": "Incomplete quarterly fundamentals; missing or invalid: "
                                                 + ", ".join(missing_fields),
                                        "phase": "fundamentals",
                                    })
                            else:
                                earnings = check_earnings(
                                    latest.get("eps_current"),
                                    latest.get("eps_year_ago"),
                                    latest.get("revenue_current"),
                                    latest.get("revenue_year_ago"),
                                    prior_eps_growth_pct=prior_eps_growth_pct,
                                )
                                fundamental_status = "PASS" if earnings["passed"] else "FAIL"
                        except Exception as exc:
                            log.exception("Fundamental data error for %s", sym)
                            if progress_callback:
                                progress_callback({
                                    "phase": "خطأ في البيانات الأساسية",
                                    "completed": total + analysis_processed,
                                    "total": progress_total,
                                    "batch": batch_no,
                                    "batch_total": len(batches),
                                    "current_symbol": sym,
                                    "error": str(exc),
                                })
                            earnings = {"passed": False, "error": str(exc)}
                            fundamental_status = "UNKNOWN"
                            failures.append({
                                "symbol": sym,
                                "error": f"Fundamental data error: {exc}",
                                "phase": "analysis_error",
                            })

                        # Final entry readiness must include the actual trade plan.
                        # A confirmed setup alone is not an executable entry when
                        # the structural stop, 10% risk cap, targets, or buy range fail.
                        use_vcp_entry = bool(vcp.get("passed") and br)
                        use_pullback_entry = bool(pb.get("passed", False) and not use_vcp_entry)
                        active_pivot = (
                            pivot_val if use_vcp_entry
                            else pb.get("entry_trigger_price") if use_pullback_entry
                            else None
                        )
                        active_low = (
                            vcp.get("final_low") if use_vcp_entry
                            else pb.get("support_low") if use_pullback_entry
                            else None
                        )
                        last_close = float(
                            pd.to_numeric(df["close"], errors="coerce").dropna().iloc[-1]
                        )
                        trade_plan = (
                            build_trade_levels(
                                symbol=sym,
                                current_price=last_close,
                                pivot_price=active_pivot,
                                candles_df=df,
                                final_low=active_low,
                            ).to_dict()
                            if entry_confirmation_passed
                            else {
                                "pivot_price": None,
                                "final_low": None,
                                "entry_zones": [],
                                "targets": [],
                                "shared_stop_loss": None,
                                "status": "بانتظار تأكيد نموذج الدخول",
                                "within_entry_range": None,
                            }
                        )
                        entry_ready = _entry_ready_from_plan(
                            tt["passed"],
                            bool(ad.get("accumulation", False)),
                            bool(earnings.get("passed", False)),
                            entry_confirmation_passed,
                            market_favorable,
                            trade_plan,
                        )

                        provider_source = (
                            self._get_provider(sym).last_source if update
                            else "Stored daily data"
                        )
                        row = {
                            "symbol": sym,
                            "data_source": provider_source,
                            "rs_rating": rating,
                            "trend_template": "PASS",
                            "trend_conditions": tt.get("conditions", {}),
                            "vcp_passed": bool(vcp.get("passed")),
                            "breakout": bool(br),
                            "pullback_passed": bool(pb.get("passed", False)),
                            "pullback_candidate": bool(pb.get("candidate", False)),
                            "pullback_turning_up": bool(pb.get("turning_up", False)),
                            "pivot_price": active_pivot if entry_confirmation_passed else pivot_val,
                            "final_low": active_low if entry_confirmation_passed else (
                                vcp.get("final_low") if vcp.get("passed") else pb.get("support_low")
                            ),
                            "entry_zones": trade_plan.get("entry_zones", []),
                            "targets": trade_plan.get("targets", []),
                            "shared_stop_loss": trade_plan.get("shared_stop_loss"),
                            "trade_plan_status": trade_plan.get("status"),
                            "within_entry_range": trade_plan.get("within_entry_range"),
                            "entry_ready": entry_ready,
                            "pullback_entry_price": pb.get("entry_trigger_price"),
                            "pullback_final_low": pb.get("support_low"),
                            "entry_confirmation_passed": entry_confirmation_passed,
                            "contraction_depths": vcp.get("contraction_depths", []),
                            "volume_dry_up": vcp.get("volume_dry_up", False),
                            "accumulation": ad.get("accumulation", False),
                            "distribution": ad.get("distribution", False),
                            "earnings_status": fundamental_status,
                            "earnings_passed": bool(earnings.get("passed", False)),
                            "prior_eps_growth_pct": prior_eps_growth_pct,
                            "eps_growth_accelerating": earnings.get("accelerating"),
                            "quarterly_eps_growth_history_pct": quarterly_eps_growth_history,
                            "quarterly_revenue_growth_history_pct": quarterly_revenue_growth_history,
                            "market_favorable": market_favorable,
                            "market_regime": market_status.get("regime", "UNKNOWN"),
                            "watchlist_eligible": bool(
                                tt["passed"]
                                and ad.get("accumulation", False)
                                and earnings.get("passed", False)
                            ),
                            "setup_passed": bool(
                                vcp.get("passed", False)
                                or pb.get("candidate", False)
                            ),
                            "passed_all_filters": entry_ready,
                            "screen_date": MarketTime.now().date().isoformat(),
                        }
                        # Publish an eligible candidate immediately after its
                        # strategy filters are evaluated. Do not wait for the
                        # remaining symbols in this batch (or the full scan).
                        rows.append(row)
                        if row["watchlist_eligible"] and batch_callback:
                            try:
                                batch_callback([row], market_status)
                            except Exception:
                                log.exception(
                                    "Immediate candidate publish failed for %s", sym
                                )
                        if row["watchlist_eligible"]:
                            try:
                                # News is strategy context, not a per-scan dependency.
                                # DataProvider serves its durable six-hour cache.
                                self._get_provider(sym).fetch_and_store_news()
                            except Exception:
                                log.exception("News refresh failed for eligible symbol %s", sym)
                        log.info(
                            "STEP=STOCK_ANALYSIS STATUS=COMPLETED symbol=%s "
                            "trend_template=PASS vcp=%s pullback=%s fundamentals=%s "
                            "accumulation=%s watchlist_eligible=%s setup_passed=%s "
                            "all_filters_passed=%s",
                            sym, row["vcp_passed"], row["pullback_passed"],
                            fundamental_status, row["accumulation"],
                            row["watchlist_eligible"], row["setup_passed"],
                            row["passed_all_filters"],
                        )
                        # Missing fundamentals remain uncertain and therefore
                        # are deliberately excluded from destructive reconciliation.
                        if fundamental_status != "UNKNOWN":
                            evaluated_symbols.add(sym)
                    except Exception as exc:
                        log.exception("Unexpected analysis error for symbol %s", sym)
                        failures.append({
                            "symbol": sym,
                            "error": str(exc),
                            "phase": "analysis_error",
                        })
                        if progress_callback:
                            progress_callback({
                                "phase": "خطأ أثناء تحليل السهم",
                                "completed": total + analysis_processed,
                                "total": progress_total,
                                "batch": batch_no,
                                "batch_total": len(batches),
                                "current_symbol": sym,
                                "error": str(exc),
                            })

                if stopped:
                    break
                if progress_callback:
                    progress_callback({
                        "phase": "اكتملت الدفعة",
                        "completed": total + analysis_processed,
                        "total": progress_total,
                        "batch": batch_no,
                        "batch_total": len(batches),
                        "current_symbol": "",
                    })

        # Keep reconciliation metadata out of the result table/JSON rows.
        market_status = {
            **market_status,
            "evaluated_symbols": sorted(evaluated_symbols),
            "trend_template_failures": trend_template_failures,
        }
        result = pd.DataFrame(rows)
        if not result.empty:
            result = result.sort_values(
                ["passed_all_filters", "breakout", "vcp_passed", "rs_rating"],
                ascending=[False, False, False, False],
            )

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        safe_result = result.replace([math.inf, -math.inf], float("nan"))
        result_records = (
            safe_result.astype(object).where(pd.notna(safe_result), None).to_dict(orient="records")
            if not safe_result.empty else []
        )
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "strategy": "Minervini SEPA — watchlist: Trend Template + Accumulation + Earnings; setup: VCP OR Pullback; market regime evaluated after setup",
            "market_status": market_status,
            "failures": failures,
            "results": result_records,
        }
        # Write to a sibling temporary file and atomically replace the last
        # complete snapshot. Startup recovery must never read a half-written JSON.
        temp_output = self.output_path.with_name(
            f"{self.output_path.name}.{os.getpid()}.tmp"
        )
        temp_output.write_text(
            json.dumps(payload, indent=2, default=str, allow_nan=False),
            encoding="utf-8",
        )
        os.replace(temp_output, self.output_path)
        # Keep the checkpoint if the user stopped the scan or if any symbol
        # failed to download; otherwise the next run is a genuinely fresh scan.
        if update and not stopped and not download_failed_symbols:
            self.storage.clear_scan_checkpoint(scan_checkpoint_name)
        log.info(
            "STEP=SCREENING_SUMMARY STATUS=%s symbols=%s rows=%s failures=%s "
            "download_failures=%s analysis_failures=%s relative_strength_warnings=%s "
            "fundamentals_warnings=%s history_warnings=%s stopped=%s",
            "STOPPED" if stopped else ("COMPLETED_WITH_WARNINGS" if failures else "SUCCESS"),
            total, len(result), len(failures),
            sum(1 for item in failures if item.get("phase") == "download"),
            sum(1 for item in failures if item.get("phase") == "analysis_error"),
            sum(1 for item in failures if item.get("phase") == "relative_strength"),
            sum(1 for item in failures if item.get("phase") == "fundamentals"),
            sum(1 for item in failures if item.get("phase") == "history"),
            stopped,
        )
        return result, failures, market_status


def main():
    parser = argparse.ArgumentParser(description="Minervini SEPA daily screener")
    parser.add_argument("--no-update", action="store_true", help="screen stored data only")
    parser.add_argument("--period", default="2y")
    parser.add_argument("--db", default="stock_data.db")
    parser.add_argument("--output", default="watchlist.json")
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--market-symbol", default="^GSPC")
    parser.add_argument("--skip-market-check", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    scanner = DailyScreener(args.db, args.output, args.symbols)
    result, failures, market_status = scanner.run_screen(
        update=not args.no_update,
        period=args.period,
        market_symbol=args.market_symbol,
        skip_market_check=args.skip_market_check,
    )

    print("\n=== Market Regime ===")
    print(f"regime: {market_status.get('regime')}")
    print(f"favorable: {market_status.get('market_favorable')}")
    print("\n=== Results ===")
    print(result.to_string(index=False) if not result.empty else "No candidates found.")
    print(f"\nFailures: {len(failures)} | Output: {args.output}")


if __name__ == "__main__":
    main()
