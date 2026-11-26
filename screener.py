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
from data.storage_manager import StorageManager
from data.sp500_symbols import SP500_SYMBOLS

log = logging.getLogger("minervini_v2")


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
            if len(df) >= 253:
                prices[sym] = df.set_index("date")["close"]

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
        return above / participating * 100.0 if participating else None

    def check_market(
        self, market_symbol: str = "^GSPC", period: str = "2y",
        breadth_pct: float | None = None,
    ) -> dict:
        index_df = self._fetch_market_index(market_symbol, period)
        status = check_market_regime(index_df, breadth_pct=breadth_pct)
        status["breadth_pct"] = breadth_pct
        status["breadth_universe_count"] = len(self.symbols)
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
        completed_downloads: set[str] = set()
        if update:
            # Record per-symbol scan progress in SQLite for restart diagnostics.
            # The durable candle cache independently decides whether a provider
            # refresh is due, so completed symbols are still checked cheaply.
            completed_downloads = self.storage.get_scan_checkpoint(
                scan_checkpoint_name, self.symbols
            )
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
                    cached_count = len(self.storage.get_candles_df(sym))
                    log.info(
                        "[%s/%s] %s: refresh_rows=%s cache_bars=%s",
                        i, total, sym, refreshed, cached_count,
                    )
                except Exception as exc:
                    download_failed_symbols.add(sym)
                    failures.append({"symbol": sym, "error": str(exc), "phase": "download"})
                    log.warning("Data download failed for %s: %s", sym, exc)
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

                # Publish completed analysis results once per five-symbol batch.
                # This lets the realtime layer subscribe/seed quotes in groups
                # instead of making a separate Longbridge API round-trip per stock.
                batch_rows: list[dict] = []
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

                    try:
                        rating = rs.get(sym, float("nan"))
                        tt = check_trend_template(df, rating)
                        if not tt["passed"]:
                            # A valid completed TT failure is a definitive
                            # ineligibility result and can safely reconcile a card.
                            evaluated_symbols.add(sym)
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

                        try:
                            fdf = self._get_provider(sym).get_fundamentals_dataframe()
                            latest = fdf.iloc[0].to_dict() if not fdf.empty else {}
                            if fdf.empty:
                                failures.append({
                                    "symbol": sym,
                                    "error": "Fundamental data unavailable; eligibility cannot be confirmed",
                                    "phase": "fundamentals",
                                })
                            missing_fields = _missing_fundamental_fields(latest)

                            if fdf.empty or missing_fields:
                                earnings = {"passed": False}
                                fundamental_status = "UNKNOWN"
                                if not fdf.empty and missing_fields:
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
                            "pivot_price": pivot_val,
                            "final_low": vcp.get("final_low") if vcp.get("passed") else pb.get("support_low"),
                            "pullback_entry_price": pb.get("entry_trigger_price"),
                            "pullback_final_low": pb.get("support_low"),
                            "entry_confirmation_passed": entry_confirmation_passed,
                            "contraction_depths": vcp.get("contraction_depths", []),
                            "volume_dry_up": vcp.get("volume_dry_up", False),
                            "accumulation": ad.get("accumulation", False),
                            "distribution": ad.get("distribution", False),
                            "earnings_status": fundamental_status,
                            "earnings_passed": bool(earnings.get("passed", False)),
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
                            "passed_all_filters": bool(
                                entry_confirmation_passed
                                and ad.get("accumulation", False)
                                and earnings.get("passed", False)
                                and market_favorable
                            ),
                            "screen_date": datetime.now(timezone.utc).date().isoformat(),
                        }
                        if row["watchlist_eligible"]:
                            try:
                                # News is strategy context, not a per-scan dependency.
                                # DataProvider serves its durable six-hour cache.
                                self._get_provider(sym).fetch_and_store_news()
                            except Exception:
                                log.exception("News refresh failed for eligible symbol %s", sym)
                        rows.append(row)
                        batch_rows.append(row)
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
                if batch_callback and batch_rows:
                    try:
                        batch_callback(batch_rows, market_status)
                    except Exception:
                        log.exception(
                            "Result publish failed for batch %s (%s symbols)",
                            batch_no, len(batch_rows),
                        )
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
        market_status = {**market_status, "evaluated_symbols": sorted(evaluated_symbols)}
        result = pd.DataFrame(rows)
        if not result.empty:
            result = result.sort_values(
                ["passed_all_filters", "breakout", "vcp_passed", "rs_rating"],
                ascending=[False, False, False, False],
            )

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "strategy": "Minervini SEPA — watchlist: Trend Template + Accumulation + Earnings; setup: VCP OR Pullback; market regime evaluated after setup",
            "market_status": market_status,
            "failures": failures,
            "results": result.to_dict(orient="records"),
        }
        # Write to a sibling temporary file and atomically replace the last
        # complete snapshot. Startup recovery must never read a half-written JSON.
        temp_output = self.output_path.with_name(
            f"{self.output_path.name}.{os.getpid()}.tmp"
        )
        temp_output.write_text(
            json.dumps(payload, indent=2, default=str),
            encoding="utf-8",
        )
        os.replace(temp_output, self.output_path)
        # Keep the checkpoint if the user stopped the scan or if any symbol
        # failed to download; otherwise the next run is a genuinely fresh scan.
        if update and not stopped and not download_failed_symbols:
            self.storage.clear_scan_checkpoint(scan_checkpoint_name)
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
