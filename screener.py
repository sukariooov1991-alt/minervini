"""Daily SEPA candidate scanner for minervini_v2 structure."""
from __future__ import annotations

import argparse
import json
import logging
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
                n = self._get_provider(sym).fetch_and_store_daily_candles(period)
                log.info("[%s/%s] %s: stored %s bars", i, total, sym, n)
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

    def compute_rs_ratings(self, exclude_symbols: set[str] | None = None) -> tuple[dict, dict]:
        prices: dict[str, pd.Series] = {}
        frames: dict[str, pd.DataFrame] = {}
        excluded = {str(sym).upper().strip() for sym in (exclude_symbols or set())}

        for sym in self.symbols:
            if sym.upper().strip() in excluded:
                continue
            df = self.storage.get_candles_df(sym)
            if len(df) >= 252:
                frames[sym] = df
                prices[sym] = df.set_index("date")["close"]

        if not prices:
            return {}, frames

        rs = rank_relative_strength(prices)
        return dict(zip(rs.symbol, rs.rs_rating)), frames

    def _fetch_market_index(self, market_symbol: str, period: str) -> pd.DataFrame:
        try:
            dp = DataProvider(market_symbol, self.storage)
            dp.fetch_and_store_daily_candles(period)
            return self.storage.get_candles_df(market_symbol)
        except Exception as exc:
            log.warning("Market index %s: %s", market_symbol, exc)
            return pd.DataFrame()

    def check_market(self, market_symbol: str = "^GSPC", period: str = "2y") -> dict:
        index_df = self._fetch_market_index(market_symbol, period)
        status = check_market_regime(index_df)
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

        market_status = (
            {"market_favorable": True, "regime": "SKIPPED", "reasons": ["market check disabled"]}
            if skip_market_check else self.check_market(market_symbol, period)
        )
        market_favorable = bool(market_status.get("market_favorable", False))

        # Complete the download phase before calculating cross-sectional RS.
        # Otherwise early batches would be ranked against stale rows from symbols
        # that have not yet been refreshed in this scan.
        download_failed_symbols: set[str] = set()
        scan_checkpoint_name = "daily_universe_update"
        completed_downloads: set[str] = set()
        if update:
            # Successful downloads are checkpointed in SQLite after each symbol.
            # If Render/Gunicorn restarts the worker, the next scan resumes at the
            # first unfinished symbol instead of starting the 495-symbol universe over.
            completed_downloads = self.storage.get_scan_checkpoint(
                scan_checkpoint_name, self.symbols
            )
            for i, sym in enumerate(self.symbols, 1):
                if stop_callback and stop_callback():
                    stopped = True
                    break
                if sym in completed_downloads:
                    processed = i
                    if progress_callback:
                        progress_callback({
                            "phase": "استئناف تحميل البيانات",
                            "completed": processed,
                            "total": progress_total,
                            "batch": (i - 1) // batch_size + 1,
                            "batch_total": (total + batch_size - 1) // batch_size,
                            "current_symbol": sym,
                        })
                    continue
                try:
                    n = self._get_provider(sym).fetch_and_store_daily_candles(period)
                    self.storage.mark_scan_symbol_completed(scan_checkpoint_name, sym)
                    completed_downloads.add(sym)
                    log.info("[%s/%s] %s: stored %s bars", i, total, sym, n)
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
                    if df is None or len(df) < 252:
                        log.warning(
                            "Skipping %s during analysis: insufficient daily candles (%s; need 252)",
                            sym, 0 if df is None else len(df),
                        )
                        continue

                    try:
                        rating = rs.get(sym, float("nan"))
                        tt = check_trend_template(df, rating)
                        if not tt["passed"]:
                            continue

                        # Exclude the current candle from pattern construction;
                        # breakout validation is performed against the latest candle.
                        base_df = df.iloc[:-1].copy()
                        vcp = check_vcp(base_df)
                        pivot_val = vcp.get("pivot_price")
                        br = check_breakout(df, pivot=pivot_val) if vcp.get("passed") else False
                        pb = check_pullback(df)
                        ad = accumulation_distribution_summary(df)

                        try:
                            fdf = self._get_provider(sym).get_fundamentals_dataframe()
                            latest = fdf.iloc[0].to_dict() if not fdf.empty else {}
                            earnings = check_earnings(
                                latest.get("eps_current"),
                                latest.get("eps_year_ago"),
                                latest.get("revenue_current"),
                                latest.get("revenue_year_ago"),
                            )
                            fundamental_status = (
                                "UNKNOWN" if fdf.empty
                                else ("PASS" if earnings["passed"] else "FAIL")
                            )
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
                            "pivot_price": pivot_val,
                            "final_low": vcp.get("final_low"),
                            "contraction_depths": vcp.get("contraction_depths", []),
                            "volume_dry_up": vcp.get("volume_dry_up", False),
                            "accumulation": ad.get("accumulation", False),
                            "distribution": ad.get("distribution", False),
                            "earnings_status": fundamental_status,
                            "earnings_passed": bool(earnings.get("passed", False)),
                            "market_favorable": market_favorable,
                            "market_regime": market_status.get("regime", "UNKNOWN"),
                            "passed_all_filters": bool(
                                vcp.get("passed", False)
                                and br
                                and pb.get("passed", False)
                                and ad.get("accumulation", False)
                                and earnings.get("passed", False)
                                and market_favorable
                            ),
                            "screen_date": datetime.now(timezone.utc).date().isoformat(),
                        }
                        rows.append(row)
                        if batch_callback:
                            try:
                                batch_callback([row], market_status)
                            except Exception:
                                log.exception(
                                    "Result publish failed for %s in batch %s",
                                    sym, batch_no,
                                )
                    except Exception as exc:
                        log.exception("Unexpected analysis error for symbol %s", sym)
                        failures.append({
                            "symbol": sym,
                            "error": str(exc),
                            "phase": "analysis",
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

        result = pd.DataFrame(rows)
        if not result.empty:
            result = result.sort_values(
                ["passed_all_filters", "breakout", "vcp_passed", "rs_rating"],
                ascending=[False, False, False, False],
            )

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "strategy": "Minervini SEPA — TT gate, VCP, breakout, pullback, AD, earnings, market regime",
            "market_status": market_status,
            "failures": failures,
            "results": result.to_dict(orient="records"),
        }
        self.output_path.write_text(
            json.dumps(payload, indent=2, default=str),
            encoding="utf-8",
        )
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
