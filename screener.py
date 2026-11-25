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
from data.data_provider import DataProvider, YahooRateLimitError
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
        self.symbols = list(symbols or SP500_SYMBOLS)
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
                log.info("[%s/%s] %s: stored %s bars", i, len(self.symbols), sym, n)
            except YahooRateLimitError as exc:
                failures.append({"symbol": sym, "error": str(exc)})
                log.error("%s", exc)
                deferred = self.symbols[i:]
                failures.extend({
                    "symbol": pending,
                    "error": "Deferred: Yahoo Finance rate limit reached; retry in a later scan",
                } for pending in deferred)
                log.warning(
                    "Stopping Yahoo downloads after rate limit; deferred %s remaining symbols "
                    "to avoid sending more requests.",
                    len(deferred),
                )
                if progress_callback:
                    progress_callback({
                        "phase": "تحميل البيانات متوقف مؤقتاً بسبب حد Yahoo",
                        "completed": i,
                        "total": total,
                        "current_symbol": sym,
                    })
                break
            except Exception as exc:
                failures.append({"symbol": sym, "error": str(exc)})
                log.warning("%s: %s", sym, exc)
            if progress_callback:
                progress_callback({"phase": "تحميل البيانات", "completed": i, "total": total, "current_symbol": sym})
        return failures

    def compute_rs_ratings(self) -> tuple[dict, dict]:
        prices: dict[str, pd.Series] = {}
        frames: dict[str, pd.DataFrame] = {}

        for sym in self.symbols:
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
        self,
        update: bool = True,
        period: str = "2y",
        market_symbol: str = "^GSPC",
        skip_market_check: bool = False,
        progress_callback=None,
    ) -> tuple[pd.DataFrame, list[dict], dict]:
        failures = self.update_universe_data(period, progress_callback) if update else []

        market_status = (
            {"market_favorable": True, "regime": "SKIPPED", "reasons": ["market check disabled"]}
            if skip_market_check
            else self.check_market(market_symbol, period)
        )
        market_favorable = bool(market_status.get("market_favorable", False))

        rs, frames = self.compute_rs_ratings()

        rows = []
        total_frames = len(frames)
        if progress_callback:
            progress_callback({"phase": "تحليل الأسهم", "completed": 0, "total": total_frames, "current_symbol": ""})
        for index, (sym, df) in enumerate(frames.items(), 1):
            rating = rs.get(sym, float("nan"))
            tt = check_trend_template(df, rating)
            if not tt["passed"]:
                continue

            # Build the contraction/pivot from completed bars before today's bar.
            # The latest bar is tested separately as the possible breakout candle.
            base_df = df.iloc[:-1].copy()
            vcp = check_vcp(base_df)
            pivot_val = vcp.get("pivot_price")
            br = check_breakout(df, pivot=pivot_val) if vcp.get("passed") else False
            pb = check_pullback(df)
            ad = accumulation_distribution_summary(df)

            try:
                provider = self._get_provider(sym)
                fdf = provider.get_fundamentals_dataframe()
                latest = fdf.iloc[0].to_dict() if not fdf.empty else {}
                earnings = check_earnings(
                    latest.get("eps_current"),
                    latest.get("eps_year_ago"),
                    latest.get("revenue_current"),
                    latest.get("revenue_year_ago"),
                )
                fundamental_status = "UNKNOWN" if fdf.empty else ("PASS" if earnings["passed"] else "FAIL")
            except Exception as exc:
                earnings = {"passed": False, "error": str(exc)}
                fundamental_status = "UNKNOWN"

            passed_all = bool(
                tt["passed"]
                and vcp.get("passed", False)
                and br
                and pb.get("passed", False)
                and ad.get("accumulation", False)
                and earnings.get("passed", False)
                and market_favorable
            )

            rows.append({
                "symbol": sym,
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
                "passed_all_filters": passed_all,
                "screen_date": datetime.now(timezone.utc).date().isoformat(),
            })
            if progress_callback:
                progress_callback({"phase": "تحليل الأسهم", "completed": index, "total": total_frames, "current_symbol": sym})

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
            json.dumps(payload, indent=2, default=str), encoding="utf-8"
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
