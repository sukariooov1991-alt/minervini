import unittest

import pandas as pd

from analysis.market_filter import check_market_regime
from analysis.accumulation_distribution import accumulation_distribution_summary
from analysis.earnings import check_earnings
from analysis.relative_strength import relative_strength_score
from analysis.trend_template import check_trend_template
from analysis.pullback import check_pullback
from data.trade_levels import build_trade_levels, compute_atr
from screener import DailyScreener, _entry_ready_from_plan


class EntryDecisionTests(unittest.TestCase):
    def _index(self):
        close = [100.0 + i * 0.2 for i in range(220)]
        return pd.DataFrame({"close": close, "volume": [1_000_000 + i * 1000 for i in range(220)]})

    def test_wilder_atr_can_seed_from_exactly_fourteen_ohlc_bars(self):
        frame = pd.DataFrame({
            "high": [101.0] * 14,
            "low": [99.0] * 14,
            "close": [100.0] * 14,
        })
        self.assertAlmostEqual(compute_atr(frame, period=14), 2.0)

    def test_market_breadth_is_required_and_thresholded(self):
        index = self._index()
        missing = check_market_regime(index)
        self.assertFalse(missing["market_favorable"])
        self.assertFalse(missing["breadth_ok"])
        weak = check_market_regime(index, breadth_pct=24.9)
        self.assertFalse(weak["market_favorable"])
        threshold = check_market_regime(index, breadth_pct=25.0)
        self.assertTrue(threshold["breadth_ok"])

    def test_final_entry_readiness_requires_a_complete_plan_and_buy_range(self):
        base = dict(
            trend_template_passed=True,
            accumulation_passed=True,
            earnings_passed=True,
            entry_confirmation_passed=True,
            market_favorable=True,
        )
        self.assertTrue(_entry_ready_from_plan(
            **base, plan={"status": "جاهز", "within_entry_range": True}
        ))
        self.assertFalse(_entry_ready_from_plan(
            **base, plan={"status": "غير مكتمل", "within_entry_range": True}
        ))
        self.assertFalse(_entry_ready_from_plan(
            **base, plan={"status": "جاهز", "within_entry_range": False}
        ))
        self.assertFalse(_entry_ready_from_plan(
            **{**base, "market_favorable": False},
            plan={"status": "جاهز", "within_entry_range": True},
        ))

    def test_accumulation_accepts_more_up_volume_without_an_arbitrary_25_percent_premium(self):
        closes = [100.0]
        moves = [1] * 11 + [-1] * 10 + [0] * 29
        for move in moves:
            closes.append(closes[-1] * (1.01 if move > 0 else 0.99 if move < 0 else 1.0))
        volumes = [100_000.0] * len(closes)
        for index, move in enumerate(moves, start=1):
            if move > 0:
                volumes[index] = 110_000.0
        frame = pd.DataFrame({"close": closes, "volume": volumes})

        result = accumulation_distribution_summary(frame)

        self.assertAlmostEqual(result["up_down_volume_ratio"], 1.21, places=6)
        self.assertEqual(result["power_up_days"], 11)
        self.assertEqual(result["power_down_days"], 0)
        self.assertTrue(result["accumulation"])

    def test_incomplete_fundamentals_are_detected_as_unknown_inputs(self):
        from screener import _missing_fundamental_fields

        self.assertEqual(
            _missing_fundamental_fields({
                "eps_current": 2.0,
                "eps_year_ago": 1.0,
                "revenue_current": 100.0,
            }),
            ["revenue_year_ago"],
        )
        self.assertEqual(
            _missing_fundamental_fields({
                "eps_current": 2.0,
                "eps_year_ago": 1.0,
                "revenue_current": 100.0,
                "revenue_year_ago": 80.0,
            }),
            [],
        )

    def test_market_regime_is_unknown_when_recent_index_volume_is_missing(self):
        index = self._index()
        index.loc[index.index[-1], "volume"] = float("nan")

        result = check_market_regime(index, breadth_pct=100.0)

        self.assertFalse(result["market_favorable"])
        self.assertEqual(result["regime"], "UNKNOWN")
        self.assertIsNone(result["distribution_days"])
        self.assertTrue(any("volume data is incomplete" in reason for reason in result["reasons"]))

    def test_market_breadth_defaults_to_unavailable_when_index_data_is_missing(self):
        result = check_market_regime(pd.DataFrame())
        self.assertFalse(result["market_favorable"])
        self.assertFalse(result["breadth_ok"])

    def test_realtime_trend_template_keeps_moving_averages_on_completed_closes(self):
        dates = pd.bdate_range("2025-01-01", periods=252)
        completed = pd.DataFrame({
            "date": dates,
            "open": [100.0] * 252,
            "high": [101.0] * 252,
            "low": [99.0] * 252,
            "close": [100.0] * 252,
        })
        live = pd.concat([
            completed,
            pd.DataFrame([{
                "date": dates[-1] + pd.offsets.BDay(1),
                "open": 100.0,
                "high": 10000.0,
                "low": 99.0,
                "close": 10000.0,
            }]),
        ], ignore_index=True)

        result = check_trend_template(live, 99, current_price=100.0)
        self.assertFalse(result["conditions"]["sma150_above_sma200"])
        self.assertFalse(result["conditions"]["sma50_above_150_and_200"])

    def test_trend_template_rejects_infinite_relative_strength(self):
        close = [100.0 + i * 0.3 for i in range(252)]
        frame = pd.DataFrame({
            "close": close,
            "high": [value + 1.0 for value in close],
            "low": [value - 1.0 for value in close],
        })
        result = check_trend_template(frame, float("inf"))
        self.assertFalse(result["conditions"]["relative_strength_rating_at_least_70"])
        self.assertFalse(result["passed"])

    def test_market_breadth_excludes_symbols_with_failed_downloads(self):
        class FakeStorage:
            def get_candles_df(self, symbol):
                if symbol == "A":
                    values = [100.0] * 50
                else:
                    values = [100.0] * 49 + [110.0]
                return pd.DataFrame({"close": values})

        scanner = DailyScreener.__new__(DailyScreener)
        scanner.symbols = ["A", "B"]
        scanner.storage = FakeStorage()
        self.assertEqual(scanner.compute_market_breadth(), 50.0)
        self.assertEqual(scanner.compute_market_breadth(exclude_symbols={"A"}), 100.0)

    def test_relative_strength_refuses_to_rank_when_universe_coverage_is_low(self):
        class FakeStorage:
            def get_candles_df(self, symbol):
                if symbol != "A":
                    return pd.DataFrame()
                close = [100.0 + i * 0.2 for i in range(253)]
                return pd.DataFrame({
                    "date": pd.date_range("2025-01-01", periods=253, freq="B"),
                    "close": close,
                })

        scanner = DailyScreener.__new__(DailyScreener)
        scanner.symbols = ["A", "B", "C"]
        scanner.storage = FakeStorage()

        ratings, frames = scanner.compute_rs_ratings()

        self.assertEqual(ratings, {})
        self.assertEqual(set(frames), {"A"})

    def test_market_breadth_is_unknown_when_universe_coverage_is_low(self):
        class FakeStorage:
            def get_candles_df(self, symbol):
                if symbol != "A":
                    return pd.DataFrame()
                return pd.DataFrame({"close": [100.0 + i for i in range(50)]})

        scanner = DailyScreener.__new__(DailyScreener)
        scanner.symbols = ["A", "B", "C", "D", "E"]
        scanner.storage = FakeStorage()

        self.assertIsNone(scanner.compute_market_breadth())
        self.assertEqual(scanner._last_breadth_participating, 1)

    def test_relative_strength_excludes_symbols_with_failed_daily_refresh(self):
        class FakeStorage:
            def get_candles_df(self, symbol):
                close = [100.0 + i * (0.1 if symbol == "A" else 0.2) for i in range(253)]
                return pd.DataFrame({
                    "date": pd.date_range("2025-01-01", periods=253, freq="B"),
                    "close": close,
                })

        scanner = DailyScreener.__new__(DailyScreener)
        scanner.symbols = ["A", "B"]
        scanner.storage = FakeStorage()

        ratings, frames = scanner.compute_rs_ratings(exclude_symbols={"A"})

        self.assertEqual(set(ratings), {"B"})
        self.assertEqual(set(frames), {"B"})

    def _pullback_frame(self, current_open, current_close):
        close = [100.0] * 74 + [99.5, 99.0, 98.7, 98.2, 98.0]
        low = [value - 0.5 for value in close]
        high = [value + 0.5 for value in close]
        close.append(current_close)
        low.append(min(current_open, current_close) - 0.4)
        high.append(max(current_open, current_close) + 0.4)
        open_prices = [value for value in close[:-1]] + [current_open]
        return pd.DataFrame({
            "open": open_prices,
            "high": high,
            "low": low,
            "close": close,
            "volume": [100000.0] * len(close),
        })

    def test_pullback_is_independent_and_passes_after_turn_up(self):
        frame = self._pullback_frame(98.1, 98.8)
        result = check_pullback(frame)
        self.assertTrue(result["candidate"])
        self.assertTrue(result["pullback_detected"])
        self.assertTrue(result["turning_up"])
        self.assertTrue(result["passed"])
        self.assertEqual(result["entry_trigger_price"], 98.8)
        self.assertLess(result["support_low"], result["entry_trigger_price"])

    def test_pullback_waits_while_current_candle_is_falling(self):
        frame = self._pullback_frame(98.5, 98.1)
        result = check_pullback(frame)
        self.assertTrue(result["candidate"])
        self.assertFalse(result["turning_up"])
        self.assertFalse(result["passed"])
        self.assertIsNone(result["entry_trigger_price"])

    def test_pullback_rejects_non_finite_or_non_positive_recent_prices(self):
        frame = self._pullback_frame(98.1, 98.8)
        frame.loc[frame.index[-1], "close"] = float("inf")
        self.assertFalse(check_pullback(frame)["passed"])
        frame.loc[frame.index[-1], "close"] = -1.0
        self.assertFalse(check_pullback(frame)["passed"])

    def test_trade_plan_uses_atr_fallback_when_structural_low_is_missing(self):
        frame = pd.DataFrame({
            "high": [101.0 + i * 0.1 for i in range(60)],
            "low": [99.0 + i * 0.1 for i in range(60)],
            "close": [100.0 + i * 0.1 for i in range(60)],
        })
        atr = compute_atr(frame, period=14)
        plan = build_trade_levels("TEST", 100.0, 100.0, frame, final_low=None)
        self.assertIsNotNone(atr)
        self.assertEqual(plan.stop_source, "atr")
        self.assertAlmostEqual(plan.shared_stop_loss, 100.0 - 2.0 * atr, places=4)
        self.assertEqual(plan.status, "جاهز")

    def test_earnings_acceleration_is_reported_without_becoming_a_default_hard_gate(self):
        accelerating = check_earnings(
            eps_current=12.0,
            eps_year_ago=10.0,
            revenue_current=105.0,
            revenue_year_ago=100.0,
            prior_eps_growth_pct=15.0,
        )
        decelerating = check_earnings(
            eps_current=12.0,
            eps_year_ago=10.0,
            revenue_current=105.0,
            revenue_year_ago=100.0,
            prior_eps_growth_pct=25.0,
        )
        unknown = check_earnings(
            eps_current=12.0,
            eps_year_ago=10.0,
            revenue_current=105.0,
            revenue_year_ago=100.0,
        )
        required_deceleration = check_earnings(
            eps_current=12.0,
            eps_year_ago=10.0,
            revenue_current=105.0,
            revenue_year_ago=100.0,
            prior_eps_growth_pct=25.0,
            require_acceleration=True,
        )
        self.assertTrue(accelerating["passed"])
        self.assertTrue(accelerating["accelerating"])
        self.assertTrue(decelerating["passed"])
        self.assertFalse(decelerating["accelerating"])
        self.assertTrue(unknown["passed"])
        self.assertIsNone(unknown["accelerating"])
        self.assertFalse(required_deceleration["passed"])

    def test_default_earnings_gate_does_not_impose_a_universal_ten_percent_sales_threshold(self):
        modest_sales_growth = check_earnings(
            eps_current=12.0,
            eps_year_ago=10.0,
            revenue_current=105.0,
            revenue_year_ago=100.0,
        )
        declining_sales = check_earnings(
            eps_current=12.0,
            eps_year_ago=10.0,
            revenue_current=95.0,
            revenue_year_ago=100.0,
        )
        self.assertTrue(modest_sales_growth["passed"])
        self.assertAlmostEqual(modest_sales_growth["revenue_growth_pct"], 5.0)
        self.assertFalse(declining_sales["passed"])

    def test_trade_plan_without_atr_does_not_fabricate_targets_or_stop(self):
        frame = pd.DataFrame({
            "high": [101.0 + i * 0.1 for i in range(10)],
            "low": [99.0 + i * 0.1 for i in range(10)],
            "close": [100.0 + i * 0.1 for i in range(10)],
            "volume": [100000.0] * 10,
        })
        plan = build_trade_levels("TEST", 105.0, 105.0, frame, final_low=None)
        self.assertNotEqual(plan.status, "جاهز")
        self.assertEqual(plan.targets, [])
        self.assertIsNone(plan.shared_stop_loss)
        self.assertTrue(any("ATR" in note for note in plan.notes))

    def test_trade_plan_without_anchor_is_never_complete(self):
        frame = pd.DataFrame({
            "high": [101.0 + i * 0.1 for i in range(60)],
            "low": [99.0 + i * 0.1 for i in range(60)],
            "close": [100.0 + i * 0.1 for i in range(60)],
            "volume": [100000.0] * 60,
        })
        plan = build_trade_levels("TEST", 105.0, None, frame, final_low=98.0)
        self.assertNotEqual(plan.status, "جاهز")
        self.assertEqual(plan.entry_zones, [])
        self.assertEqual(plan.targets, [])


    def test_trade_plan_uses_two_to_three_risk_reward_targets_and_caps_stop_at_10_percent(self):
        frame = pd.DataFrame({
            "high": [101.0] * 20,
            "low": [99.0] * 20,
            "close": [100.0] * 20,
        })
        valid = build_trade_levels(
            "TEST", current_price=100.0, pivot_price=100.0,
            candles_df=frame, final_low=95.0,
        )
        self.assertEqual(valid.status, "جاهز")
        first_risk = valid.entry_zones[0] - valid.shared_stop_loss
        first_reward = valid.targets[0] - valid.entry_zones[0]
        last_risk = valid.entry_zones[-1] - valid.shared_stop_loss
        last_reward = valid.targets[-1] - valid.entry_zones[-1]
        self.assertAlmostEqual(first_reward / first_risk, 2.0)
        self.assertAlmostEqual(last_reward / last_risk, 3.0)

        too_wide = build_trade_levels(
            "WIDE", current_price=100.0, pivot_price=100.0,
            candles_df=frame, final_low=80.0,
        )
        self.assertNotEqual(too_wide.status, "جاهز")
        self.assertTrue(any("الحد الأقصى 10%" in note for note in too_wide.notes))

    def test_trade_plan_rejects_wrong_number_of_custom_entry_zones_without_crashing(self):
        plan = build_trade_levels(
            symbol="AAPL",
            current_price=100.0,
            pivot_price=100.0,
            candles_df=self._index(),
            final_low=95.0,
            entry_calculator=lambda *args: [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
        )
        self.assertEqual(len(plan.entry_zones), 6)
        self.assertEqual(plan.targets, [])
        self.assertNotEqual(plan.status, "جاهز")

    def test_trade_plan_rejects_non_monotonic_target_ladder(self):
        frame = pd.DataFrame({
            "high": [101.0] * 20,
            "low": [99.0] * 20,
            "close": [100.0] * 20,
        })
        plan = build_trade_levels(
            "BADTARGET",
            current_price=100.0,
            pivot_price=100.0,
            candles_df=frame,
            final_low=95.0,
            target_calculator=lambda *args: [150.0, 140.0, 160.0, 170.0, 180.0],
        )
        self.assertNotEqual(plan.status, "جاهز")
        self.assertTrue(any("هدف غير صالح" in note for note in plan.notes))

    def test_earnings_growth_exact_minimum_thresholds_pass(self):
        result = check_earnings(
            eps_current=12.0,
            eps_year_ago=10.0,
            revenue_current=110.0,
            revenue_year_ago=100.0,
            min_eps_growth_pct=20.0,
            min_revenue_growth_pct=10.0,
        )
        self.assertTrue(result["passed"])
        self.assertAlmostEqual(result["eps_growth_pct"], 20.0)
        self.assertAlmostEqual(result["revenue_growth_pct"], 10.0)

    def test_252_session_relative_strength_requires_253_closes(self):
        self.assertTrue(pd.isna(relative_strength_score(pd.Series(range(1, 253)))))
        self.assertTrue(pd.notna(relative_strength_score(pd.Series(range(1, 254)))))

    def test_rs_universe_does_not_admit_252_close_history(self):
        class FakeStorage:
            def get_candles_df(self, symbol):
                return pd.DataFrame({
                    "date": pd.date_range("2025-01-01", periods=252, freq="B"),
                    "close": range(1, 253),
                })

        scanner = DailyScreener.__new__(DailyScreener)
        scanner.symbols = ["SHORT"]
        scanner.storage = FakeStorage()

        ratings, frames = scanner.compute_rs_ratings()

        self.assertEqual(ratings, {})
        self.assertNotIn("SHORT", frames)

    def test_atr_uses_wilder_smoothing(self):
        frame = pd.DataFrame({
            "high": [11.0, 12.0, 13.0, 14.0],
            "low": [9.0, 8.0, 7.0, 6.0],
            "close": [10.0, 10.0, 10.0, 10.0],
        })
        # True ranges are 2, 4, 6, 8; Wilder seed is 4, then
        # ((4 * 2) + 8) / 3 = 16/3.
        self.assertAlmostEqual(compute_atr(frame, period=3), 16.0 / 3.0)

if __name__ == "__main__":
    unittest.main()
