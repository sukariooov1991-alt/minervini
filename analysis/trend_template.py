"""Minervini Trend Template: eight yes/no conditions."""
import math

import pandas as pd


from analysis.frame_utils import ordered_frame

def check_trend_template(
    df: pd.DataFrame,
    rs_rating: float,
    current_price: float | None = None,
) -> dict:
    required = {"close", "high", "low"}
    result = {
        "passed": False,
        "conditions": {},
        "failed": [],
        "criteria": {
            "min_above_52w_low_pct": 30.0,
            "max_below_52w_high_pct": 25.0,
            "min_rs_rating": 70.0,
        },
    }
    if df is None or not required.issubset(df.columns):
        result["failed"] = ["requires close, high and low columns"]
        return result

    df_clean = ordered_frame(df)
    for col in required:
        df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")
    df_clean = df_clean.dropna(subset=["close", "high", "low"]).reset_index(drop=True)
    if len(df_clean) < 252:
        result["failed"] = ["requires at least 252 valid daily OHLC bars"]
        return result

    close = df_clean["close"]
    # Realtime callers pass the evolving session candle and its current price.
    # Moving averages must remain based on completed daily closes; the evolving
    # candle is retained only for the rolling 52-week high/low context.
    average_close = close.iloc[:-1] if current_price is not None else close
    if len(average_close) < 221:
        result["failed"] = ["requires at least 221 completed daily closes for realtime averages"]
        return result
    sma50 = average_close.rolling(50).mean()
    sma150 = average_close.rolling(150).mean()
    sma200 = average_close.rolling(200).mean()

    try:
        price = float(current_price) if current_price is not None else float(close.iloc[-1])
    except (TypeError, ValueError):
        price = float("nan")
    ma50 = float(sma50.iloc[-1])
    ma150 = float(sma150.iloc[-1])
    ma200 = float(sma200.iloc[-1])
    ma200_1m_ago = float(sma200.iloc[-22])

    low52 = float(df_clean["low"].tail(252).min())
    high52 = float(df_clean["high"].tail(252).max())

    if not all(math.isfinite(value) and value > 0 for value in (
        price, ma50, ma150, ma200, ma200_1m_ago, low52, high52,
    )):
        result["failed"] = ["invalid or non-finite OHLC/average values"]
        return result

    try:
        rs_value = float(rs_rating)
    except (TypeError, ValueError):
        rs_value = float("nan")

    conditions = {
        "price_above_150_and_200": price > ma150 and price > ma200,
        "sma150_above_sma200": ma150 > ma200,
        "sma200_rising_for_at_least_one_month": ma200 > ma200_1m_ago,
        "sma50_above_150_and_200": ma50 > ma150 and ma50 > ma200,
        "price_above_sma50": price > ma50,
        "price_at_least_30_percent_above_52w_low": price >= low52 * 1.30,
        "price_within_25_percent_of_52w_high": price >= high52 * 0.75,
        "relative_strength_rating_at_least_70": (
            math.isfinite(rs_value) and rs_value >= 70.0
        ),
    }
    result["conditions"] = conditions
    result["failed"] = [name for name, passed in conditions.items() if not passed]
    result["passed"] = all(conditions.values())
    return result


def filter_trend_template(prices: dict, rs_ratings: dict) -> list:
    return [
        symbol
        for symbol, frame in prices.items()
        if check_trend_template(frame, rs_ratings.get(symbol, float("nan")))["passed"]
    ]
