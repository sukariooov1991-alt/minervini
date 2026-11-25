"""Minervini Trend Template: eight yes/no conditions."""
import pandas as pd


def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    out = df.copy()
    if isinstance(out.index, pd.DatetimeIndex) and not out.index.is_monotonic_increasing:
        out = out.sort_index()
    return out


def check_trend_template(df: pd.DataFrame, rs_rating: float) -> dict:
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

    df_clean = _prepare_df(df)
    for col in required:
        df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")
    df_clean = df_clean.dropna(subset=["close", "high", "low"]).reset_index(drop=True)
    if len(df_clean) < 252:
        result["failed"] = ["requires at least 252 valid daily OHLC bars"]
        return result

    close = df_clean["close"]
    sma50 = close.rolling(50).mean()
    sma150 = close.rolling(150).mean()
    sma200 = close.rolling(200).mean()

    price = float(close.iloc[-1])
    ma50 = float(sma50.iloc[-1])
    ma150 = float(sma150.iloc[-1])
    ma200 = float(sma200.iloc[-1])
    ma200_1m_ago = float(sma200.iloc[-22])

    low52 = float(df_clean["low"].tail(252).min())
    high52 = float(df_clean["high"].tail(252).max())

    if low52 <= 0 or high52 <= 0 or price <= 0:
        result["failed"] = ["invalid non-positive prices"]
        return result

    conditions = {
        "price_above_150_and_200": price > ma150 and price > ma200,
        "sma150_above_sma200": ma150 > ma200,
        "sma200_rising_for_at_least_one_month": ma200 > ma200_1m_ago,
        "sma50_above_150_and_200": ma50 > ma150 and ma50 > ma200,
        "price_above_sma50": price > ma50,
        "price_at_least_30_percent_above_52w_low": price >= low52 * 1.30,
        "price_within_25_percent_of_52w_high": price >= high52 * 0.75,
        "relative_strength_rating_at_least_70": (
            pd.notna(rs_rating) and pd.api.types.is_number(rs_rating)
            and float(rs_rating) >= 70.0 and pd.notna(float(rs_rating))
            and abs(float(rs_rating)) != float("inf")
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
