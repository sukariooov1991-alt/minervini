"""General Market Health Filter based on Minervini's market timing principles."""
import pandas as pd


from analysis.frame_utils import ordered_frame

def count_distribution_days(
    index_df: pd.DataFrame,
    lookback: int = 25,
    drop_threshold_pct: float = -0.2,
) -> int:
    if index_df is None or not {"close", "volume"}.issubset(index_df.columns):
        return 0
    if len(index_df) < lookback + 1:
        return 0

    recent = ordered_frame(index_df, reset_index=True).tail(lookback + 1).copy()
    close = pd.to_numeric(recent["close"], errors="coerce")
    volume = pd.to_numeric(recent["volume"], errors="coerce")

    pct_change = close.pct_change() * 100.0
    vol_higher = volume > volume.shift(1)

    dist_days = (pct_change <= drop_threshold_pct) & vol_higher
    return int(dist_days.iloc[1:].sum())


def check_market_regime(
    index_df: pd.DataFrame,
    breadth_pct: float = None,
    min_breadth_pct: float = 25.0,
    max_distribution_days: int = 5,
    drop_threshold_pct: float = -0.2,
) -> dict:
    out = {
        "market_favorable": False,
        "regime": "UNKNOWN",
        "above_sma200": False,
        "above_sma50": False,
        "sma50_above_sma200": False,
        "distribution_days": 0,
        "breadth_ok": False,
        "reasons": [],
    }
    if index_df is None or not {"close", "volume"}.issubset(index_df.columns) or len(index_df) < 200:
        out["reasons"].append("requires at least 200 bars of index close and volume")
        return out

    clean_df = ordered_frame(index_df, reset_index=True)
    # Distribution-day counting is only trustworthy when every close and
    # volume observation in its 25-session window is present and finite.
    # Missing volume must not silently become zero distribution days.
    recent = clean_df.tail(26)
    recent_close = pd.to_numeric(recent["close"], errors="coerce").to_numpy(dtype=float)
    recent_volume = pd.to_numeric(recent["volume"], errors="coerce").to_numpy(dtype=float)
    if (
        len(recent) < 26
        or not pd.notna(recent_close).all()
        or not pd.notna(recent_volume).all()
        or not __import__("numpy").isfinite(recent_close).all()
        or not __import__("numpy").isfinite(recent_volume).all()
        or (recent_close <= 0).any()
        or (recent_volume < 0).any()
    ):
        out["distribution_days"] = None
        out["reasons"].append(
            "index close/volume data is incomplete or invalid in the 25-session distribution-day window"
        )
        return out

    close = pd.to_numeric(clean_df["close"], errors="coerce").dropna().reset_index(drop=True)
    if len(close) < 200:
        out["reasons"].append("insufficient valid close values")
        return out

    sma50 = float(close.rolling(50).mean().iloc[-1])
    sma200 = float(close.rolling(200).mean().iloc[-1])
    price = float(close.iloc[-1])

    above_200 = price > sma200
    above_50 = price > sma50
    aligned = sma50 > sma200
    dist_days = count_distribution_days(clean_df, drop_threshold_pct=drop_threshold_pct)

    # Unknown breadth must not be silently treated as a healthy market.
    breadth_ok = bool(
        breadth_pct is not None
        and pd.notna(breadth_pct)
        and float(breadth_pct) >= min_breadth_pct
    )

    if not above_200:
        out["reasons"].append("index is below 200-day SMA")
    if not above_50:
        out["reasons"].append("index is below 50-day SMA")
    if not aligned:
        out["reasons"].append("50-day SMA is below 200-day SMA")
    if dist_days > max_distribution_days:
        out["reasons"].append(f"high distribution days ({dist_days} > {max_distribution_days})")
    if not breadth_ok:
        if breadth_pct is None or pd.isna(breadth_pct):
            out["reasons"].append("market breadth unavailable; market entry filter cannot pass")
        else:
            out["reasons"].append(f"weak market breadth ({float(breadth_pct):.1f}% < {min_breadth_pct}%)")

    favorable = bool(
        above_200
        and above_50
        and aligned
        and dist_days <= max_distribution_days
        and breadth_ok
    )

    if favorable and aligned:
        regime = "CONFIRMED_UPTREND"
    elif above_200 and dist_days <= max_distribution_days + 1:
        regime = "UPTREND_UNDER_PRESSURE"
    else:
        regime = "CORRECTION_OR_DOWNTREND"

    out.update({
        "market_favorable": favorable,
        "regime": regime,
        "above_sma200": above_200,
        "above_sma50": above_50,
        "sma50_above_sma200": aligned,
        "distribution_days": dist_days,
        "breadth_ok": breadth_ok,
    })
    return out
