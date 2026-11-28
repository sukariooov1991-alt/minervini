"""Quarterly earnings/sales growth filter used in SEPA-style screening."""
import math
import pandas as pd


def check_earnings(
    eps_current: float,
    eps_year_ago: float,
    revenue_current: float,
    revenue_year_ago: float,
    min_eps_growth_pct: float = 20.0,
    min_revenue_growth_pct: float | None = None,
    require_acceleration: bool = False,
    prior_eps_growth_pct: float | None = None,
) -> dict:
    def to_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return float("nan")

    def growth(current, prior):
        if not (math.isfinite(current) and math.isfinite(prior)) or prior <= 0:
            return float("nan")
        return (current / prior - 1.0) * 100.0

    ec, ey = to_float(eps_current), to_float(eps_year_ago)
    rc, ry = to_float(revenue_current), to_float(revenue_year_ago)
    eg, rg = growth(ec, ey), growth(rc, ry)

    prior_growth = to_float(prior_eps_growth_pct)
    accelerating = (
        bool(math.isfinite(eg) and math.isfinite(prior_growth) and eg > prior_growth)
        if math.isfinite(eg) and math.isfinite(prior_growth)
        else None
    )
    accel_ok = bool(not require_acceleration or accelerating is True)

    # Minervini discusses strong sales growth but does not set one universal
    # percentage threshold for every industry. By default, require sales growth
    # to be positive and report its actual rate; allow an explicit override for
    # users who want a stricter research screen.
    revenue_growth_ok = bool(
        pd.notna(rg)
        and math.isfinite(rg)
        and (
            rg > 0
            if min_revenue_growth_pct is None
            else rg >= (float(min_revenue_growth_pct) - 1e-9)
        )
    )
    passed = bool(
        math.isfinite(ec)
        and ec > 0
        and pd.notna(eg)
        and math.isfinite(eg)
        and eg >= (min_eps_growth_pct - 1e-9)
        and revenue_growth_ok
        and accel_ok
    )
    return {
        "passed": passed,
        "eps_growth_pct": eg,
        "revenue_growth_pct": rg,
        "revenue_growth_ok": revenue_growth_ok,
        "accelerating": accelerating,
    }


def filter_earnings(frame: pd.DataFrame, **kwargs) -> pd.DataFrame:
    required = {"eps_current", "eps_year_ago", "revenue_current", "revenue_year_ago"}
    if frame is None or frame.empty or not required.issubset(frame.columns):
        return pd.DataFrame() if frame is None else frame.iloc[0:0].copy()

    def _row_check(r):
        row_kwargs = dict(kwargs)
        if "prior_eps_growth_pct" in r and "prior_eps_growth_pct" not in row_kwargs:
            row_kwargs["prior_eps_growth_pct"] = r["prior_eps_growth_pct"]
        return check_earnings(
            r["eps_current"],
            r["eps_year_ago"],
            r["revenue_current"],
            r["revenue_year_ago"],
            **row_kwargs,
        )["passed"]

    checks = frame.apply(_row_check, axis=1)
    return frame.loc[checks].copy()
