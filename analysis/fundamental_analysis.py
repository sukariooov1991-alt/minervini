"""Fundamental quality/growth filter using caller-supplied financial statements."""
import math
import pandas as pd


def check_fundamentals(
    row,
    min_annual_eps_growth: float = 0.0,
    min_annual_sales_growth: float = 0.0,
) -> bool:
    try:
        eps = float(row["annual_eps"])
        eps_growth = float(row["annual_eps_growth_pct"])
        sales_growth = float(row["annual_sales_growth_pct"])
    except (KeyError, TypeError, ValueError):
        return False
    if not all(math.isfinite(v) for v in (eps, eps_growth, sales_growth)):
        return False
    return bool(
        eps > 0
        and eps_growth >= min_annual_eps_growth
        and sales_growth >= min_annual_sales_growth
    )


def filter_fundamentals(frame: pd.DataFrame, **kwargs) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame() if frame is None else frame.copy()
    mask = frame.apply(lambda row: check_fundamentals(row, **kwargs), axis=1)
    return frame.loc[mask].copy()
