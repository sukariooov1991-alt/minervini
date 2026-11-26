"""Shared chronological DataFrame ordering for daily market-analysis modules."""
from __future__ import annotations

import pandas as pd


def ordered_frame(
    frame: pd.DataFrame | None,
    *,
    reset_index: bool = False,
) -> pd.DataFrame:
    """Return a copy ordered oldest-to-newest without mutating provider data."""
    if frame is None:
        return pd.DataFrame()

    out = frame.copy()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.sort_values("date", kind="stable")
    elif isinstance(out.index, pd.DatetimeIndex) and not out.index.is_monotonic_increasing:
        out = out.sort_index(kind="stable")

    return out.reset_index(drop=True) if reset_index else out
