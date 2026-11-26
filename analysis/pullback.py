"""Source-grounded Pullback classification.

Mark Minervini describes pullbacks qualitatively (inside a base, to a prior
breakout level, or occasionally to a moving average after a breakout), and
requires price to turn up rather than buying while it is falling. The cited
material does not define a mechanical trigger, tolerance, ATR ratio, or stop.
Therefore this module deliberately does not emit an automated entry signal.
"""


def check_pullback(df, **kwargs) -> dict:
    """Keep Pullback visible for manual review; never mark it entry-ready.

    No numeric rule is inferred from qualitative source material.
    """
    if df is None or getattr(df, "empty", True):
        return {
            "passed": False,
            "candidate": False,
            "entry_eligible": False,
            "reason": "Pullback requires manual review; no source-defined mechanical trigger is available.",
        }
    return {
        "passed": False,
        "candidate": False,
        "entry_eligible": False,
        "reason": (
            "Manual review only: Minervini describes pullbacks inside a base, "
            "to a prior breakout level, or occasionally to a moving average "
            "after a breakout, and waits for price to turn up. The source does "
            "not define a numeric trigger or stop for automatic execution."
        ),
    }


def filter_pullbacks(prices: dict, **kwargs) -> list:
    """No automatic Pullback entries are emitted pending a documented trigger."""
    return []
