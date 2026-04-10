"""Odds conversion and probability utilities."""

from __future__ import annotations


def parse_american_odds(raw: str | None) -> int | None:
    """Parse an American odds string to an integer.

    Handles unicode minus (−), en-dash (–), 'even' keyword, and bare numbers.
    Returns None if the value cannot be parsed.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    # Normalise various minus-like characters
    s = s.replace("\u2212", "-").replace("\u2013", "-")
    if s.lower() in ("even", "pk", "pick"):
        return 100
    # Strip non-numeric except leading sign
    import re
    match = re.fullmatch(r"([+-]?\d+)", s.replace(" ", ""))
    if match:
        return int(match.group(1))
    return None


def american_to_decimal(american: int) -> float:
    """Convert American odds to decimal (European) format."""
    if american >= 100:
        return round(american / 100 + 1, 4)
    else:
        return round(100 / abs(american) + 1, 4)


def american_to_implied_prob(american: int) -> float:
    """Compute raw implied probability from American odds (includes vig)."""
    if american >= 100:
        return round(100 / (american + 100), 6)
    else:
        return round(abs(american) / (abs(american) + 100), 6)


def remove_vig(over_prob: float, under_prob: float) -> tuple[float, float]:
    """Remove the vig from over/under implied probabilities.

    Normalises so probabilities sum to 1.0.
    """
    total = over_prob + under_prob
    if total <= 0:
        return over_prob, under_prob
    return round(over_prob / total, 6), round(under_prob / total, 6)


def format_american(odds: int) -> str:
    """Format an integer as an American odds string with explicit +/- sign."""
    if odds >= 0:
        return f"+{odds}"
    return str(odds)
