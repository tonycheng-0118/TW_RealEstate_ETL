"""
season_utils.py — Season computation, validation, and parameter resolution.

ROC season format: '{roc_year}S{quarter}', e.g. '115S2' = AD 2026 Q2.
ROC year = AD year - 1911, quarter = ceil(month / 3).

Public API:
    get_current_season() → str
    validate_season(s) → bool
    parse_season(s) → (int, int)
    compare_seasons(a, b) → int
    season_range(start, end) → list[str]
    resolve_params(start, end) → (start, end, is_current_only)
"""

import math
import re
from datetime import date

# Regex for valid season format: {1-3 digit year}S{1-4}
SEASON_PATTERN = re.compile(r"^(\d{1,3})S([1-4])$")


def get_current_season() -> str:
    """Compute the current ROC season from today's date.

    ROC year = AD year - 1911, quarter = ceil(month / 3).
    Example: 2026-04-06 → 115S2
    """
    today = date.today()
    roc_year = today.year - 1911
    quarter = math.ceil(today.month / 3)
    return f"{roc_year}S{quarter}"


def validate_season(s: str) -> bool:
    """Check if a string is a valid season format like '115S2'."""
    return bool(SEASON_PATTERN.match(s))


def parse_season(s: str) -> tuple[int, int]:
    """Parse a season string into (roc_year, quarter).

    Raises ValueError if format is invalid.
    """
    m = SEASON_PATTERN.match(s)
    if not m:
        raise ValueError(f"Invalid season format: '{s}'. Expected format: 113S1, 114S2, etc.")
    return int(m.group(1)), int(m.group(2))


def compare_seasons(a: str, b: str) -> int:
    """Compare two season strings.

    Returns -1 if a < b, 0 if a == b, 1 if a > b.
    """
    ya, qa = parse_season(a)
    yb, qb = parse_season(b)
    if (ya, qa) < (yb, qb):
        return -1
    elif (ya, qa) > (yb, qb):
        return 1
    return 0


def season_range(start: str, end: str) -> list[str]:
    """Generate a list of seasons from start to end (inclusive).

    Example: season_range('113S3', '114S2') → ['113S3', '113S4', '114S1', '114S2']
    """
    sy, sq = parse_season(start)
    ey, eq = parse_season(end)
    result = []
    y, q = sy, sq
    while (y, q) <= (ey, eq):
        result.append(f"{y}S{q}")
        q += 1
        if q > 4:
            q = 1
            y += 1
    return result


def resolve_params(
    start_raw: str | None, end_raw: str | None
) -> tuple[str, str, bool]:
    """Resolve start/end season parameters into validated values.

    Args:
        start_raw: User input for start_season (None or empty = current).
        end_raw: User input for end_season (None or empty = start through current).

    Returns:
        (start_season, end_season, is_current_only) tuple.
        is_current_only=True means only process the current period (download_current).

    Raises:
        ValueError on invalid input.
    """
    current = get_current_season()

    # Normalize empty/whitespace to None
    start = (start_raw or "").strip() or None
    end = (end_raw or "").strip() or None

    # Normalize 'current' string to None (treated same as empty)
    if start and start.lower() == "current":
        start = None
    if end and end.lower() == "current":
        end = None

    # --- Case 1: Both empty → current single season ---
    if start is None and end is None:
        return current, current, True

    # --- Case 2: start empty + end has value → ERROR ---
    # start=empty means current, current can't be start of range
    if start is None and end is not None:
        raise ValueError(
            f"start_season is empty (= current), but end_season is '{end_raw}'. "
            "current cannot be the start of a range with a specific season end."
        )

    # --- Case 3: start has value, validate format ---
    assert start is not None
    if not validate_season(start):
        raise ValueError(
            f"Invalid start_season format: '{start_raw}'. "
            "Expected format: 113S1, 114S2, etc."
        )

    # Validate start doesn't exceed current season
    if compare_seasons(start, current) > 0:
        raise ValueError(
            f"start_season '{start}' exceeds current season '{current}'."
        )

    # --- Case 4: start has value + end empty → start through current ---
    if end is None:
        return start, current, False

    # --- Case 5: Both have values, validate end ---
    if not validate_season(end):
        raise ValueError(
            f"Invalid end_season format: '{end_raw}'. "
            "Expected format: 113S1, 114S2, etc."
        )

    if compare_seasons(end, current) > 0:
        raise ValueError(
            f"end_season '{end}' exceeds current season '{current}'."
        )

    if compare_seasons(end, start) < 0:
        raise ValueError(
            f"end_season '{end}' is before start_season '{start}'. "
            "end_season must be >= start_season."
        )

    # start == end → single season (is_current_only only if it equals current)
    is_current = (start == current and end == current)
    return start, end, is_current
