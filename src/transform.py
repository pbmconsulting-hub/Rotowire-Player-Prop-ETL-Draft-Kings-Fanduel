"""Transform layer: cleans, validates, and filters raw prop records."""

from __future__ import annotations

from typing import Optional
import pandas as pd
from loguru import logger
from pydantic import ValidationError

from src.config import Config
from src.utils.validation import RawPropRecord, CleanPropRecord
from src.utils.odds import parse_american_odds, american_to_implied_prob, remove_vig


# ---------------------------------------------------------------------------
# Prop-type normalisation map
# ---------------------------------------------------------------------------

PROP_TYPE_MAP: dict[str, str] = {
    "pts": "Points",
    "points": "Points",
    "point": "Points",
    "reb": "Rebounds",
    "rebounds": "Rebounds",
    "rebound": "Rebounds",
    "ast": "Assists",
    "assists": "Assists",
    "assist": "Assists",
    "3pm": "Three Pointers Made",
    "threes": "Three Pointers Made",
    "three pointers made": "Three Pointers Made",
    "3-pt made": "Three Pointers Made",
    "3 pointers made": "Three Pointers Made",
    "pra": "Pts+Reb+Ast",
    "pts+reb+ast": "Pts+Reb+Ast",
    "points+rebounds+assists": "Pts+Reb+Ast",
    "stl": "Steals",
    "steals": "Steals",
    "blk": "Blocks",
    "blocks": "Blocks",
    "to": "Turnovers",
    "turnovers": "Turnovers",
    "turnover": "Turnovers",
    "min": "Minutes",
    "minutes": "Minutes",
    "pr": "Pts+Reb",
    "pts+reb": "Pts+Reb",
    "pa": "Pts+Ast",
    "pts+ast": "Pts+Ast",
    "ra": "Reb+Ast",
    "reb+ast": "Reb+Ast",
}


def _normalise_prop_type(raw: str) -> str:
    """Map a raw prop type string to a canonical name."""
    key = raw.strip().lower()
    return PROP_TYPE_MAP.get(key, raw.strip().title())


def _parse_line(raw) -> Optional[float]:
    """Parse a line value to float, handling strings with non-numeric chars."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    import re
    s = str(raw).strip()
    match = re.search(r"[-+]?\d*\.?\d+", s)
    if match:
        return float(match.group())
    return None


def _resolve_sportsbook(raw: Optional[str]) -> Optional[str]:
    """Resolve a raw sportsbook string to its canonical name via aliases."""
    if raw is None:
        return None
    return Config.SPORTSBOOK_ALIASES.get(str(raw).strip().lower(), str(raw).strip())


# ---------------------------------------------------------------------------
# Core transform function
# ---------------------------------------------------------------------------

def transform(
    raw_records: list[dict],
) -> tuple[pd.DataFrame, int, int]:
    """Clean, validate, and filter raw prop records.

    Args:
        raw_records: List of raw dicts from the extraction layer.

    Returns:
        (clean_df, num_validated, num_rejected)
    """
    logger.info("Transform: received {} raw records", len(raw_records))
    validated_rows: list[dict] = []
    rejected = 0

    for record in raw_records:
        # --- Step 1: loose parse via RawPropRecord ---
        try:
            raw = RawPropRecord.model_validate(record)
        except Exception:
            rejected += 1
            continue

        # --- Step 2: resolve sportsbook and filter ---
        book = _resolve_sportsbook(raw.sportsbook)
        if book not in Config.ALLOWED_SPORTSBOOKS:
            logger.debug("Rejected sportsbook: {}", raw.sportsbook)
            rejected += 1
            continue

        # --- Step 3: required field presence ---
        if not raw.player_name:
            rejected += 1
            continue
        if raw.line is None:
            rejected += 1
            continue
        if not raw.prop_type:
            rejected += 1
            continue

        # --- Step 4: parse / normalise fields ---
        line_val = _parse_line(raw.line)
        if line_val is None:
            rejected += 1
            continue

        prop_type_clean = _normalise_prop_type(str(raw.prop_type))

        # Compute implied probabilities
        over_prob: Optional[float] = None
        under_prob: Optional[float] = None
        over_int = parse_american_odds(str(raw.over_odds) if raw.over_odds is not None else None)
        under_int = parse_american_odds(str(raw.under_odds) if raw.under_odds is not None else None)
        if over_int is not None:
            over_prob = american_to_implied_prob(over_int)
        if under_int is not None:
            under_prob = american_to_implied_prob(under_int)
        if over_prob is not None and under_prob is not None:
            over_prob, under_prob = remove_vig(over_prob, under_prob)

        # Coerce player_id
        pid = None
        if raw.player_id is not None:
            try:
                pid = int(raw.player_id)
            except (ValueError, TypeError):
                pid = None

        cleaned = {
            "player_name": str(raw.player_name).strip(),
            "player_id": pid,
            "team": str(raw.team).strip().upper() if raw.team else None,
            "position": str(raw.position).strip().upper() if raw.position else None,
            "opponent": str(raw.opponent).strip().upper() if raw.opponent else None,
            "prop_type": prop_type_clean,
            "line": line_val,
            "over_odds": str(raw.over_odds)[:16] if raw.over_odds is not None else None,
            "under_odds": str(raw.under_odds)[:16] if raw.under_odds is not None else None,
            "over_implied_prob": over_prob,
            "under_implied_prob": under_prob,
            "sportsbook": book,
            "game_date": str(raw.game_date) if raw.game_date else None,
            "game_time": str(raw.game_time) if raw.game_time else None,
        }

        # --- Step 5: strict validation via CleanPropRecord ---
        try:
            CleanPropRecord.model_validate(cleaned)
        except ValidationError as exc:
            logger.debug("Validation failed: {}", exc)
            rejected += 1
            continue

        validated_rows.append(cleaned)

    logger.info(
        "Transform: validated={} rejected={}", len(validated_rows), rejected
    )

    if not validated_rows:
        return pd.DataFrame(), 0, rejected

    df = pd.DataFrame(validated_rows)

    # --- Step 6: require game_date ---
    before = len(df)
    df = df[df["game_date"].notna() & (df["game_date"] != "None")]
    rejected += before - len(df)

    # --- Step 7: deduplication ---
    dedup_cols = [
        "player_name", "prop_type", "line",
        "over_odds", "under_odds", "sportsbook", "game_date",
    ]
    before_dedup = len(df)
    df = df.drop_duplicates(subset=dedup_cols)
    dupes = before_dedup - len(df)
    if dupes:
        logger.info("Deduplicated {} duplicate rows", dupes)

    logger.info("Transform complete: {} clean rows", len(df))
    return df, len(df), rejected
