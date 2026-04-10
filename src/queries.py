"""Pre-built analytics query helpers filtered to DraftKings + FanDuel."""

from __future__ import annotations

from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.config import Config


def _engine(engine=None):
    if engine is None:
        return create_engine(Config.DATABASE_URL)
    return engine


def get_current_props(
    game_date: Optional[str] = None,
    prop_type: Optional[str] = None,
    player: Optional[str] = None,
    engine=None,
) -> pd.DataFrame:
    """Return the latest snapshot per (player, prop, sportsbook, date).

    Optionally filtered by game_date, prop_type, or player name (LIKE search).
    Always restricted to DraftKings and FanDuel.
    """
    eng = _engine(engine)
    filters = ["sportsbook IN ('DraftKings', 'FanDuel')"]
    params: dict = {}

    if game_date:
        filters.append("game_date = :game_date")
        params["game_date"] = game_date
    if prop_type:
        filters.append("prop_type = :prop_type")
        params["prop_type"] = prop_type
    if player:
        filters.append("player_name LIKE :player")
        params["player"] = f"%{player}%"

    where = " AND ".join(filters)

    sql = text(f"""
        SELECT *
        FROM player_props
        WHERE id IN (
            SELECT MAX(id)
            FROM player_props
            WHERE {where}
            GROUP BY player_name, prop_type, sportsbook, game_date
        )
        ORDER BY game_date DESC, player_name, prop_type, sportsbook
    """)
    with eng.connect() as conn:
        return pd.read_sql(sql, conn, params=params)


def get_dk_vs_fd(game_date: str, engine=None) -> pd.DataFrame:
    """Side-by-side DraftKings vs FanDuel comparison for a game date.

    Columns include dk_line, dk_over, dk_under, fd_line, fd_over, fd_under,
    line_diff, and prob_diff, ordered by largest discrepancy.
    """
    current = get_current_props(game_date=game_date, engine=engine)
    if current.empty:
        return pd.DataFrame()

    dk = current[current["sportsbook"] == "DraftKings"][
        ["player_name", "prop_type", "line", "over_odds", "under_odds", "over_implied_prob"]
    ].rename(
        columns={
            "line": "dk_line",
            "over_odds": "dk_over",
            "under_odds": "dk_under",
            "over_implied_prob": "dk_over_prob",
        }
    )
    fd = current[current["sportsbook"] == "FanDuel"][
        ["player_name", "prop_type", "line", "over_odds", "under_odds", "over_implied_prob"]
    ].rename(
        columns={
            "line": "fd_line",
            "over_odds": "fd_over",
            "under_odds": "fd_under",
            "over_implied_prob": "fd_over_prob",
        }
    )

    merged = pd.merge(dk, fd, on=["player_name", "prop_type"], how="inner")
    merged["line_diff"] = (merged["dk_line"] - merged["fd_line"]).round(2)
    merged["prob_diff"] = (merged["dk_over_prob"] - merged["fd_over_prob"]).round(4)
    merged = merged.reindex(
        columns=[
            "player_name", "prop_type",
            "dk_line", "dk_over", "dk_under",
            "fd_line", "fd_over", "fd_under",
            "line_diff", "prob_diff",
        ]
    )
    merged["abs_line_diff"] = merged["line_diff"].abs()
    merged = merged.sort_values("abs_line_diff", ascending=False).drop(
        columns=["abs_line_diff"]
    )
    return merged.reset_index(drop=True)


def get_edges(
    game_date: str,
    min_line_diff: float = 0.5,
    engine=None,
) -> pd.DataFrame:
    """Return DK vs FD rows where |line_diff| >= min_line_diff."""
    df = get_dk_vs_fd(game_date=game_date, engine=engine)
    if df.empty:
        return df
    return df[df["line_diff"].abs() >= min_line_diff].reset_index(drop=True)


def get_line_movement(
    player_name: str,
    prop_type: str,
    game_date: str,
    engine=None,
) -> pd.DataFrame:
    """Full chronological history of line + odds for a specific player prop.

    Returns both DK and FD rows ordered by sportsbook then scraped_at.
    """
    eng = _engine(engine)
    sql = text("""
        SELECT player_name, prop_type, game_date, sportsbook,
               line, over_odds, under_odds,
               over_implied_prob, under_implied_prob, scraped_at
        FROM player_props
        WHERE player_name = :player_name
          AND prop_type    = :prop_type
          AND game_date    = :game_date
          AND sportsbook  IN ('DraftKings', 'FanDuel')
        ORDER BY sportsbook, scraped_at
    """)
    params = {
        "player_name": player_name,
        "prop_type": prop_type,
        "game_date": game_date,
    }
    with eng.connect() as conn:
        return pd.read_sql(sql, conn, params=params)
