"""Load layer: inserts cleaned records into the database."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.config import Config
from src.models import PlayerProp, ScrapeRun, LineMovement


def _get_session(engine=None) -> Session:
    """Create a new SQLAlchemy Session."""
    if engine is None:
        engine = create_engine(Config.DATABASE_URL)
    return Session(engine)


def load(df: pd.DataFrame, scrape_run_id: int, engine=None) -> int:
    """Bulk-insert cleaned prop records and update the scrape run audit record.

    Args:
        df: Cleaned DataFrame from the transform layer.
        scrape_run_id: ID of the current ScrapeRun row.
        engine: Optional SQLAlchemy engine (uses Config.DATABASE_URL if None).

    Returns:
        Number of rows successfully inserted.
    """
    if df.empty:
        logger.info("Load: empty DataFrame, nothing to insert")
        _update_scrape_run(scrape_run_id, rows_loaded=0, engine=engine)
        return 0

    scraped_at = datetime.utcnow()
    objects = []
    for _, row in df.iterrows():
        obj = PlayerProp(
            player_name=row["player_name"],
            player_id=row.get("player_id"),
            team=row.get("team"),
            position=row.get("position"),
            opponent=row.get("opponent"),
            prop_type=row["prop_type"],
            line=row["line"],
            over_odds=row.get("over_odds"),
            under_odds=row.get("under_odds"),
            over_implied_prob=row.get("over_implied_prob"),
            under_implied_prob=row.get("under_implied_prob"),
            sportsbook=row.get("sportsbook"),
            game_date=row["game_date"],
            game_time=row.get("game_time"),
            scraped_at=scraped_at,
        )
        objects.append(obj)

    session = _get_session(engine)
    try:
        session.bulk_save_objects(objects)
        session.commit()
        rows_loaded = len(objects)
        logger.info("Load: inserted {} rows", rows_loaded)
        _update_scrape_run(scrape_run_id, rows_loaded=rows_loaded, engine=engine)
        return rows_loaded
    except Exception as exc:
        session.rollback()
        logger.error("Load failed: {}", exc)
        raise
    finally:
        session.close()


def _update_scrape_run(
    scrape_run_id: int,
    rows_loaded: int,
    engine=None,
) -> None:
    """Update the rows_loaded field on a ScrapeRun record."""
    session = _get_session(engine)
    try:
        run = session.get(ScrapeRun, scrape_run_id)
        if run:
            run.rows_loaded = rows_loaded
            session.commit()
    finally:
        session.close()


def compute_line_movements(game_date: str, engine=None) -> None:
    """Compute and store line movement summaries for a given game date.

    Uses window functions where available; falls back gracefully on SQLite.

    Args:
        game_date: ISO date string (YYYY-MM-DD).
        engine: Optional SQLAlchemy engine.
    """
    if engine is None:
        engine = create_engine(Config.DATABASE_URL)

    session = Session(engine)
    try:
        # Clear existing movements for this date
        session.query(LineMovement).filter(
            LineMovement.game_date == game_date
        ).delete()
        session.commit()

        # Attempt window-function query (PostgreSQL / SQLite 3.25+)
        try:
            sql = text("""
                SELECT
                    player_name,
                    prop_type,
                    game_date,
                    sportsbook,
                    FIRST_VALUE(line) OVER w  AS opening_line,
                    LAST_VALUE(line)  OVER w  AS current_line,
                    LAST_VALUE(line)  OVER w  - FIRST_VALUE(line) OVER w AS line_diff,
                    FIRST_VALUE(over_odds) OVER w AS opening_over_odds,
                    LAST_VALUE(over_odds)  OVER w AS current_over_odds,
                    MIN(scraped_at) OVER w   AS first_seen_at,
                    MAX(scraped_at) OVER w   AS last_seen_at,
                    COUNT(*) OVER w          AS num_changes
                FROM player_props
                WHERE game_date = :game_date
                  AND sportsbook IN ('DraftKings', 'FanDuel')
                WINDOW w AS (
                    PARTITION BY player_name, prop_type, sportsbook, game_date
                    ORDER BY scraped_at
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )
            """)
            rows = session.execute(sql, {"game_date": game_date}).fetchall()
        except Exception as exc:
            logger.warning("Window function query failed ({}); using fallback", exc)
            rows = _compute_line_movements_fallback(session, game_date)

        movements = []
        seen = set()
        for row in rows:
            key = (row[0], row[1], row[3], row[2])  # name, prop, book, date
            if key in seen:
                continue
            seen.add(key)
            movements.append(
                LineMovement(
                    player_name=row[0],
                    prop_type=row[1],
                    game_date=row[2],
                    sportsbook=row[3],
                    opening_line=float(row[4]),
                    current_line=float(row[5]),
                    line_diff=float(row[6]),
                    opening_over_odds=row[7],
                    current_over_odds=row[8],
                    first_seen_at=row[9],
                    last_seen_at=row[10],
                    num_changes=int(row[11]) if row[11] is not None else None,
                )
            )

        if movements:
            session.bulk_save_objects(movements)
            session.commit()
        logger.info(
            "compute_line_movements: {} records for {}", len(movements), game_date
        )
    except Exception as exc:
        session.rollback()
        logger.error("compute_line_movements failed: {}", exc)
    finally:
        session.close()


def _compute_line_movements_fallback(session, game_date: str):
    """Fallback line movement computation without window functions."""
    from src.models import PlayerProp as PP

    groups: dict[tuple, list] = {}
    rows = (
        session.query(PP)
        .filter(
            PP.game_date == game_date,
            PP.sportsbook.in_(["DraftKings", "FanDuel"]),
        )
        .order_by(PP.scraped_at)
        .all()
    )
    for r in rows:
        key = (r.player_name, r.prop_type, r.game_date, r.sportsbook)
        groups.setdefault(key, []).append(r)

    result = []
    for (pname, ptype, gdate, book), items in groups.items():
        first = items[0]
        last = items[-1]
        result.append((
            pname, ptype, gdate, book,
            first.line, last.line,
            last.line - first.line,
            first.over_odds, last.over_odds,
            first.scraped_at, last.scraped_at,
            len(items),
        ))
    return result
