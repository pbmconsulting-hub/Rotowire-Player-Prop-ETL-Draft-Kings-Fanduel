"""SQLAlchemy ORM models for the NBA Props ETL pipeline."""

from datetime import datetime, UTC
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Text,
    UniqueConstraint, Index, create_engine,
)
from sqlalchemy.orm import declarative_base, Session

Base = declarative_base()


class PlayerProp(Base):
    """Point-in-time snapshot of a player prop betting line."""

    __tablename__ = "player_props"

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_name = Column(String(128), nullable=False)
    player_id = Column(Integer, nullable=True)
    team = Column(String(10), nullable=True)
    position = Column(String(10), nullable=True)
    opponent = Column(String(10), nullable=True)
    prop_type = Column(String(64), nullable=False)
    line = Column(Float, nullable=False)
    over_odds = Column(String(16), nullable=True)
    under_odds = Column(String(16), nullable=True)
    over_implied_prob = Column(Float, nullable=True)
    under_implied_prob = Column(Float, nullable=True)
    sportsbook = Column(String(64), nullable=True)
    game_date = Column(String(16), nullable=False)
    game_time = Column(String(16), nullable=True)
    scraped_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))

    __table_args__ = (
        UniqueConstraint(
            "player_name", "prop_type", "line", "over_odds", "under_odds",
            "sportsbook", "game_date", "scraped_at",
            name="uq_player_prop_snapshot",
        ),
        Index("ix_player_prop_date", "player_name", "prop_type", "game_date"),
        Index("ix_game_date", "game_date"),
        Index("ix_scraped_at", "scraped_at"),
        Index("ix_prop_type", "prop_type"),
    )


class ScrapeRun(Base):
    """Audit log for every ETL execution."""

    __tablename__ = "scrape_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    started_at = Column(DateTime, nullable=False)
    finished_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, nullable=True)
    status = Column(String(16))  # running, success, failed
    extraction_method = Column(String(16))  # api, selenium, hybrid
    rows_extracted = Column(Integer)
    rows_validated = Column(Integer)
    rows_rejected = Column(Integer)
    rows_loaded = Column(Integer)
    error_message = Column(Text, nullable=True)
    error_traceback = Column(Text, nullable=True)


class LineMovement(Base):
    """Computed summary table for line movement tracking."""

    __tablename__ = "line_movements"

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_name = Column(String(128), nullable=False)
    prop_type = Column(String(64), nullable=False)
    game_date = Column(String(16), nullable=False)
    sportsbook = Column(String(64), nullable=True)
    opening_line = Column(Float, nullable=False)
    current_line = Column(Float, nullable=False)
    line_diff = Column(Float, nullable=False)
    opening_over_odds = Column(String(16), nullable=True)
    current_over_odds = Column(String(16), nullable=True)
    first_seen_at = Column(DateTime, nullable=True)
    last_seen_at = Column(DateTime, nullable=True)
    num_changes = Column(Integer, nullable=True)


def init_db(engine=None) -> None:
    """Create all database tables if they do not exist."""
    from src.config import Config
    if engine is None:
        engine = create_engine(Config.DATABASE_URL)
    Base.metadata.create_all(engine)
