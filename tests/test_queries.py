"""Tests for src/queries.py."""

import pytest
from datetime import datetime
from sqlalchemy.orm import Session

from src.models import PlayerProp
from src.queries import get_current_props, get_dk_vs_fd, get_edges, get_line_movement

GAME_DATE = "2024-01-15"


def _insert_props(session, engine):
    """Insert sample props for DK, FD, and a third book."""
    props = [
        # DraftKings - LeBron Points
        PlayerProp(
            player_name="LeBron James", prop_type="Points",
            line=25.5, over_odds="-115", under_odds="-105",
            sportsbook="DraftKings", game_date=GAME_DATE,
            scraped_at=datetime(2024, 1, 15, 12, 0, 0),
        ),
        # FanDuel - LeBron Points (different line)
        PlayerProp(
            player_name="LeBron James", prop_type="Points",
            line=26.5, over_odds="-110", under_odds="-110",
            sportsbook="FanDuel", game_date=GAME_DATE,
            scraped_at=datetime(2024, 1, 15, 12, 0, 0),
        ),
        # Third book - should be filtered out
        PlayerProp(
            player_name="LeBron James", prop_type="Points",
            line=24.5, over_odds="-112", under_odds="-108",
            sportsbook="Caesars", game_date=GAME_DATE,
            scraped_at=datetime(2024, 1, 15, 12, 0, 0),
        ),
        # DraftKings - Curry Points
        PlayerProp(
            player_name="Stephen Curry", prop_type="Points",
            line=29.5, over_odds="-120", under_odds="+100",
            sportsbook="DraftKings", game_date=GAME_DATE,
            scraped_at=datetime(2024, 1, 15, 12, 0, 0),
        ),
        # FanDuel - Curry Points
        PlayerProp(
            player_name="Stephen Curry", prop_type="Points",
            line=29.5, over_odds="-115", under_odds="-105",
            sportsbook="FanDuel", game_date=GAME_DATE,
            scraped_at=datetime(2024, 1, 15, 12, 0, 0),
        ),
    ]
    session.add_all(props)
    session.commit()


class TestGetCurrentProps:
    def test_only_dk_and_fd_returned(self, engine, session):
        _insert_props(session, engine)
        df = get_current_props(game_date=GAME_DATE, engine=engine)
        books = set(df["sportsbook"].unique())
        assert "Caesars" not in books
        assert "DraftKings" in books
        assert "FanDuel" in books

    def test_filter_by_player(self, engine, session):
        _insert_props(session, engine)
        df = get_current_props(game_date=GAME_DATE, player="LeBron", engine=engine)
        assert all("LeBron" in name for name in df["player_name"])

    def test_filter_by_prop_type(self, engine, session):
        _insert_props(session, engine)
        df = get_current_props(game_date=GAME_DATE, prop_type="Points", engine=engine)
        assert all(r == "Points" for r in df["prop_type"])


class TestGetDkVsFd:
    def test_side_by_side_columns(self, engine, session):
        _insert_props(session, engine)
        df = get_dk_vs_fd(GAME_DATE, engine=engine)
        assert "dk_line" in df.columns
        assert "fd_line" in df.columns
        assert "line_diff" in df.columns

    def test_line_diff_computed(self, engine, session):
        _insert_props(session, engine)
        df = get_dk_vs_fd(GAME_DATE, engine=engine)
        lebron_row = df[df["player_name"] == "LeBron James"]
        assert len(lebron_row) == 1
        assert abs(lebron_row.iloc[0]["line_diff"] - (-1.0)) < 0.01


class TestGetEdges:
    def test_filters_by_min_line_diff(self, engine, session):
        _insert_props(session, engine)
        df = get_edges(GAME_DATE, min_line_diff=0.5, engine=engine)
        if not df.empty:
            assert all(abs(df["line_diff"]) >= 0.5)

    def test_no_edges_above_large_threshold(self, engine, session):
        _insert_props(session, engine)
        df = get_edges(GAME_DATE, min_line_diff=10.0, engine=engine)
        assert df.empty


class TestGetLineMovement:
    def test_returns_history(self, engine, session):
        _insert_props(session, engine)
        df = get_line_movement("LeBron James", "Points", GAME_DATE, engine=engine)
        assert len(df) >= 1
        assert set(df["sportsbook"].unique()).issubset({"DraftKings", "FanDuel"})
