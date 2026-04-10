"""Tests for src/load.py."""

import pytest
import pandas as pd
from datetime import datetime
from sqlalchemy.orm import Session

from src.models import PlayerProp, ScrapeRun
from src.load import load


def _make_run(session) -> int:
    run = ScrapeRun(started_at=datetime.utcnow(), status="running")
    session.add(run)
    session.commit()
    return run.id


def _clean_df() -> pd.DataFrame:
    return pd.DataFrame([{
        "player_name": "LeBron James",
        "player_id": 1001,
        "team": "LAL",
        "position": "SF",
        "opponent": "GSW",
        "prop_type": "Points",
        "line": 25.5,
        "over_odds": "-115",
        "under_odds": "-105",
        "over_implied_prob": 0.535,
        "under_implied_prob": 0.465,
        "sportsbook": "DraftKings",
        "game_date": "2024-01-15",
        "game_time": "7:30 PM ET",
    }])


class TestLoad:
    def test_bulk_insert(self, engine, session):
        run_id = _make_run(session)
        df = _clean_df()
        rows = load(df, run_id, engine=engine)
        assert rows == 1

    def test_empty_df_returns_zero(self, engine, session):
        run_id = _make_run(session)
        rows = load(pd.DataFrame(), run_id, engine=engine)
        assert rows == 0

    def test_scrape_run_updated(self, engine, session):
        run_id = _make_run(session)
        df = _clean_df()
        load(df, run_id, engine=engine)
        session.expire_all()
        run = session.get(ScrapeRun, run_id)
        assert run.rows_loaded == 1

    def test_row_persisted(self, engine, session):
        run_id = _make_run(session)
        df = _clean_df()
        load(df, run_id, engine=engine)
        session.expire_all()
        props = session.query(PlayerProp).all()
        assert len(props) == 1
        assert props[0].player_name == "LeBron James"
        assert props[0].line == 25.5
