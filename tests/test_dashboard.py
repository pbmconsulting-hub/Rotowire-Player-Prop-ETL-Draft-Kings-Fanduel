"""Tests for the FastAPI dashboard application."""

from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from src.models import Base, PlayerProp, ScrapeRun, LineMovement
from src.dashboard import app, get_engine


@pytest.fixture(autouse=True)
def _override_engine(monkeypatch):
    """Use an in-memory SQLite database for all dashboard tests."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    import src.dashboard as dashboard_mod
    monkeypatch.setattr(dashboard_mod, "_engine", engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture()
def client():
    return TestClient(app)


@pytest.fixture()
def seeded_engine(_override_engine):
    """Seed the database with sample data."""
    engine = _override_engine
    session = Session(engine)
    run = ScrapeRun(
        started_at=datetime(2024, 1, 15, 10, 0, 0),
        finished_at=datetime(2024, 1, 15, 10, 0, 5),
        duration_seconds=5.0,
        status="success",
        extraction_method="api",
        rows_extracted=4,
        rows_validated=4,
        rows_rejected=0,
        rows_loaded=4,
    )
    session.add(run)
    session.commit()

    props = [
        PlayerProp(
            player_name="LeBron James", team="LAL", opponent="GSW",
            prop_type="Points", line=25.5, over_odds="-110", under_odds="-110",
            over_implied_prob=0.5238, under_implied_prob=0.4762,
            sportsbook="DraftKings", game_date="2024-01-15",
            scraped_at=datetime(2024, 1, 15, 10, 0, 0),
        ),
        PlayerProp(
            player_name="LeBron James", team="LAL", opponent="GSW",
            prop_type="Points", line=26.0, over_odds="-115", under_odds="-105",
            over_implied_prob=0.535, under_implied_prob=0.465,
            sportsbook="FanDuel", game_date="2024-01-15",
            scraped_at=datetime(2024, 1, 15, 10, 0, 0),
        ),
        PlayerProp(
            player_name="Stephen Curry", team="GSW", opponent="LAL",
            prop_type="Three Pointers Made", line=4.5, over_odds="+100", under_odds="-120",
            over_implied_prob=0.4545, under_implied_prob=0.5455,
            sportsbook="DraftKings", game_date="2024-01-15",
            scraped_at=datetime(2024, 1, 15, 10, 0, 0),
        ),
        PlayerProp(
            player_name="Stephen Curry", team="GSW", opponent="LAL",
            prop_type="Three Pointers Made", line=4.5, over_odds="-105", under_odds="-115",
            over_implied_prob=0.4762, under_implied_prob=0.5238,
            sportsbook="FanDuel", game_date="2024-01-15",
            scraped_at=datetime(2024, 1, 15, 10, 0, 0),
        ),
    ]
    session.add_all(props)
    session.commit()

    mvmt = LineMovement(
        player_name="LeBron James", prop_type="Points", game_date="2024-01-15",
        sportsbook="DraftKings", opening_line=25.0, current_line=25.5,
        line_diff=0.5, first_seen_at=datetime(2024, 1, 15, 9, 0),
        last_seen_at=datetime(2024, 1, 15, 10, 0), num_changes=2,
    )
    session.add(mvmt)
    session.commit()
    session.close()
    return engine


class TestDashboardHome:
    def test_home_loads_empty(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "NBA Props" in resp.text

    def test_home_loads_with_data(self, client, seeded_engine):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "4" in resp.text  # total props
        assert "success" in resp.text.lower()

    def test_home_shows_message_param(self, client):
        resp = client.get("/?message=Test+message&message_type=success")
        assert resp.status_code == 200
        assert "Test message" in resp.text


class TestCurrentProps:
    def test_props_page_loads(self, client):
        resp = client.get("/props")
        assert resp.status_code == 200
        assert "Current Props" in resp.text

    def test_props_with_data(self, client, seeded_engine):
        resp = client.get("/props?game_date=2024-01-15")
        assert resp.status_code == 200
        assert "LeBron James" in resp.text
        assert "Stephen Curry" in resp.text

    def test_props_filter_by_player(self, client, seeded_engine):
        resp = client.get("/props?player=LeBron")
        assert resp.status_code == 200
        assert "LeBron James" in resp.text

    def test_props_filter_by_sportsbook(self, client, seeded_engine):
        resp = client.get("/props?sportsbook=DraftKings&game_date=2024-01-15")
        assert resp.status_code == 200
        assert "DraftKings" in resp.text


class TestCompare:
    def test_compare_page_loads(self, client):
        resp = client.get("/compare")
        assert resp.status_code == 200
        assert "DraftKings vs FanDuel" in resp.text

    def test_compare_with_data(self, client, seeded_engine):
        resp = client.get("/compare?game_date=2024-01-15")
        assert resp.status_code == 200
        assert "LeBron James" in resp.text


class TestEdges:
    def test_edges_page_loads(self, client):
        resp = client.get("/edges")
        assert resp.status_code == 200
        assert "Edges" in resp.text

    def test_edges_with_data(self, client, seeded_engine):
        resp = client.get("/edges?game_date=2024-01-15&min_diff=0.1")
        assert resp.status_code == 200


class TestMovements:
    def test_movements_page_loads(self, client):
        resp = client.get("/movements")
        assert resp.status_code == 200
        assert "Line Movements" in resp.text

    def test_movements_with_data(self, client, seeded_engine):
        resp = client.get("/movements?game_date=2024-01-15")
        assert resp.status_code == 200
        assert "LeBron James" in resp.text

    def test_movements_filter_by_player(self, client, seeded_engine):
        resp = client.get("/movements?game_date=2024-01-15&player=LeBron")
        assert resp.status_code == 200
        assert "LeBron James" in resp.text


class TestRuns:
    def test_runs_page_loads(self, client):
        resp = client.get("/runs")
        assert resp.status_code == 200
        assert "Scrape Runs" in resp.text

    def test_runs_with_data(self, client, seeded_engine):
        resp = client.get("/runs")
        assert resp.status_code == 200
        assert "success" in resp.text.lower()


class TestAPIHealth:
    def test_health_endpoint(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "total_props" in data

    def test_health_with_data(self, client, seeded_engine):
        resp = client.get("/api/health")
        data = resp.json()
        assert data["total_props"] == 4
        assert data["last_run_status"] == "success"


class TestPipelineTrigger:
    def test_trigger_redirects(self, client, monkeypatch):
        """Test that the trigger endpoint redirects without actually running the pipeline."""
        import src.pipeline as pipeline_mod
        monkeypatch.setattr(pipeline_mod, "run_pipeline", lambda engine=None: None)
        resp = client.post("/run-pipeline", follow_redirects=False)
        assert resp.status_code == 303
        assert "message" in resp.headers["location"]
