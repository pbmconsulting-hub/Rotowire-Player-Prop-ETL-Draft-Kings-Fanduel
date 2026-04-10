"""Shared pytest fixtures for the NBA Props ETL test suite."""

import json
import os
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.models import Base


@pytest.fixture(scope="function")
def engine():
    """In-memory SQLite engine, tables created fresh for each test."""
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    yield eng
    Base.metadata.drop_all(eng)
    eng.dispose()


@pytest.fixture(scope="function")
def session(engine):
    """SQLAlchemy Session that rolls back after each test."""
    with Session(engine) as sess:
        yield sess
        sess.rollback()


@pytest.fixture(scope="session")
def sample_api_response():
    """Load the sample API response JSON fixture."""
    fixtures_dir = os.path.join(os.path.dirname(__file__), "fixtures")
    with open(os.path.join(fixtures_dir, "sample_api_response.json")) as f:
        return json.load(f)


@pytest.fixture(scope="session")
def sample_html():
    """Load the sample HTML fixture."""
    fixtures_dir = os.path.join(os.path.dirname(__file__), "fixtures")
    with open(os.path.join(fixtures_dir, "sample_html.html")) as f:
        return f.read()
