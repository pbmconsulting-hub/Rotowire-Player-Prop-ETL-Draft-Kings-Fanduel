"""FastAPI web dashboard for viewing and testing the NBA Props ETL pipeline."""

from __future__ import annotations

import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from loguru import logger
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import Session

from src.config import Config
from src.models import Base, LineMovement, PlayerProp, ScrapeRun, init_db

app = FastAPI(title="NBA Props Dashboard")

_TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

# ---------------------------------------------------------------------------
# Engine / session helpers
# ---------------------------------------------------------------------------

_engine = None


def get_engine():
    """Return the shared SQLAlchemy engine, creating it on first use."""
    global _engine
    if _engine is None:
        _engine = create_engine(Config.DATABASE_URL)
        init_db(_engine)
    return _engine


def get_session() -> Session:
    """Create a new database session."""
    return Session(get_engine())


# ---------------------------------------------------------------------------
# Helper: distinct game dates
# ---------------------------------------------------------------------------


def _game_dates(session: Session) -> list[str]:
    """Return distinct game_date values sorted descending."""
    rows = (
        session.query(PlayerProp.game_date)
        .filter(PlayerProp.sportsbook.in_(["DraftKings", "FanDuel"]))
        .distinct()
        .order_by(PlayerProp.game_date.desc())
        .all()
    )
    return [r[0] for r in rows if r[0]]


def _prop_types(session: Session) -> list[str]:
    """Return distinct prop_type values sorted alphabetically."""
    rows = (
        session.query(PlayerProp.prop_type)
        .distinct()
        .order_by(PlayerProp.prop_type)
        .all()
    )
    return [r[0] for r in rows if r[0]]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, message: Optional[str] = None, message_type: str = "info"):
    """Home dashboard with overview stats."""
    session = get_session()
    try:
        total_props = session.query(func.count(PlayerProp.id)).scalar() or 0
        today = datetime.utcnow().strftime("%Y-%m-%d")
        today_props = (
            session.query(func.count(PlayerProp.id))
            .filter(PlayerProp.game_date == today)
            .scalar()
            or 0
        )
        total_runs = session.query(func.count(ScrapeRun.id)).scalar() or 0

        last_run = (
            session.query(ScrapeRun)
            .order_by(ScrapeRun.started_at.desc())
            .first()
        )
        last_run_status = last_run.status if last_run else None

        dk_count = (
            session.query(func.count(PlayerProp.id))
            .filter(PlayerProp.sportsbook == "DraftKings")
            .scalar()
            or 0
        )
        fd_count = (
            session.query(func.count(PlayerProp.id))
            .filter(PlayerProp.sportsbook == "FanDuel")
            .scalar()
            or 0
        )
        movement_count = session.query(func.count(LineMovement.id)).scalar() or 0
        unique_players = (
            session.query(func.count(func.distinct(PlayerProp.player_name))).scalar()
            or 0
        )

        recent_runs = (
            session.query(ScrapeRun)
            .order_by(ScrapeRun.started_at.desc())
            .limit(10)
            .all()
        )

        game_dates = _game_dates(session)

        return templates.TemplateResponse(
            request,
            "dashboard.html",
            {
                "active_page": "home",
                "total_props": total_props,
                "today_props": today_props,
                "total_runs": total_runs,
                "last_run_status": last_run_status,
                "dk_count": dk_count,
                "fd_count": fd_count,
                "movement_count": movement_count,
                "unique_players": unique_players,
                "recent_runs": recent_runs,
                "game_dates": game_dates,
                "message": message,
                "message_type": message_type,
            },
        )
    finally:
        session.close()


@app.post("/run-pipeline")
async def trigger_pipeline():
    """Trigger a pipeline run in a background thread."""
    from src.pipeline import run_pipeline

    def _run():
        try:
            run_pipeline(engine=get_engine())
        except Exception as exc:
            logger.error("Background pipeline run failed: {}", exc)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    return RedirectResponse(
        url="/?message=Pipeline+run+started+in+background&message_type=success",
        status_code=303,
    )


@app.get("/props", response_class=HTMLResponse)
async def current_props(
    request: Request,
    game_date: Optional[str] = None,
    prop_type: Optional[str] = None,
    sportsbook: Optional[str] = None,
    player: Optional[str] = None,
):
    """Current props with filters."""
    session = get_session()
    try:
        game_dates = _game_dates(session)
        prop_types = _prop_types(session)

        query = session.query(PlayerProp).filter(
            PlayerProp.sportsbook.in_(["DraftKings", "FanDuel"])
        )
        if game_date:
            query = query.filter(PlayerProp.game_date == game_date)
        if prop_type:
            query = query.filter(PlayerProp.prop_type == prop_type)
        if sportsbook:
            query = query.filter(PlayerProp.sportsbook == sportsbook)
        if player:
            query = query.filter(PlayerProp.player_name.ilike(f"%{player}%"))

        # Get latest snapshot per (player, prop, book, date)
        subq = (
            session.query(func.max(PlayerProp.id).label("max_id"))
            .filter(PlayerProp.sportsbook.in_(["DraftKings", "FanDuel"]))
        )
        if game_date:
            subq = subq.filter(PlayerProp.game_date == game_date)
        if prop_type:
            subq = subq.filter(PlayerProp.prop_type == prop_type)
        if sportsbook:
            subq = subq.filter(PlayerProp.sportsbook == sportsbook)
        if player:
            subq = subq.filter(PlayerProp.player_name.ilike(f"%{player}%"))

        subq = subq.group_by(
            PlayerProp.player_name,
            PlayerProp.prop_type,
            PlayerProp.sportsbook,
            PlayerProp.game_date,
        ).subquery()

        props = (
            session.query(PlayerProp)
            .join(subq, PlayerProp.id == subq.c.max_id)
            .order_by(
                PlayerProp.game_date.desc(),
                PlayerProp.player_name,
                PlayerProp.prop_type,
            )
            .limit(500)
            .all()
        )

        return templates.TemplateResponse(
            request,
            "props.html",
            {
                "active_page": "props",
                "props": props,
                "game_dates": game_dates,
                "prop_types": prop_types,
                "selected_date": game_date,
                "selected_prop_type": prop_type,
                "selected_sportsbook": sportsbook,
                "selected_player": player,
            },
        )
    finally:
        session.close()


@app.get("/compare", response_class=HTMLResponse)
async def compare_dk_fd(
    request: Request,
    game_date: Optional[str] = None,
):
    """DK vs FD side-by-side comparison."""
    session = get_session()
    try:
        game_dates = _game_dates(session)
        selected_date = game_date or (game_dates[0] if game_dates else "")

        rows = []
        if selected_date:
            from src.queries import get_dk_vs_fd

            df = get_dk_vs_fd(selected_date, engine=get_engine())
            if not df.empty:
                rows = df.to_dict("records")

        # Convert dicts to namespace-like objects for template dot access
        class Row:
            def __init__(self, d):
                for k, v in d.items():
                    setattr(self, k, v)

        rows = [Row(r) for r in rows]

        return templates.TemplateResponse(
            request,
            "compare.html",
            {
                "active_page": "compare",
                "rows": rows,
                "game_dates": game_dates,
                "selected_date": selected_date,
            },
        )
    finally:
        session.close()


@app.get("/edges", response_class=HTMLResponse)
async def edges_view(
    request: Request,
    game_date: Optional[str] = None,
    min_diff: float = 0.5,
):
    """Edges / line discrepancies."""
    session = get_session()
    try:
        game_dates = _game_dates(session)
        selected_date = game_date or (game_dates[0] if game_dates else "")

        rows = []
        if selected_date:
            from src.queries import get_edges

            df = get_edges(selected_date, min_line_diff=min_diff, engine=get_engine())
            if not df.empty:
                rows = df.to_dict("records")

        class Row:
            def __init__(self, d):
                for k, v in d.items():
                    setattr(self, k, v)

        rows = [Row(r) for r in rows]

        return templates.TemplateResponse(
            request,
            "edges.html",
            {
                "active_page": "edges",
                "rows": rows,
                "game_dates": game_dates,
                "selected_date": selected_date,
                "min_diff": min_diff,
            },
        )
    finally:
        session.close()


@app.get("/movements", response_class=HTMLResponse)
async def movements_view(
    request: Request,
    game_date: Optional[str] = None,
    player: Optional[str] = None,
):
    """Line movement tracking."""
    session = get_session()
    try:
        game_dates = _game_dates(session)
        selected_date = game_date or (game_dates[0] if game_dates else "")

        query = session.query(LineMovement)
        if selected_date:
            query = query.filter(LineMovement.game_date == selected_date)
        if player:
            query = query.filter(LineMovement.player_name.ilike(f"%{player}%"))

        movements = (
            query.order_by(func.abs(LineMovement.line_diff).desc())
            .limit(500)
            .all()
        )

        return templates.TemplateResponse(
            request,
            "movements.html",
            {
                "active_page": "movements",
                "movements": movements,
                "game_dates": game_dates,
                "selected_date": selected_date,
                "selected_player": player,
            },
        )
    finally:
        session.close()


@app.get("/runs", response_class=HTMLResponse)
async def scrape_runs(request: Request):
    """Scrape runs audit log."""
    session = get_session()
    try:
        runs = (
            session.query(ScrapeRun)
            .order_by(ScrapeRun.started_at.desc())
            .limit(100)
            .all()
        )

        success_count = sum(1 for r in runs if r.status == "success")
        success_rate = (success_count / len(runs) * 100) if runs else 0
        durations = [r.duration_seconds for r in runs if r.duration_seconds]
        avg_duration = sum(durations) / len(durations) if durations else 0
        total_loaded = sum(r.rows_loaded or 0 for r in runs)
        total_rejected = sum(r.rows_rejected or 0 for r in runs)

        return templates.TemplateResponse(
            request,
            "runs.html",
            {
                "active_page": "runs",
                "runs": runs,
                "success_rate": success_rate,
                "avg_duration": avg_duration,
                "total_loaded": total_loaded,
                "total_rejected": total_rejected,
            },
        )
    finally:
        session.close()


# ---------------------------------------------------------------------------
# JSON API endpoints
# ---------------------------------------------------------------------------


@app.get("/api/health")
async def api_health():
    """Quick health check endpoint."""
    session = get_session()
    try:
        total = session.query(func.count(PlayerProp.id)).scalar() or 0
        last_run = (
            session.query(ScrapeRun)
            .order_by(ScrapeRun.started_at.desc())
            .first()
        )
        return {
            "status": "ok",
            "total_props": total,
            "last_run_status": last_run.status if last_run else None,
            "last_run_at": str(last_run.started_at) if last_run else None,
            "database_url_type": "postgresql" if "postgresql" in Config.DATABASE_URL else "sqlite",
        }
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Start the dashboard server."""
    import uvicorn

    port = int(os.getenv("DASHBOARD_PORT", "8000"))
    host = os.getenv("DASHBOARD_HOST", "127.0.0.1")

    logger.info("Starting NBA Props Dashboard on http://{}:{}", host, port)
    uvicorn.run(
        "src.dashboard:app",
        host=host,
        port=port,
        reload=os.getenv("DASHBOARD_RELOAD", "false").lower() == "true",
    )


if __name__ == "__main__":
    main()
