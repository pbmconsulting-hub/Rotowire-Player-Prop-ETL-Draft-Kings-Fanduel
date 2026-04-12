"""Pipeline orchestrator: wires together extract, transform, and load."""

from __future__ import annotations

import traceback
from datetime import datetime, UTC

from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.config import Config
from src.models import ScrapeRun, init_db
from src.extract import extract
from src.transform import transform
from src.load import load, compute_line_movements


def run_pipeline(engine=None) -> None:
    """Execute a single end-to-end ETL run.

    Creates a ScrapeRun audit record, runs extract/transform/load,
    updates line movements, and records success or failure.
    """
    if engine is None:
        engine = create_engine(Config.DATABASE_URL)

    session = Session(engine)
    run = ScrapeRun(
        started_at=datetime.now(UTC),
        status="running",
    )
    session.add(run)
    session.commit()
    run_id = run.id
    session.close()

    start_time = datetime.now(UTC)
    logger.info("Pipeline run #{} started", run_id)

    try:
        # --- Extract ---
        records, method = extract()
        logger.info("Extracted {} records via {}", len(records), method)
        _patch_run(engine, run_id, rows_extracted=len(records), extraction_method=method)

        # --- Transform ---
        clean_df, num_validated, num_rejected = transform(records)
        logger.info(
            "Transform: validated={} rejected={}", num_validated, num_rejected
        )
        _patch_run(
            engine, run_id,
            rows_validated=num_validated,
            rows_rejected=num_rejected,
        )

        # --- Load ---
        rows_loaded = load(clean_df, run_id, engine=engine)

        # --- Post-load: line movements ---
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        compute_line_movements(today, engine=engine)

        # --- Finalise success ---
        duration = (datetime.now(UTC) - start_time).total_seconds()
        _patch_run(
            engine, run_id,
            status="success",
            finished_at=datetime.now(UTC),
            duration_seconds=duration,
        )
        logger.info(
            "Pipeline run #{} succeeded in {:.1f}s: {} rows loaded",
            run_id, duration, rows_loaded,
        )

    except Exception as exc:
        duration = (datetime.now(UTC) - start_time).total_seconds()
        tb = traceback.format_exc()
        logger.error("Pipeline run #{} failed: {}\n{}", run_id, exc, tb)
        _patch_run(
            engine, run_id,
            status="failed",
            finished_at=datetime.now(UTC),
            duration_seconds=duration,
            error_message=str(exc),
            error_traceback=tb,
        )
        raise


def _patch_run(engine, run_id: int, **kwargs) -> None:
    """Update arbitrary fields on a ScrapeRun record."""
    session = Session(engine)
    try:
        run = session.get(ScrapeRun, run_id)
        if run:
            for k, v in kwargs.items():
                setattr(run, k, v)
            session.commit()
    finally:
        session.close()


def main() -> None:
    """Entry point: configure logging, initialise DB, run pipeline, start scheduler."""
    import sys
    from loguru import logger as _logger
    import os

    # Configure loguru
    _logger.remove()
    _logger.add(sys.stderr, level=Config.LOG_LEVEL)
    os.makedirs("logs", exist_ok=True)
    _logger.add(
        Config.LOG_FILE,
        level=Config.LOG_LEVEL,
        rotation=Config.LOG_ROTATION,
        enqueue=True,
    )

    engine = create_engine(Config.DATABASE_URL)
    init_db(engine)

    # Run once immediately
    try:
        run_pipeline(engine=engine)
    except Exception:
        pass  # already logged inside run_pipeline

    # Schedule recurring runs
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler()
    scheduler.add_job(
        lambda: run_pipeline(engine=engine),
        "interval",
        minutes=Config.SCRAPE_INTERVAL_MINUTES,
        max_instances=1,
        misfire_grace_time=120,
    )
    logger.info(
        "Scheduler started: interval={} min", Config.SCRAPE_INTERVAL_MINUTES
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")


if __name__ == "__main__":
    main()
