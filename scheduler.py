"""
scheduler.py — Runs the weekly pipeline automatically.

Run this instead of main.py to get both the API server and the scheduler:
    python scheduler.py

The scheduler fires every Saturday at 08:00 Eastern Time, which is typically
after LARA has published the new MasterList for the week.
"""

import logging
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pipeline import run_weekly_pipeline

logger = logging.getLogger("scheduler")


def start_scheduler():
    scheduler = BackgroundScheduler(timezone="America/Detroit")
    scheduler.add_job(
        run_weekly_pipeline,
        CronTrigger(day_of_week="sat", hour=8, minute=0),
        id="weekly_delta",
        name="Michigan LARA weekly delta",
        replace_existing=True,
        misfire_grace_time=3600,  # Allow up to 1 hour late if server was down
    )
    scheduler.start()
    logger.info("Scheduler started — pipeline runs every Saturday at 08:00 ET.")
    return scheduler


if __name__ == "__main__":
    import database as db
    db.init_db()

    scheduler = start_scheduler()

    # Run immediately on startup if no data exists yet
    from database import get_latest_week
    if get_latest_week() is None:
        logger.info("No data found — running initial pipeline now.")
        run_weekly_pipeline()

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info",
    )
