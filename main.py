"""
main.py — LeadStart Delta API
Michigan Liquor License Week-Over-Week Change Detection

Endpoints:
  GET /v1/michigan/licenses/delta        — latest week's changes
  GET /v1/michigan/licenses/delta/weeks  — list all available weeks
  GET /v1/health                         — service health check
  POST /admin/run-pipeline               — manually trigger pipeline (secured)
"""

import logging
import os
from datetime import date
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

import database as db
from models import (
    DeltaResponse,
    WeeksResponse,
    HealthResponse,
    SignalType,
)
from pipeline import run_weekly_pipeline

# ── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("api")

# ── App setup ─────────────────────────────────────────────────────────────
app = FastAPI(
    title="LeadStart — Michigan Liquor License Delta API",
    description=(
        "Week-over-week change detection for Michigan liquor licenses. "
        "Automatically tracks new licenses, activations, escrow placements, "
        "reactivations, location changes, and removals from the Michigan LARA "
        "MasterList — updated every Saturday morning."
    ),
    version="1.0.0",
    contact={
        "name": "LeadStart Data",
        "url": "https://rapidapi.com/leadstart",
    },
    license_info={
        "name": "Source data: Michigan LARA (public domain)",
        "url": "https://www.michigan.gov/lara/bureau-list/lcc/licensing-list",
    },
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── Startup ───────────────────────────────────────────────────────────────
@app.on_event("startup")
def startup():
    db.init_db()
    logger.info("Database initialised.")


# ── Auth helper (admin routes only) ───────────────────────────────────────
ADMIN_KEY = os.environ.get("ADMIN_API_KEY", "change-me-in-env")

def require_admin(x_admin_key: str = Header(...)):
    if x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Invalid admin key.")


# ── Public endpoints ───────────────────────────────────────────────────────

@app.get(
    "/v1/michigan/licenses/delta",
    response_model=DeltaResponse,
    summary="Get license changes for a given week",
    tags=["Michigan Liquor Licenses"],
)
def get_delta(
    week: Optional[date] = Query(
        default=None,
        description="Week ending date (YYYY-MM-DD). Defaults to the most recent available week.",
        examples=["2026-03-28"],
    ),
    signal_type: Optional[SignalType] = Query(
        default=None,
        description=(
            "Filter by signal type. Options: "
            "new_license, activation, escrowed, reactivation, location_change, removed"
        ),
    ),
    county: Optional[str] = Query(
        default=None,
        description="Filter by county name (case-insensitive). Example: Wayne, Oakland, Macomb",
        examples=["Wayne"],
    ),
    limit: int = Query(default=500, ge=1, le=1000, description="Max results to return (1–1000)."),
    offset: int = Query(default=0, ge=0, description="Pagination offset."),
):
    """
    Returns week-over-week changes detected in the Michigan LARA liquor license MasterList.

    **Signal types explained:**
    - `new_license` — A license that did not exist in the previous week's file
    - `activation` — Status changed from Conditional → Active (business ready to operate)
    - `escrowed` — Status changed from Active → Escrowed (ownership transfer in progress)
    - `reactivation` — Status changed from Escrowed → Active (transfer complete or escrow lifted)
    - `location_change` — Same license, different address
    - `removed` — License no longer appears in the MasterList

    **Refreshed every Saturday morning** from the public LARA MasterList.
    """
    signal_str = signal_type.value if signal_type else None
    result = db.get_delta(
        week_ending=week,
        signal_type=signal_str,
        county=county,
        limit=limit,
        offset=offset,
    )

    if result is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No data found for week {week}. "
                "Use /v1/michigan/licenses/delta/weeks to see available dates."
                if week
                else "No data available yet. The pipeline may not have run."
            ),
        )

    return result


@app.get(
    "/v1/michigan/licenses/delta/weeks",
    response_model=WeeksResponse,
    summary="List all weeks with available data",
    tags=["Michigan Liquor Licenses"],
)
def list_weeks():
    """
    Returns all available week-ending dates and their change counts.
    Use a `week` value from this list as the `week` parameter in the delta endpoint.
    """
    weeks = db.get_available_weeks()
    return WeeksResponse(available_weeks=weeks, count=len(weeks))


@app.get(
    "/v1/health",
    response_model=HealthResponse,
    summary="Service health check",
    tags=["System"],
)
def health():
    """Check that the service is running and the database is reachable."""
    try:
        latest = db.get_latest_week()
        total = db.get_total_records()
        return HealthResponse(
            status="ok",
            latest_week=latest,
            total_records_stored=total,
        )
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "detail": str(e)},
        )


# ── Admin endpoints ────────────────────────────────────────────────────────

@app.post(
    "/admin/run-pipeline",
    summary="Manually trigger the delta pipeline",
    tags=["Admin"],
    dependencies=[Depends(require_admin)],
)
def trigger_pipeline():
    """
    Manually trigger a LARA download and delta computation.
    Requires the X-Admin-Key header matching ADMIN_API_KEY env var.
    Normally runs automatically every Saturday at 08:00 ET.
    """
    success = run_weekly_pipeline()
    if success:
        latest = db.get_latest_week()
        return {"status": "success", "week_processed": str(latest)}
    else:
        return {
            "status": "skipped",
            "reason": "Data unchanged since last download, or pipeline error. Check logs.",
        }
