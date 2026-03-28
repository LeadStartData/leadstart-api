"""
database.py — SQLite persistence for license delta records.
Stores every processed delta run so the API can serve historical weeks.
"""

import sqlite3
import json
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
from models import LicenseChange, DeltaSummary, DeltaResponse, AvailableWeek, SIGNAL_LABELS


DB_PATH = Path("leadstart.db")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create tables if they don't exist."""
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS delta_runs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                week_ending TEXT NOT NULL UNIQUE,   -- ISO date string YYYY-MM-DD
                generated_at TEXT NOT NULL,
                total_changes INTEGER NOT NULL,
                summary_json TEXT NOT NULL           -- JSON blob of DeltaSummary
            );

            CREATE TABLE IF NOT EXISTS license_changes (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                week_ending     TEXT NOT NULL,
                license_number  TEXT NOT NULL,
                dba_name        TEXT,
                address         TEXT,
                city            TEXT,
                county          TEXT,
                state           TEXT DEFAULT 'MI',
                license_type    TEXT,
                previous_status TEXT,
                current_status  TEXT,
                signal_type     TEXT NOT NULL,
                signal_label    TEXT NOT NULL,
                detected_date   TEXT NOT NULL,
                FOREIGN KEY (week_ending) REFERENCES delta_runs(week_ending)
            );

            CREATE INDEX IF NOT EXISTS idx_changes_week ON license_changes(week_ending);
            CREATE INDEX IF NOT EXISTS idx_changes_signal ON license_changes(signal_type);
            CREATE INDEX IF NOT EXISTS idx_changes_county ON license_changes(county);
        """)


def upsert_delta_run(week_ending: date, summary: DeltaSummary, changes: List[LicenseChange]):
    """Store a processed delta run. Replaces existing data for that week."""
    week_str = week_ending.isoformat()
    now = datetime.utcnow().isoformat()

    with get_conn() as conn:
        # Upsert the run record
        conn.execute("""
            INSERT INTO delta_runs (week_ending, generated_at, total_changes, summary_json)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(week_ending) DO UPDATE SET
                generated_at = excluded.generated_at,
                total_changes = excluded.total_changes,
                summary_json = excluded.summary_json
        """, (week_str, now, summary.total, json.dumps(summary.dict())))

        # Delete old changes for this week and re-insert
        conn.execute("DELETE FROM license_changes WHERE week_ending = ?", (week_str,))

        rows = []
        for c in changes:
            rows.append((
                week_str,
                c.license_number,
                c.dba_name,
                c.address,
                c.city,
                c.county,
                c.state,
                c.license_type,
                c.previous_status,
                c.current_status,
                c.signal_type,
                c.signal_label,
                c.detected_date.isoformat(),
            ))

        conn.executemany("""
            INSERT INTO license_changes
            (week_ending, license_number, dba_name, address, city, county, state,
             license_type, previous_status, current_status, signal_type, signal_label, detected_date)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)


def get_latest_week() -> Optional[date]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT week_ending FROM delta_runs ORDER BY week_ending DESC LIMIT 1"
        ).fetchone()
    return date.fromisoformat(row["week_ending"]) if row else None


def get_available_weeks() -> List[AvailableWeek]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT week_ending, total_changes, generated_at FROM delta_runs ORDER BY week_ending DESC"
        ).fetchall()
    return [
        AvailableWeek(
            week_ending=date.fromisoformat(r["week_ending"]),
            total_changes=r["total_changes"],
            generated_at=datetime.fromisoformat(r["generated_at"]),
        )
        for r in rows
    ]


def get_delta(
    week_ending: Optional[date] = None,
    signal_type: Optional[str] = None,
    county: Optional[str] = None,
    limit: int = 1000,
    offset: int = 0,
) -> Optional[DeltaResponse]:
    """Fetch a delta run with optional filters."""

    with get_conn() as conn:
        # Resolve which week to use
        if week_ending is None:
            run_row = conn.execute(
                "SELECT * FROM delta_runs ORDER BY week_ending DESC LIMIT 1"
            ).fetchone()
        else:
            run_row = conn.execute(
                "SELECT * FROM delta_runs WHERE week_ending = ?",
                (week_ending.isoformat(),)
            ).fetchone()

        if not run_row:
            return None

        week_str = run_row["week_ending"]

        # Build the change query
        query = "SELECT * FROM license_changes WHERE week_ending = ?"
        params: List[Any] = [week_str]

        if signal_type:
            query += " AND signal_type = ?"
            params.append(signal_type)
        if county:
            query += " AND UPPER(county) = ?"
            params.append(county.upper())

        query += " ORDER BY signal_type, county, dba_name"
        query += f" LIMIT {limit} OFFSET {offset}"

        rows = conn.execute(query, params).fetchall()

    summary_dict = json.loads(run_row["summary_json"])
    summary = DeltaSummary(**summary_dict)

    changes = [
        LicenseChange(
            license_number=r["license_number"],
            dba_name=r["dba_name"],
            address=r["address"],
            city=r["city"],
            county=r["county"],
            state=r["state"] or "MI",
            license_type=r["license_type"],
            previous_status=r["previous_status"],
            current_status=r["current_status"],
            signal_type=r["signal_type"],
            signal_label=r["signal_label"],
            detected_date=date.fromisoformat(r["detected_date"]),
        )
        for r in rows
    ]

    return DeltaResponse(
        week_ending=date.fromisoformat(week_str),
        generated_at=datetime.fromisoformat(run_row["generated_at"]),
        total_changes=run_row["total_changes"],
        summary=summary,
        changes=changes,
    )


def get_total_records() -> int:
    with get_conn() as conn:
        row = conn.execute("SELECT COUNT(*) as n FROM license_changes").fetchone()
    return row["n"] if row else 0
