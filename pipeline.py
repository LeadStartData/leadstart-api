"""
pipeline.py — Download LARA MasterList and compute week-over-week delta.

The LARA MasterList is a public Excel file updated weekly by the Michigan
Liquor Control Commission. This module handles:
  1. Downloading the current file
  2. Loading the previous file from local cache
  3. Comparing the two to detect all license status changes
  4. Persisting results to the database

LARA Download URL (as of 2026):
  https://www.michigan.gov/lara/bureau-list/lcc/licensing-list
  Direct XLS link rotates — we fetch the page and parse the href.
"""

import io
import re
import hashlib
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Tuple, List

import database as db
from models import LicenseChange, DeltaSummary, SignalType, SIGNAL_LABELS

logger = logging.getLogger("pipeline")

# ── Paths ──────────────────────────────────────────────────────────────────
# Points at your existing Raw folder one level up.
# If running from D:\BLUE\LeadStart\api\, this resolves to D:\BLUE\LeadStart\Raw\
# You can also set LEADSTART_RAW_DIR environment variable to override.
import os
_raw_env = os.environ.get("LEADSTART_RAW_DIR")
CACHE_DIR = Path(_raw_env) if _raw_env else Path(__file__).parent.parent / "Raw"
CACHE_DIR.mkdir(exist_ok=True)

# ── LARA page / download ───────────────────────────────────────────────────
LARA_LISTING_URL = "https://www.michigan.gov/lara/bureau-list/lcc/licensing-list"
DIRECT_DOWNLOAD_URL = (
    "https://www.michigan.gov/documents/lara/Mdos-LCC-MasterList_557813_7.xlsx"
)
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; LeadStartDataBot/1.0; "
        "+https://rapidapi.com/leadstart)"
    )
}

# ── LARA column name map ───────────────────────────────────────────────────
# LARA occasionally renames columns. Map all known variants to internal names.
COLUMN_MAP = {
    # License number — actual LARA column is "number"
    "number": "license_number",
    "license number": "license_number",
    "licensenumber": "license_number",
    "lic_no": "license_number",
    "lara business id": "business_id",
    # Business name
    "dba_name": "dba_name",
    "dba": "dba_name",
    "doing business as": "dba_name",
    "account name": "account_name",
    "licensee": "dba_name",
    "business name": "dba_name",
    # Address
    "address": "address",
    "street address": "address",
    "premise address": "address",
    # City — LARA calls this the LGU (Local Government Unit)
    "city": "city",
    "current lgu: lgu name": "city",
    "issuing lgu: lgu name": "city",
    "lgu name": "city",
    # County
    "county": "county",
    "county: county": "county",
    # Status
    "status": "status",
    "license status": "status",
    # License type
    "license_type": "license_type",
    "license type": "license_type",
    "type": "license_type",
    "licensetype": "license_type",
    "group": "license_group",
    "subtype": "license_subtype",
}


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and map column names to internal schema."""
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})
    # Drop duplicate columns that result from multiple source cols mapping to same name
    df = df.loc[:, ~df.columns.duplicated(keep="first")]
    return df


def _file_hash(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


def download_master_list() -> Optional[Path]:
    """
    Download the current LARA MasterList xlsx.
    Returns local path if new data found, None if unchanged since last download.
    """
    logger.info("Attempting LARA MasterList download...")

    try:
        # First try scraping the listing page for the real current link
        resp = requests.get(LARA_LISTING_URL, headers=REQUEST_HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        xlsx_link = None
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "masterlist" in href.lower() and href.lower().endswith(".xlsx"):
                xlsx_link = href if href.startswith("http") else f"https://www.michigan.gov{href}"
                break
        download_url = xlsx_link or DIRECT_DOWNLOAD_URL
    except Exception as e:
        logger.warning(f"Could not scrape listing page ({e}), using direct URL")
        download_url = DIRECT_DOWNLOAD_URL

    logger.info(f"Downloading from: {download_url}")
    resp = requests.get(download_url, headers=REQUEST_HEADERS, timeout=60)
    resp.raise_for_status()

    today = date.today().strftime("%Y_%m_%d")
    dest = CACHE_DIR / f"MasterList_{today}.xlsx"
    dest.write_bytes(resp.content)
    logger.info(f"Saved to {dest} ({len(resp.content):,} bytes)")

    # Check if identical to most recent cached file
    existing = sorted(CACHE_DIR.glob("MasterList_*.xlsx"))
    if len(existing) >= 2:
        prev = existing[-2]  # second-most-recent (we just wrote the latest)
        if _file_hash(dest) == _file_hash(prev):
            logger.info("File unchanged since last download — skipping delta.")
            dest.unlink()
            return None

    return dest


def _load_master_list(path: Path) -> pd.DataFrame:
    """Load and normalise a LARA MasterList xlsx."""
    df = pd.read_excel(path, skiprows=1, dtype=str)
    df = _normalise_columns(df)

    required = {"license_number", "status"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"LARA file missing expected columns: {missing}. "
            f"Actual columns: {list(df.columns)}"
        )

    # Normalise values
    df["license_number"] = df["license_number"].str.strip()
    df["status"] = df["status"].str.strip().str.title()

    # If dba_name is missing but account_name exists, use that
    if "dba_name" not in df.columns and "account_name" in df.columns:
        df["dba_name"] = df["account_name"]

    for col in ["dba_name", "account_name", "address", "city", "county", "license_type"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace("nan", None)

    # Drop rows with no license number
    df = df[df["license_number"].notna() & (df["license_number"] != "")]
    # Deduplicate — keep first occurrence if same license number appears twice
    df = df[~df["license_number"].duplicated(keep="first")]
    return df.set_index("license_number")


def _clean(val) -> Optional[str]:
    """Convert pandas NaN/None to None, otherwise return stripped string."""
    import pandas as pd
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    return s if s and s.lower() not in ("nan", "none", "") else None



def _classify_signal(old_status: Optional[str], new_status: Optional[str]) -> Optional[SignalType]:
    """Map a status transition to a signal type. Returns None if not interesting."""
    if old_status is None and new_status is not None:
        return SignalType.new_license
    if new_status is None and old_status is not None:
        return SignalType.removed
    if old_status == new_status:
        return None  # no change — address change handled separately

    transitions = {
        ("Conditional", "Active"): SignalType.activation,
        ("Active", "Escrowed"): SignalType.escrowed,
        ("Escrowed", "Active"): SignalType.reactivation,
        ("Conditional", "Escrowed"): SignalType.escrowed,
    }
    return transitions.get((old_status, new_status))


def compute_delta(old_path: Path, new_path: Path, week_ending: date) -> Tuple[DeltaSummary, List[LicenseChange]]:
    """
    Compare two MasterList files and return the full change set.
    """
    logger.info(f"Computing delta: {old_path.name} → {new_path.name}")
    old_df = _load_master_list(old_path)
    new_df = _load_master_list(new_path)

    all_licenses = old_df.index.union(new_df.index)
    changes: List[LicenseChange] = []

    for lic in all_licenses:
        in_old = lic in old_df.index
        in_new = lic in new_df.index

        old_row = old_df.loc[lic] if in_old else None
        new_row = new_df.loc[lic] if in_new else None

        old_status = old_row["status"] if in_old else None
        new_status = new_row["status"] if in_new else None

        signal = _classify_signal(old_status, new_status)

        # Check address change separately (only for records in both)
        if in_old and in_new and signal is None:
            old_addr = old_row.get("address", "") or ""
            new_addr = new_row.get("address", "") or ""
            if old_addr.strip().lower() != new_addr.strip().lower() and new_addr.strip():
                signal = SignalType.location_change

        if signal is None:
            continue

        source_row = new_row if in_new else old_row
        changes.append(LicenseChange(
            license_number=lic,
            dba_name=_clean(source_row.get("dba_name")) or _clean(source_row.get("account_name")),
            address=_clean(source_row.get("address")),
            city=_clean(source_row.get("city")),
            county=_clean(source_row.get("county")),
            state="MI",
            license_type=_clean(source_row.get("license_type")),
            previous_status=old_status,
            current_status=new_status,
            signal_type=signal,
            signal_label=SIGNAL_LABELS[signal],
            detected_date=week_ending,
        ))

    # Build summary
    from collections import Counter
    counts = Counter(c.signal_type for c in changes)
    summary = DeltaSummary(
        new_licenses=counts.get(SignalType.new_license, 0),
        activations=counts.get(SignalType.activation, 0),
        escrowed=counts.get(SignalType.escrowed, 0),
        reactivations=counts.get(SignalType.reactivation, 0),
        location_changes=counts.get(SignalType.location_change, 0),
        removed=counts.get(SignalType.removed, 0),
        total=len(changes),
    )

    logger.info(
        f"Delta complete — {summary.total} changes detected "
        f"({summary.new_licenses} new, {summary.activations} activations, "
        f"{summary.escrowed} escrowed, {summary.removed} removed)"
    )
    return summary, changes


def run_weekly_pipeline() -> bool:
    """
    Full pipeline: download → diff → persist.
    Called by the scheduler. Returns True if new data was processed.
    """
    try:
        new_file = download_master_list()
        if new_file is None:
            logger.info("No new data this week.")
            return False

        # Find the previous file
        cached = sorted(CACHE_DIR.glob("MasterList_*.xlsx"))
        if len(cached) < 2:
            logger.info("Only one file in cache — need at least two to compute delta.")
            return False

        old_file = cached[-2]
        week_ending = date.today()

        summary, changes = compute_delta(old_file, new_file, week_ending)
        db.upsert_delta_run(week_ending, summary, changes)
        logger.info(f"Pipeline complete. Week {week_ending} stored with {summary.total} changes.")
        return True

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        return False


def backfill_from_local_files():
    """
    One-time utility: process all existing MasterList files in cache/ in order.
    Run manually to seed the database from your existing archive.

    Usage: python -c "from pipeline import backfill_from_local_files; backfill_from_local_files()"
    """
    files = sorted(CACHE_DIR.glob("MasterList_*.xlsx"))
    logger.info(f"Found {len(files)} local files for backfill.")
    if len(files) < 2:
        logger.warning("Need at least 2 files to backfill.")
        return

    for i in range(1, len(files)):
        old_f = files[i - 1]
        new_f = files[i]
        # Derive week_ending from filename — handles both:
        #   MasterList_2026_03_26.xlsx  (underscore, your format)
        #   MasterList_2026-03-26.xlsx  (dash format)
        try:
            m = re.search(r"(\d{4})[_-](\d{2})[_-](\d{2})", new_f.name)
            week_ending = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            week_ending = date.today()

        logger.info(f"Backfilling {old_f.name} → {new_f.name} ...")
        summary, changes = compute_delta(old_f, new_f, week_ending)
        db.upsert_delta_run(week_ending, summary, changes)

    logger.info("Backfill complete.")
