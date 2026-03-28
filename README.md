# LeadStart — Michigan Liquor License Delta API

**Week-over-week change detection for Michigan liquor licenses.**  
Stop downloading spreadsheets. Get a clean JSON feed of what actually changed.

---

## What This API Does

The Michigan Liquor Control Commission publishes a full MasterList of all active 
liquor licenses every week. It's public, it's messy, and it's 17,000+ rows with 
no change tracking.

This API downloads that file every Saturday, diffs it against the prior week, and 
serves the delta as clean, structured JSON.

---

## Signal Types

| Signal | Meaning | Who cares |
|--------|---------|-----------|
| `new_license` | License not in previous week | Distributors, insurance agents, equipment suppliers |
| `activation` | Conditional → Active (ready to operate) | Liquor distributors (first sale opportunity) |
| `escrowed` | Active → Escrowed (ownership transfer) | Business brokers, attorneys |
| `reactivation` | Escrowed → Active (transfer complete) | Distributors |
| `location_change` | Same license, moved address | POS vendors, delivery services |
| `removed` | License dropped from MasterList | Compliance teams |

---

## Endpoints

### `GET /v1/michigan/licenses/delta`

Returns changes for a given week.

**Query parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `week` | date (YYYY-MM-DD) | latest | Week-ending date to retrieve |
| `signal_type` | string | all | Filter: new_license, activation, escrowed, reactivation, location_change, removed |
| `county` | string | all | Filter by county name (e.g. Wayne, Oakland, Macomb) |
| `limit` | int | 500 | Max results (1–1000) |
| `offset` | int | 0 | Pagination offset |

**Example request:**
```
GET /v1/michigan/licenses/delta?signal_type=activation&county=Wayne
```

**Example response:**
```json
{
  "week_ending": "2026-03-28",
  "generated_at": "2026-03-28T08:14:33Z",
  "source": "Michigan LARA - Liquor Control Commission",
  "source_url": "https://www.michigan.gov/lara/bureau-list/lcc/licensing-list",
  "total_changes": 127,
  "summary": {
    "new_licenses": 23,
    "activations": 18,
    "escrowed": 14,
    "reactivations": 9,
    "location_changes": 41,
    "removed": 22,
    "total": 127
  },
  "changes": [
    {
      "license_number": "L-000491269",
      "dba_name": "Morning Belle & Blue Porch",
      "address": "45225 Marketplace Blvd",
      "city": "Sterling Heights",
      "county": "Macomb",
      "state": "MI",
      "license_type": "SDD",
      "previous_status": "Conditional",
      "current_status": "Active",
      "signal_type": "activation",
      "signal_label": "Activation: Ready to Buy",
      "detected_date": "2026-03-28"
    }
  ]
}
```

---

### `GET /v1/michigan/licenses/delta/weeks`

Returns all available week-ending dates and change counts.

```json
{
  "count": 8,
  "available_weeks": [
    { "week_ending": "2026-03-28", "total_changes": 127, "generated_at": "..." },
    { "week_ending": "2026-03-21", "total_changes": 109, "generated_at": "..." }
  ]
}
```

---

### `GET /v1/health`

Service health check.

```json
{
  "status": "ok",
  "latest_week": "2026-03-28",
  "total_records_stored": 2841,
  "version": "1.0.0"
}
```

---

## Data Refresh Schedule

Every **Saturday at 8:00 AM Eastern** the pipeline:
1. Downloads the current LARA MasterList
2. Compares it to the previous week's file
3. Stores the delta in the database
4. Makes it available via the API immediately

---

## Who Should Use This

- **Liquor distributors** — get day-zero leads on new activations before competitors call
- **Insurance agents** — Michigan requires liquor liability coverage; new licensees need quotes
- **Business brokers** — escrow signals indicate ownership transfers in progress
- **Compliance software** — embed license change tracking without scraping LARA yourself
- **Commercial real estate** — location changes signal lease starts and business movement

---

## Running Your Own Instance

```
D:\BLUE\LeadStart\
├── Raw\                ← your existing MasterList files live here already
└── api\                ← unzip leadstart-api.zip contents here
```

```bash
cd D:\BLUE\LeadStart\api
pip install -r requirements.txt

# Seed the database from your existing Raw\ files (run once)
python -c "from pipeline import backfill_from_local_files; backfill_from_local_files()"

# Start API + scheduler
copy .env.example .env    # then edit ADMIN_API_KEY in Notepad
python scheduler.py
```

The API will be live at `http://localhost:8000`.  
Interactive docs at `http://localhost:8000/docs`.

---

## Source Data

All data is derived from the Michigan LARA Liquor Control Commission MasterList,
a public government document updated weekly. Source URL:
https://www.michigan.gov/lara/bureau-list/lcc/licensing-list

LeadStart does not claim ownership of the underlying data.
The value is in the processing, the delta detection, and the API layer.
