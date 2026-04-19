import requests
from google.cloud import bigquery
from datetime import datetime, timezone
import os

# ── Config ────────────────────────────────────────────────────────
PROJECT_ID  = "citibike-analytics-492418"
DATASET     = "citibike_raw"
KEY_FILE    = os.path.expanduser(
    "~/Downloads/citibike-analytics-492418-d228aa789500.json"
)

# CitiBike GBFS API endpoints — no auth required, fully public
ENDPOINTS = {
    "station_information": "https://gbfs.citibikenyc.com/gbfs/en/station_information.json",
    "station_status":      "https://gbfs.citibikenyc.com/gbfs/en/station_status.json",
    "system_regions":      "https://gbfs.citibikenyc.com/gbfs/en/system_regions.json",
    "free_bike_status":    "https://gbfs.citibikenyc.com/gbfs/en/free_bike_status.json",
    "system_pricing_plans":"https://gbfs.citibikenyc.com/gbfs/en/system_pricing_plans.json",
}

# ── BigQuery client ───────────────────────────────────────────────
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_FILE
client = bigquery.Client(project=PROJECT_ID)

def fetch(url: str) -> dict:
    """Fetch JSON from a GBFS endpoint."""
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()

def load_to_bq(table_id: str, rows: list[dict]) -> None:
    """Load rows into BigQuery using batch load (free tier compatible)."""
    full_table = f"{PROJECT_ID}.{DATASET}.{table_id}"

    # Build schema from first row
    schema = []
    for key, value in rows[0].items():
        if isinstance(value, bool):
            field_type = "BOOL"
        elif isinstance(value, int):
            field_type = "INT64"
        elif isinstance(value, float):
            field_type = "FLOAT64"
        else:
            field_type = "STRING"
        schema.append(bigquery.SchemaField(key, field_type))

    # Create table if it doesn't exist
    table = bigquery.Table(full_table, schema=schema)
    try:
        client.create_table(table)
        print(f"  + Created table {table_id}")
    except Exception:
        pass  # Already exists

    # Batch load — works on free tier unlike streaming insert
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_json(rows, full_table, job_config=job_config)
    job.result()  # Wait for job to complete

    if job.errors:
        print(f"  ✗ Errors loading {table_id}: {job.errors}")
    else:
        print(f"  ✓ Loaded {len(rows)} rows into {table_id}")

def ingest_station_information() -> None:
    data        = fetch(ENDPOINTS["station_information"])
    stations    = data["data"]["stations"]
    ingested_at = datetime.now(timezone.utc).isoformat()

    rows = [
        {
            "station_id":  s.get("station_id"),
            "name":        s.get("name"),
            "short_name":  s.get("short_name"),
            "lat":         s.get("lat"),
            "lon":         s.get("lon"),
            "region_id":   s.get("region_id"),
            "capacity":    s.get("capacity"),
            "has_kiosk":   s.get("has_kiosk"),
            "ingested_at": ingested_at,
        }
        for s in stations
    ]
    load_to_bq("raw_station_information", rows)

def ingest_station_status() -> None:
    data        = fetch(ENDPOINTS["station_status"])
    stations    = data["data"]["stations"]
    ingested_at = datetime.now(timezone.utc).isoformat()

    rows = [
        {
            "station_id":           s.get("station_id"),
            "num_bikes_available":  s.get("num_bikes_available"),
            "num_bikes_disabled":   s.get("num_bikes_disabled"),
            "num_docks_available":  s.get("num_docks_available"),
            "num_docks_disabled":   s.get("num_docks_disabled"),
            "is_installed":         s.get("is_installed"),
            "is_renting":           s.get("is_renting"),
            "is_returning":         s.get("is_returning"),
            "last_reported":        s.get("last_reported"),
            "num_ebikes_available": s.get("num_ebikes_available"),
            "ingested_at":          ingested_at,
        }
        for s in stations
    ]
    load_to_bq("raw_station_status", rows)

def ingest_system_regions() -> None:
    data        = fetch(ENDPOINTS["system_regions"])
    regions     = data["data"]["regions"]
    ingested_at = datetime.now(timezone.utc).isoformat()

    rows = [
        {
            "region_id":   r.get("region_id"),
            "name":        r.get("name"),
            "ingested_at": ingested_at,
        }
        for r in regions
    ]
    load_to_bq("raw_system_regions", rows)

FREE_BIKE_SCHEMA = [
    bigquery.SchemaField("bike_id",         "STRING"),
    bigquery.SchemaField("lat",             "FLOAT64"),
    bigquery.SchemaField("lon",             "FLOAT64"),
    bigquery.SchemaField("is_reserved",     "BOOL"),
    bigquery.SchemaField("is_disabled",     "BOOL"),
    bigquery.SchemaField("vehicle_type_id", "STRING"),
    bigquery.SchemaField("ingested_at",     "STRING"),
]

def ingest_free_bike_status() -> None:
    data        = fetch(ENDPOINTS["free_bike_status"])
    bikes       = data["data"]["bikes"]
    ingested_at = datetime.now(timezone.utc).isoformat()

    # Always ensure table exists with correct schema
    full_table = f"{PROJECT_ID}.{DATASET}.raw_free_bike_status"
    table = bigquery.Table(full_table, schema=FREE_BIKE_SCHEMA)
    try:
        client.create_table(table)
        print("  + Created table raw_free_bike_status")
    except Exception:
        pass

    if not bikes:
        print("  ⚠ free_bike_status: no dockless bikes in field right now — table exists, no rows loaded")
        return

    rows = [
        {
            "bike_id":          b.get("bike_id"),
            "lat":              b.get("lat"),
            "lon":              b.get("lon"),
            "is_reserved":      b.get("is_reserved"),
            "is_disabled":      b.get("is_disabled"),
            "vehicle_type_id":  b.get("vehicle_type_id"),
            "ingested_at":      ingested_at,
        }
        for b in bikes
    ]
    load_to_bq("raw_free_bike_status", rows)

def ingest_system_pricing_plans() -> None:
    data        = fetch(ENDPOINTS["system_pricing_plans"])
    plans       = data["data"]["plans"]
    ingested_at = datetime.now(timezone.utc).isoformat()

    rows = [
        {
            "plan_id":      p.get("plan_id"),
            "name":         p.get("name"),
            "currency":     p.get("currency"),
            "price":        str(p.get("price", "")),
            "is_taxable":   p.get("is_taxable"),
            "description":  p.get("description"),
            "ingested_at":  ingested_at,
        }
        for p in plans
    ]
    load_to_bq("raw_pricing_plans", rows)

if __name__ == "__main__":
    print(f"\n🚲 CitiBike ingestion — {datetime.now(timezone.utc).isoformat()}\n")
    ingest_station_information()
    ingest_station_status()
    ingest_system_regions()
    ingest_free_bike_status()
    ingest_system_pricing_plans()
    print("\n✅ Done\n")