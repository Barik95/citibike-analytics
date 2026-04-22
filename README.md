# CitiBike Analytics Platform

A production-grade analytics engineering project built on live CitiBike data. Demonstrates end-to-end data engineering: live API ingestion, a multi-layer dbt data model on BigQuery, automated testing, SCD Type 2 snapshots, and an Airflow orchestration layer.

---

## Architecture

```
CitiBike GBFS API (live, every 30s)
        │
        ▼
┌─────────────────┐
│  Python Ingest  │  scripts/ingest_citibike.py
│  5 endpoints    │  → BigQuery citibike_raw dataset
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Staging Layer  │  models/staging/   (views)
│  5 models       │  clean, rename, type-cast
└────────┬────────┘
         │
         ▼
┌─────────────────────┐
│  Intermediate Layer │  models/intermediate/   (views)
│  5 models           │  joins, derived metrics, health scores
└────────┬────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│              Mart Layer                 │  models/marts/   (tables)
│  Core        Operations    Experiments  │
│  4 models    3 models      2 models     │
└────────┬────────────────────────────────┘
         │
         ▼
┌──────────────────┐    ┌──────────────────┐
│   Snapshots      │    │   Airflow DAGs   │
│   SCD Type 2     │    │   30-min + daily │
└──────────────────┘    └──────────────────┘
```

---

## Business Questions Answered

| Question | Model |
|---|---|
| Which stations are chronically under-stocked? | `agg_station_daily_performance` — rebalancing priority score |
| How does availability vary by region and hour? | `agg_region_hourly_availability` |
| Which stations are healthy vs at risk? | `int_station_health_scores` — composite 0–100 score |
| What is fleet utilisation across bike types? | `fct_bike_availability` — e-bike vs classic split |
| Did a station intervention improve availability? | `agg_intervention_results` — pre/post A/B analysis |

---

## Data Model

### Raw Layer — `citibike_raw` dataset
As-ingested from CitiBike GBFS API. Never modified.

| Table | Source | Rows per run |
|---|---|---|
| `raw_station_information` | `/station_information` | ~2,400 |
| `raw_station_status` | `/station_status` | ~2,400 |
| `raw_system_regions` | `/system_regions` | 7 |
| `raw_free_bike_status` | `/free_bike_status` | variable |
| `raw_pricing_plans` | `/system_pricing_plans` | 1 |

### Staging Layer — `citibike_staging` dataset (views)
Clean, renamed, type-cast. One model per raw table.

`stg_citibike__stations` · `stg_citibike__station_status` · `stg_citibike__regions` · `stg_citibike__bikes` · `stg_citibike__pricing_plans`

### Intermediate Layer — `citibike_staging` dataset (views)

| Model | What it computes |
|---|---|
| `int_stations_enriched` | Stations joined with region names |
| `int_station_status_history` | Snapshots + station metadata + bike/dock availability rates |
| `int_station_availability_windows` | Rolling 3-snapshot averages, lag deltas, recency rank |
| `int_bikes_with_station` | Dockless bikes matched to nearest station via haversine distance |
| `int_station_health_scores` | Composite health score (0–100) per station, labeled healthy/at_risk/critical |

### Mart Layer — `citibike_staging` dataset (tables)

**Core**
- `dim_stations` — station master with capacity tier and health score
- `dim_regions` — regions with station counts and health breakdown
- `dim_pricing_plans` — latest pricing plan per plan
- `fct_station_status_snapshots` — incremental fact table, all status snapshots

**Operations**
- `fct_bike_availability` — fleet-level utilisation per ingestion run
- `agg_station_daily_performance` — daily rollup with rebalancing priority score
- `agg_region_hourly_availability` — hourly availability patterns by region and day of week

**Experiments**
- `fct_station_interventions` — intervention records joined with station metadata
- `agg_intervention_results` — pre/post availability delta per intervention (±14 days)

### Snapshots — `citibike_snapshots` dataset (SCD Type 2)
- `snap_station_capacity_changes` — tracks when total docks or kiosk status changes
- `snap_station_operational_status` — tracks when a station goes offline or stops renting

---

## Key Technical Details

**Incremental loading** — `fct_station_status_snapshots` uses `merge` strategy: each run only processes new snapshots, keeping BigQuery costs near zero.

**Haversine distance** — `int_bikes_with_station` matches dockless bikes to their nearest station using a hand-rolled haversine formula (BigQuery has no `RADIANS()` function).

**Health scoring** — `int_station_health_scores` builds a 0–100 composite from four 25-point components: bike availability, dock availability, operational flags, and disabled rate.

**Availability rate capping** — CitiBike occasionally reports more bikes than a station's listed capacity (capacity changes mid-day). All rates are capped at 1.0.

**A/B experiment infrastructure** — `agg_intervention_results` computes pre/post availability deltas using a pivot + window function pattern, backed by an approximate z-test significance macro.

---

## Project Structure

```
citibike-analytics/
├── scripts/
│   └── ingest_citibike.py        # Ingests all 5 GBFS endpoints into BigQuery
├── models/
│   ├── staging/
│   │   ├── sources.yml           # Source definitions + column docs
│   │   ├── schema.yml            # 23 automated tests
│   │   └── stg_citibike__*.sql   # 5 staging models
│   ├── intermediate/
│   │   └── int_*.sql             # 5 intermediate models
│   └── marts/
│       ├── core/                 # Dimensions + incremental fact
│       ├── operations/           # Aggregations for dashboards
│       └── experiments/          # A/B test infrastructure
├── seeds/
│   ├── station_interventions.csv
│   ├── nyc_boroughs.csv
│   ├── station_capacity_tiers.csv
│   └── bike_type_definitions.csv
├── snapshots/                    # SCD Type 2 tracking
├── macros/                       # availability_bucket, haversine_distance, health_score, z-test
├── tests/                        # 3 custom SQL data tests
├── airflow/
│   └── dags/
│       ├── citibike_pipeline.py  # 30-min ingest + staging DAG
│       └── citibike_daily.py     # Daily mart rebuild + snapshot DAG
└── .github/workflows/
    └── dbt_ci.yml                # CI: compile + test on every PR
```

---

## Setup

### Prerequisites
- Python 3.11+
- Google Cloud project with BigQuery enabled and billing linked
- Service account JSON key with BigQuery Data Editor + Job User roles

### 1. Install dependencies
```bash
pip install requests google-cloud-bigquery dbt-core dbt-bigquery
```

### 2. Configure BigQuery credentials
```bash
export GOOGLE_APPLICATION_CREDENTIALS=~/path/to/keyfile.json
```

### 3. Run ingestion
```bash
python scripts/ingest_citibike.py
```

### 4. Run dbt
```bash
dbt seed
dbt run
dbt snapshot
dbt test
```

### 5. Set up Airflow (optional)
Install [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli), then:
```bash
cd airflow
astro dev start
```

---

## CI/CD

Every pull request touching `models/`, `tests/`, `macros/`, `snapshots/`, or `seeds/` triggers the GitHub Actions workflow which compiles all models, loads seeds, runs all models, and runs all 23 tests.

Add your service account JSON as a GitHub secret named `GCP_SERVICE_ACCOUNT_JSON` to enable CI.

---

## Data Source

[CitiBike GBFS](https://gbfs.citibikenyc.com/gbfs/gbfs.json) — free, public, no API key required. Updated every 30 seconds.
