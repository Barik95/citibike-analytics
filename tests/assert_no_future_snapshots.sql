-- Fails if any snapshot has an ingested_at timestamp in the future.
-- Future timestamps indicate a system clock issue in the ingestion script.

select
    station_id,
    ingested_at

from {{ ref('fct_station_status_snapshots') }}

where ingested_at > current_timestamp()
