{% snapshot snap_station_operational_status %}

{{
    config(
        target_schema='citibike_snapshots',
        unique_key='station_id',
        strategy='check',
        check_cols=['is_installed', 'is_renting', 'is_returning'],
        invalidate_hard_deletes=True
    )
}}

-- Tracks when a station's operational flags change over time (SCD Type 2).
-- A new record is created whenever a station goes offline, stops renting,
-- or stops accepting returns — and again when it recovers.
-- Use this to measure outage duration and recovery patterns.

select
    station_id,
    station_name,
    region_id,
    region_name,
    is_installed,
    is_renting,
    is_returning,
    num_bikes_available,
    num_docks_available,
    last_reported_at,
    ingested_at

from {{ ref('int_station_status_history') }}
-- most recent snapshot per station only — snapshots track the current state, not history
qualify row_number() over (partition by station_id order by ingested_at desc) = 1

{% endsnapshot %}
