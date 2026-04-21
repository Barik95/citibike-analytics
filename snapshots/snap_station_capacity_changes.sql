{% snapshot snap_station_capacity_changes %}

{{
    config(
        target_schema='citibike_snapshots',
        unique_key='station_id',
        strategy='check',
        check_cols=['total_docks', 'has_kiosk'],
        invalidate_hard_deletes=True
    )
}}

-- Tracks when a station's physical capacity changes over time (SCD Type 2).
-- A new record is created whenever total_docks or has_kiosk changes.
-- Use this to answer: did adding docks at a station improve availability?

select
    station_id,
    station_name,
    region_id,
    region_name,
    total_docks,
    has_kiosk,
    latitude,
    longitude,
    ingested_at

from {{ ref('int_stations_enriched') }}
qualify row_number() over (partition by station_id order by ingested_at desc) = 1

{% endsnapshot %}
