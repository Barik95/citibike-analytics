-- Computes rolling availability metrics per station using window functions.
-- One row per station per snapshot, enriched with rolling averages.

with history as (

    select * from {{ ref('int_station_status_history') }}

),

windowed as (

    select
        station_id,
        station_name,
        region_id,
        region_name,
        total_docks,
        last_reported_at,
        ingested_at,
        num_bikes_available,
        num_bikes_disabled,
        num_docks_available,
        num_docks_disabled,
        is_installed,
        is_renting,
        is_returning,
        bike_availability_rate,
        dock_availability_rate,

        -- rolling 3-snapshot average availability
        avg(bike_availability_rate) over (
            partition by station_id
            order by ingested_at
            rows between 2 preceding and current row
        ) as rolling_3_bike_availability_rate,

        -- lag to detect changes between snapshots
        lag(num_bikes_available) over (
            partition by station_id
            order by ingested_at
        ) as prev_bikes_available,

        lag(num_docks_available) over (
            partition by station_id
            order by ingested_at
        ) as prev_docks_available,

        -- snapshot rank per station (latest = 1)
        row_number() over (
            partition by station_id
            order by ingested_at desc
        ) as snapshot_recency_rank

    from history

)

select * from windowed
