-- Daily performance rollup per station.
-- Answers: which stations are chronically understocked? which need rebalancing?

with snapshots as (

    select * from {{ ref('fct_station_status_snapshots') }}

),

daily as (

    select
        station_id,
        station_name,
        region_id,
        region_name,
        total_docks,
        date(ingested_at)                                       as snapshot_date,

        count(*)                                                as snapshots_that_day,
        round(avg(bike_availability_rate), 4)                   as avg_bike_availability,
        round(avg(dock_availability_rate), 4)                   as avg_dock_availability,
        round(min(bike_availability_rate), 4)                   as min_bike_availability,
        round(max(bike_availability_rate), 4)                   as max_bike_availability,

        -- how many snapshots had zero bikes (station was empty)
        countif(num_bikes_available = 0)                        as empty_snapshots,
        countif(num_docks_available = 0)                        as full_snapshots,

        round(
            safe_divide(
                countif(num_bikes_available = 0),
                count(*)
            ),
            4
        )                                                       as empty_rate,

        sum(num_bikes_available)                                as total_bikes_available_sum,
        sum(num_ebikes_available)                               as total_ebikes_available_sum,
        sum(num_bikes_disabled)                                 as total_bikes_disabled_sum,

        -- rebalancing priority score: higher = more urgently needs attention
        -- weight empty snapshots heavily, penalise consistently low availability
        round(
            (safe_divide(countif(num_bikes_available = 0), count(*)) * 60)
            + ((1 - avg(bike_availability_rate)) * 40),
            2
        )                                                       as rebalancing_priority_score

    from snapshots
    group by 1, 2, 3, 4, 5, 6

)

select * from daily
