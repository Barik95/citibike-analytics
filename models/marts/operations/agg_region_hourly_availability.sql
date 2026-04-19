-- Average availability by region and hour of day across all snapshots.
-- Answers: when does each region peak/trough in demand?

with snapshots as (

    select * from {{ ref('fct_station_status_snapshots') }}

),

hourly as (

    select
        region_id,
        region_name,
        extract(hour from ingested_at)                  as hour_of_day,
        extract(dayofweek from ingested_at)             as day_of_week,  -- 1=Sun, 7=Sat

        count(*)                                        as snapshot_count,
        count(distinct station_id)                      as stations_observed,
        round(avg(bike_availability_rate), 4)           as avg_bike_availability,
        round(avg(dock_availability_rate), 4)           as avg_dock_availability,
        round(min(bike_availability_rate), 4)           as min_bike_availability,
        round(max(bike_availability_rate), 4)           as max_bike_availability,
        countif(num_bikes_available = 0)                as empty_station_snapshots,
        round(
            safe_divide(
                countif(num_bikes_available = 0),
                count(*)
            ),
            4
        )                                               as empty_station_rate

    from snapshots
    group by 1, 2, 3, 4

)

select * from hourly
