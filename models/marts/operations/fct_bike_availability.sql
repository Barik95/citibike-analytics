-- Fleet-level availability snapshot per ingestion run.
-- One row per ingestion timestamp — answers "what was the state of the whole fleet?"

with snapshots as (

    select * from {{ ref('fct_station_status_snapshots') }}

),

fleet as (

    select
        ingested_at,

        count(distinct station_id)                              as total_stations,
        sum(num_bikes_available)                                as total_bikes_available,
        sum(num_ebikes_available)                               as total_ebikes_available,
        sum(num_bikes_available) - sum(num_ebikes_available)    as total_classic_bikes_available,
        sum(num_bikes_disabled)                                 as total_bikes_disabled,
        sum(num_docks_available)                                as total_docks_available,
        sum(num_docks_disabled)                                 as total_docks_disabled,

        -- fleet utilisation: % of all docks that have a bike
        round(
            safe_divide(
                sum(num_bikes_available),
                sum(num_bikes_available) + sum(num_docks_available)
            ),
            4
        )                                                       as fleet_utilisation_rate,

        -- e-bike share of available fleet
        round(
            safe_divide(sum(num_ebikes_available), sum(num_bikes_available)),
            4
        )                                                       as ebike_share_rate,

        countif(num_bikes_available = 0)                        as empty_stations,
        countif(num_docks_available = 0)                        as full_stations,
        countif(not is_installed or not is_renting)             as inactive_stations

    from snapshots
    group by ingested_at

)

select * from fleet
