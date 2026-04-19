-- Computes a composite health score per station from the most recent snapshot.
-- One row per station. Score ranges 0–100.
-- Components: bike availability, dock availability, operational flags, disabled bike rate.

with latest as (

    select * from {{ ref('int_station_availability_windows') }}
    where snapshot_recency_rank = 1

),

scored as (

    select
        station_id,
        station_name,
        region_id,
        region_name,
        total_docks,
        num_bikes_available,
        num_docks_available,
        num_bikes_disabled,
        num_docks_disabled,
        is_installed,
        is_renting,
        is_returning,
        bike_availability_rate,
        dock_availability_rate,
        rolling_3_bike_availability_rate,
        last_reported_at,
        ingested_at,

        -- component scores (each 0–25)
        round(coalesce(bike_availability_rate, 0) * 25, 2)  as score_bike_availability,
        round(coalesce(dock_availability_rate, 0) * 25, 2)  as score_dock_availability,

        -- operational: 25 if all three flags are true, scaled down otherwise
        round(
            (
                cast(is_installed as int64)
                + cast(is_renting  as int64)
                + cast(is_returning as int64)
            ) / 3.0 * 25,
            2
        )                                                   as score_operational,

        -- disabled penalty: 25 if 0% disabled, 0 if 100% disabled
        round(
            case
                when total_docks = 0 then 25
                else greatest(
                    0,
                    25 - safe_divide(
                        num_bikes_disabled + num_docks_disabled,
                        total_docks
                    ) * 25
                )
            end,
            2
        )                                                   as score_low_disabled

    from latest

),

final as (

    select
        *,
        round(
            score_bike_availability
            + score_dock_availability
            + score_operational
            + score_low_disabled,
            2
        ) as health_score,

        case
            when score_bike_availability + score_dock_availability
                 + score_operational + score_low_disabled >= 75 then 'healthy'
            when score_bike_availability + score_dock_availability
                 + score_operational + score_low_disabled >= 50 then 'at_risk'
            else 'critical'
        end as health_status

    from scored

)

select * from final
