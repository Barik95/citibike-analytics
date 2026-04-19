-- One row per station intervention.
-- Joins seed intervention records with station metadata.

with interventions as (

    select * from {{ ref('station_interventions') }}

),

stations as (

    select
        station_id,
        station_name,
        region_id,
        region_name,
        total_docks,
        capacity_tier,
        health_score,
        health_status

    from {{ ref('dim_stations') }}

),

final as (

    select
        i.station_id,
        s.station_name,
        s.region_id,
        s.region_name,
        s.total_docks,
        s.capacity_tier,
        s.health_score           as current_health_score,
        s.health_status          as current_health_status,

        cast(i.intervention_date as date)   as intervention_date,
        i.intervention_type,
        i.notes

    from interventions i
    left join stations s using (station_id)

)

select * from final
