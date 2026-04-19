-- One row per station with latest attributes and capacity tier.
-- Rebuilt as a table on every dbt run.

with stations as (

    select * from {{ ref('int_stations_enriched') }}

),

health as (

    select
        station_id,
        health_score,
        health_status

    from {{ ref('int_station_health_scores') }}

),

capacity_tiers as (

    select * from {{ ref('station_capacity_tiers') }}

),

final as (

    select
        s.station_id,
        s.station_name,
        s.short_name,
        s.region_id,
        s.region_name,
        s.latitude,
        s.longitude,
        s.total_docks,
        s.has_kiosk,

        ct.tier_name                as capacity_tier,
        ct.description              as capacity_tier_description,

        h.health_score,
        h.health_status,

        s.ingested_at

    from stations s
    left join health h using (station_id)
    left join capacity_tiers ct
        on s.total_docks between ct.min_docks and ct.max_docks

)

select * from final
