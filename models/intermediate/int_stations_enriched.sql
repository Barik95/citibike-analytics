-- Joins station master data with its region name.
-- One row per station.

with stations as (

    select * from {{ ref('stg_citibike__stations') }}

),

regions as (

    select * from {{ ref('stg_citibike__regions') }}

),

enriched as (

    select
        s.station_id,
        s.station_name,
        s.short_name,
        s.latitude,
        s.longitude,
        s.total_docks,
        s.has_kiosk,
        s.region_id,
        r.region_name,
        s.ingested_at

    from stations s
    left join regions r using (region_id)

)

select * from enriched
