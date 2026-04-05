-- stg_citibike__stations.sql
-- Cleans and types raw station information
-- One row per station_id

with source as (

    select * from {{ source('citibike_raw', 'raw_station_information') }}

),

renamed as (

    select
        -- ids
        station_id,

        -- station details
        name                                    as station_name,
        short_name,
        region_id,

        -- location
        cast(lat as float64)                    as latitude,
        cast(lon as float64)                    as longitude,

        -- capacity
        cast(capacity as int64)                 as total_docks,
        cast(has_kiosk as bool)                 as has_kiosk,

        -- metadata
        cast(ingested_at as timestamp)          as ingested_at

    from source

)

select * from renamed