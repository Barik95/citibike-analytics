-- stg_citibike__station_status.sql
-- Cleans and types real-time station availability data
-- One row per station_id per ingestion snapshot

with source as (

    select * from {{ source('citibike_raw', 'raw_station_status') }}

),

renamed as (

    select
        -- ids
        station_id,

        -- bike availability
        cast(num_bikes_available as int64)      as num_bikes_available,
        cast(num_ebikes_available as int64)     as num_ebikes_available,
        cast(num_bikes_disabled as int64)       as num_bikes_disabled,

        -- dock availability
        cast(num_docks_available as int64)      as num_docks_available,
        cast(num_docks_disabled as int64)       as num_docks_disabled,

        -- station state
        cast(is_installed as bool)              as is_installed,
        cast(is_renting as bool)                as is_renting,
        cast(is_returning as bool)              as is_returning,

        -- timestamps
        timestamp_seconds(
            cast(last_reported as int64)
        )                                       as last_reported_at,
        cast(ingested_at as timestamp)          as ingested_at

    from source

)

select * from renamed
