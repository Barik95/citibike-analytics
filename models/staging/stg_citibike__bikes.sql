with source as (

    select * from {{ source('citibike_raw', 'raw_free_bike_status') }}

),

renamed as (

    select
        bike_id,
        cast(lat as float64)            as latitude,
        cast(lon as float64)            as longitude,
        cast(is_reserved as bool)       as is_reserved,
        cast(is_disabled as bool)       as is_disabled,
        vehicle_type_id,
        cast(ingested_at as timestamp)  as ingested_at

    from source

)

select * from renamed
