-- stg_citibike__regions.sql
-- Cleans and types CitiBike system regions
-- One row per region_id

with source as (

    select * from {{ source('citibike_raw', 'raw_system_regions') }}

),

renamed as (

    select
        -- ids
        region_id,

        -- region details
        name                                    as region_name,

        -- metadata
        cast(ingested_at as timestamp)          as ingested_at

    from source

)

select * from renamed
