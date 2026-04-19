with source as (

    select * from {{ source('citibike_raw', 'raw_pricing_plans') }}

),

renamed as (

    select
        plan_id,
        name                                as plan_name,
        currency,
        cast(price as float64)              as price,
        cast(is_taxable as bool)            as is_taxable,
        description,
        cast(ingested_at as timestamp)      as ingested_at

    from source

)

select * from renamed
