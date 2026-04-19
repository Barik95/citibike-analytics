-- One row per pricing plan. Latest snapshot only.

with plans as (

    select
        *,
        row_number() over (
            partition by plan_id
            order by ingested_at desc
        ) as rn

    from {{ ref('stg_citibike__pricing_plans') }}

)

select
    plan_id,
    plan_name,
    currency,
    price,
    is_taxable,
    description,
    ingested_at

from plans
where rn = 1
