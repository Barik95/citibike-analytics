-- One row per region with station counts and aggregate health.

with regions as (

    select * from {{ ref('stg_citibike__regions') }}

),

station_summary as (

    select
        region_id,
        count(*)                                        as total_stations,
        sum(total_docks)                                as total_docks,
        round(avg(health_score), 2)                     as avg_health_score,
        countif(health_status = 'healthy')              as healthy_stations,
        countif(health_status = 'at_risk')              as at_risk_stations,
        countif(health_status = 'critical')             as critical_stations

    from {{ ref('dim_stations') }}
    group by region_id

)

select
    r.region_id,
    r.region_name,
    s.total_stations,
    s.total_docks,
    s.avg_health_score,
    s.healthy_stations,
    s.at_risk_stations,
    s.critical_stations,
    r.ingested_at

from regions r
left join station_summary s using (region_id)
