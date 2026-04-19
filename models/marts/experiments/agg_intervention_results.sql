-- Pre/post analysis for each station intervention.
-- Compares average bike availability in the 14 days before vs 14 days after intervention.

with interventions as (

    select
        station_id,
        station_name,
        region_name,
        intervention_date,
        intervention_type

    from {{ ref('fct_station_interventions') }}

),

snapshots as (

    select
        station_id,
        date(ingested_at)           as snapshot_date,
        bike_availability_rate,
        num_bikes_available,
        num_docks_available

    from {{ ref('fct_station_status_snapshots') }}

),

joined as (

    select
        i.station_id,
        i.station_name,
        i.region_name,
        i.intervention_date,
        i.intervention_type,

        s.snapshot_date,
        s.bike_availability_rate,
        s.num_bikes_available,
        s.num_docks_available,

        date_diff(s.snapshot_date, i.intervention_date, day) as days_from_intervention,

        case
            when s.snapshot_date < i.intervention_date  then 'pre'
            when s.snapshot_date >= i.intervention_date then 'post'
        end as period

    from interventions i
    inner join snapshots s using (station_id)
    -- only look at 14 days either side of the intervention
    where abs(date_diff(s.snapshot_date, i.intervention_date, day)) <= 14

),

aggregated as (

    select
        station_id,
        station_name,
        region_name,
        intervention_date,
        intervention_type,
        period,

        count(*)                                as snapshot_count,
        round(avg(bike_availability_rate), 4)   as avg_bike_availability,
        round(min(bike_availability_rate), 4)   as min_bike_availability,
        round(max(bike_availability_rate), 4)   as max_bike_availability,
        countif(num_bikes_available = 0)         as empty_snapshots

    from joined
    group by 1, 2, 3, 4, 5, 6

),

pivoted as (

    select
        station_id,
        station_name,
        region_name,
        intervention_date,
        intervention_type,

        max(case when period = 'pre'  then avg_bike_availability end) as pre_avg_availability,
        max(case when period = 'post' then avg_bike_availability end) as post_avg_availability,
        max(case when period = 'pre'  then snapshot_count end)        as pre_snapshot_count,
        max(case when period = 'post' then snapshot_count end)        as post_snapshot_count,
        max(case when period = 'pre'  then empty_snapshots end)       as pre_empty_snapshots,
        max(case when period = 'post' then empty_snapshots end)       as post_empty_snapshots

    from aggregated
    group by 1, 2, 3, 4, 5

),

final as (

    select
        *,
        round(post_avg_availability - pre_avg_availability, 4) as availability_delta,

        case
            when post_avg_availability > pre_avg_availability then 'improved'
            when post_avg_availability < pre_avg_availability then 'worsened'
            else 'no_change'
        end as intervention_outcome

    from pivoted

)

select * from final
