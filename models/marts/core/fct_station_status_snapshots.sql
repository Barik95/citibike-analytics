{{
    config(
        materialized='incremental',
        unique_key=['station_id', 'ingested_at'],
        incremental_strategy='merge'
    )
}}

-- Every status snapshot per station — the main fact table.
-- Incremental: only new snapshots are merged on each run.

with history as (

    select * from {{ ref('int_station_status_history') }}

    {% if is_incremental() %}
        where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}

),

final as (

    select
        station_id,
        station_name,
        region_id,
        region_name,
        total_docks,
        latitude,
        longitude,

        num_bikes_available,
        num_ebikes_available,
        num_bikes_disabled,
        num_docks_available,
        num_docks_disabled,

        is_installed,
        is_renting,
        is_returning,

        bike_availability_rate,
        dock_availability_rate,

        last_reported_at,
        ingested_at

    from history

)

select * from final
