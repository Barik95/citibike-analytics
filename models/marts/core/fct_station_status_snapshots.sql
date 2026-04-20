{{
    config(
        materialized='table'
    )
}}
{# Switch to incremental + unique_key + incremental_strategy='merge' once billing is enabled #}

-- Every status snapshot per station — the main fact table.

with history as (

    select * from {{ ref('int_station_status_history') }}

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
