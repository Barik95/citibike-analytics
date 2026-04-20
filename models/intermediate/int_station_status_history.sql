-- Enriches each status snapshot with station metadata.
-- One row per station per ingestion snapshot.

with status as (

    select * from {{ ref('stg_citibike__station_status') }}

),

stations as (

    select
        station_id,
        station_name,
        region_id,
        region_name,
        total_docks,
        latitude,
        longitude

    from {{ ref('int_stations_enriched') }}

),

joined as (

    select
        ss.station_id,
        st.station_name,
        st.region_id,
        st.region_name,
        st.total_docks,
        st.latitude,
        st.longitude,

        ss.num_bikes_available,
        ss.num_ebikes_available,
        ss.num_bikes_disabled,
        ss.num_docks_available,
        ss.num_docks_disabled,
        ss.is_installed,
        ss.is_renting,
        ss.is_returning,
        ss.last_reported_at,
        ss.ingested_at,

        -- derived availability rate (guard against zero-capacity stations)
        case
            when st.total_docks = 0 then null
            else round(
                least(1.0, safe_divide(ss.num_bikes_available, st.total_docks)),
                4
            )
        end as bike_availability_rate,

        case
            when st.total_docks = 0 then null
            else round(
                least(1.0, safe_divide(ss.num_docks_available, st.total_docks)),
                4
            )
        end as dock_availability_rate

    from status ss
    left join stations st using (station_id)

)

select * from joined
