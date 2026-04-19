-- Joins dockless bikes with the nearest station context using haversine distance.
-- One row per bike per snapshot. Empty when no dockless bikes are in field.

with bikes as (

    select * from {{ ref('stg_citibike__bikes') }}

),

stations as (

    select
        station_id,
        station_name,
        region_id,
        region_name,
        latitude,
        longitude

    from {{ ref('int_stations_enriched') }}

),

bikes_with_distances as (

    select
        b.bike_id,
        b.latitude            as bike_latitude,
        b.longitude           as bike_longitude,
        b.is_reserved,
        b.is_disabled,
        b.vehicle_type_id,
        b.ingested_at,

        s.station_id          as nearest_station_id,
        s.station_name        as nearest_station_name,
        s.region_id,
        s.region_name,

        -- haversine distance in km (BigQuery has no radians(), use * pi/180)
        round(
            2 * 6371 * asin(
                sqrt(
                    pow(sin((s.latitude  - b.latitude)  * acos(-1.0) / 180.0 / 2), 2)
                    + cos(b.latitude * acos(-1.0) / 180.0) * cos(s.latitude * acos(-1.0) / 180.0)
                    * pow(sin((s.longitude - b.longitude) * acos(-1.0) / 180.0 / 2), 2)
                )
            ),
            4
        ) as distance_km,

        row_number() over (
            partition by b.bike_id, b.ingested_at
            order by
                pow(sin((s.latitude  - b.latitude)  * acos(-1.0) / 180.0 / 2), 2)
                + cos(b.latitude * acos(-1.0) / 180.0) * cos(s.latitude * acos(-1.0) / 180.0)
                * pow(sin((s.longitude - b.longitude) * acos(-1.0) / 180.0 / 2), 2)
        ) as distance_rank

    from bikes b
    cross join stations s

),

nearest_only as (

    select * from bikes_with_distances
    where distance_rank = 1

)

select
    bike_id,
    bike_latitude,
    bike_longitude,
    is_reserved,
    is_disabled,
    vehicle_type_id,
    nearest_station_id,
    nearest_station_name,
    region_id,
    region_name,
    distance_km,
    ingested_at

from nearest_only
