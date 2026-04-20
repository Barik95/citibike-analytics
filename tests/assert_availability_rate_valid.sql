-- Fails if any availability rate is outside the valid range of 0.0 to 1.0.
-- Rates above 1 would mean more bikes than docks — a data quality issue.

select
    station_id,
    ingested_at,
    bike_availability_rate,
    dock_availability_rate

from {{ ref('fct_station_status_snapshots') }}

where
    (bike_availability_rate is not null and (bike_availability_rate < 0 or bike_availability_rate > 1))
    or
    (dock_availability_rate is not null and (dock_availability_rate < 0 or dock_availability_rate > 1))
