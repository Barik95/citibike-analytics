-- Fails if any station health score falls outside the valid range of 0 to 100.
-- Score is built from four 25-point components so it should always be 0–100.

select
    station_id,
    station_name,
    health_score

from {{ ref('int_station_health_scores') }}

where health_score < 0 or health_score > 100
