{% macro station_health_score(
    bike_availability_rate,
    dock_availability_rate,
    is_installed,
    is_renting,
    is_returning,
    num_bikes_disabled,
    num_docks_disabled,
    total_docks
) %}

    -- Returns a composite health score 0–100 built from four 25-point components:
    -- 1. Bike availability rate
    -- 2. Dock availability rate
    -- 3. Operational flags (installed, renting, returning)
    -- 4. Low disabled rate (penalty for broken bikes/docks)

    round(
        -- component 1: bike availability (0–25)
        (coalesce({{ bike_availability_rate }}, 0) * 25)

        -- component 2: dock availability (0–25)
        + (coalesce({{ dock_availability_rate }}, 0) * 25)

        -- component 3: operational flags (0–25)
        + (
            (cast({{ is_installed }} as int64)
             + cast({{ is_renting }}  as int64)
             + cast({{ is_returning }} as int64)
            ) / 3.0 * 25
        )

        -- component 4: low disabled penalty (0–25)
        + (
            case
                when {{ total_docks }} = 0 then 25
                else greatest(
                    0,
                    25 - safe_divide(
                        {{ num_bikes_disabled }} + {{ num_docks_disabled }},
                        {{ total_docks }}
                    ) * 25
                )
            end
        ),
        2
    )

{% endmacro %}
