{% macro haversine_distance(lat1, lon1, lat2, lon2) %}

    -- Returns distance in km between two GPS coordinates.
    -- BigQuery has no RADIANS() function, so we convert manually: degrees * acos(-1.0) / 180.
    round(
        2 * 6371 * asin(
            sqrt(
                pow(sin(({{ lat2 }} - {{ lat1 }}) * acos(-1.0) / 180.0 / 2), 2)
                + cos({{ lat1 }} * acos(-1.0) / 180.0)
                * cos({{ lat2 }} * acos(-1.0) / 180.0)
                * pow(sin(({{ lon2 }} - {{ lon1 }}) * acos(-1.0) / 180.0 / 2), 2)
            )
        ),
        4
    )

{% endmacro %}
