{% macro availability_bucket(availability_rate_col) %}

    case
        when {{ availability_rate_col }} is null       then 'unknown'
        when {{ availability_rate_col }} = 0           then 'empty'
        when {{ availability_rate_col }} < 0.20        then 'critical'
        when {{ availability_rate_col }} < 0.40        then 'low'
        when {{ availability_rate_col }} < 0.70        then 'moderate'
        when {{ availability_rate_col }} < 0.90        then 'good'
        else                                                'full'
    end

{% endmacro %}
