{% macro experiment_significance(
    pre_mean,
    post_mean,
    pre_count,
    post_count,
    pooled_std=0.25
) %}

    -- Approximate two-proportion z-test for pre/post intervention analysis.
    -- Returns the z-score: > 1.96 = statistically significant at 95% confidence.
    -- pooled_std defaults to 0.25 (conservative estimate for availability rates 0–1).
    -- This is an approximation — use with caution on small sample sizes.

    case
        when {{ pre_count }} = 0 or {{ post_count }} = 0 then null
        else round(
            safe_divide(
                abs({{ post_mean }} - {{ pre_mean }}),
                {{ pooled_std }} * sqrt(
                    safe_divide(1, {{ pre_count }})
                    + safe_divide(1, {{ post_count }})
                )
            ),
            4
        )
    end

{% endmacro %}
