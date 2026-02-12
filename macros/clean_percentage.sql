{% macro clean_percentage(column_name) %}
    SAFE_CAST(REPLACE({{ column_name }}, '%', '') AS NUMERIC) / 100
{% endmacro %}