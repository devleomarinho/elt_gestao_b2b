{% macro clean_dates(column_name) %}
    COALESCE(
        SAFE.PARSE_DATE('%d/%m/%Y', {{ column_name }}),
        SAFE.PARSE_DATE('%Y-%m-%d', {{ column_name }}),
        SAFE.PARSE_DATE('%d-%m-%Y', {{ column_name }}),
        SAFE.PARSE_DATE('%Y/%m/%d', {{ column_name }})
    )
{% endmacro %}