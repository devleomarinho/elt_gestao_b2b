{% macro clean_monetary_values(column_name) %}
    SAFE_CAST(
        REPLACE(
            REPLACE(
                REGEXP_REPLACE({{ column_name }}, r'[^\d,.-]', ''), 
            '.', ''), 
        ',', '.')     
    AS NUMERIC)
{% endmacro %}