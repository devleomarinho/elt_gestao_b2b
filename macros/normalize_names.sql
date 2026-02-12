{% macro normalize_names(column_name) %}
    REGEXP_REPLACE(NORMALIZE({{ column_name }}, NFD), r"\pM", '')
{% endmacro %}