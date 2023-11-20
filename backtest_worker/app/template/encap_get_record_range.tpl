SELECT * FROM {{ table_name }}
WHERE
    time::date >= '{{ included_min_timestamp }}'
    AND time::date <= '{{ included_max_timestamp }}'
    {% if target_symbols is not none -%}
    AND {{ symbol_column }} in ('{{ target_symbols | join('\',\'') }}')
    {%- endif -%}
    {% if filter_query is not none -%}
    {{ filter_query }}
    {%- endif -%}
;
