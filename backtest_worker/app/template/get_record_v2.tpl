SELECT * FROM {{ table_name }}
WHERE
    {{ timestamp_column }} = '{{ indexed_timestamp }}'
    {% if target_symbols is not none -%}
    AND {{ symbol_column }} in ('{{ target_symbols | join('\',\'') }}')
    {%- endif -%}

    {% if filter_query is not none -%}
    {{ filter_query }}
    {%- endif -%}
;
