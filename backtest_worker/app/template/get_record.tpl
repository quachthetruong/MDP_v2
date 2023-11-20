SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY {{ symbol_column }} ORDER BY {{ timestamp_column }} DESC) AS n
    FROM {{ table_name }} where {{ symbol_column }} in ('{{ target_symbols | join('\',\'') }}')
    AND {{ timestamp_column }} = '{{ indexed_timestamp }}'
    {% if filter_query is not none -%}
    {{ filter_query }}
    {%- endif -%}
) AS x
WHERE n <= {{limit}};