SELECT * FROM {{ table_name }}
WHERE
    time = '{{ time }}'
    {% if target_symbols is not none -%}
    AND symbol in ('{{ target_symbols | join('\',\'') }}')
    {%- endif -%}

    {% if filter_query is not none -%}
    {{ filter_query }}
    {%- endif -%}
;
