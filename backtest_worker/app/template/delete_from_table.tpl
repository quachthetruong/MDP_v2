DELETE from {{ table_name }}
WHERE
{% if filter_query is not none -%}
{{ filter_query }}
{%- endif -%}