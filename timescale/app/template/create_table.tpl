CREATE TABLE IF NOT EXISTS {{ table_name }} (
{%- for col, data_type in columns_list %}
    "{{ col }}" {{ data_type }} {% if not loop.last %},{% endif %}
{%- endfor %}
{% if primary_list -%}
    , PRIMARY KEY ({{ primary_list| join(', ') }})
{% endif -%}

);
