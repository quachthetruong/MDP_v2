SELECT  {% if columns|length>0 %}
          {% for column in columns %}
              {{column}} {% if not loop.last %},{% endif -%}
          {% endfor %}
        {% else %}*{% endif -%} 
FROM {{ table_name }} {% if conditions|length>0 %}
WHERE {% for col,value in conditions %} {{col}}='{{value}}' {% if not loop.last %} AND{% endif -%} {% endfor %}{% endif-%};
