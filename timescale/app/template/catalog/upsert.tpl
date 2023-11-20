INSERT INTO {{table_name}}({% for column in columns %}{{column}}{% if not loop.last %},{% endif -%}{% endfor %})
VALUES {% for row in df_record.itertuples(index=False) %}({% for col in row %}'{{col}}'{% if not loop.last %},{% endif -%}{% endfor %}){% if not loop.last %},{% endif -%}{% endfor %}
ON CONFLICT ({% for conflict in conflicts %}{{conflict}}{% if not loop.last %},{% endif -%}{% endfor %}) 
DO UPDATE SET {% for column in columns %}{{column}}=EXCLUDED.{{column}}{% if not loop.last %},{% endif -%}{% endfor %}
{% if returnings|length>0 %}returning {% for returning in returnings %}{{returning}}{% endfor %}{% endif -%} ;

