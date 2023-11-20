kind: stream
metadata:
  name: {{stream_name}}
  signal_name: {{ stream.signal_name }}
  same_table_name: {{ stream.same_table_name }}
  timestep:
    - days: {{ stream.timestep.days }}
    - seconds: {{ stream.timestep.seconds }}
  timestamp_field: {{ stream.timestamp_field }}
  version: {{ stream.version }}
  to_create: {{ stream.to_create }}
  symbol_field: {{ stream.symbol_field }}
  stream_fields: {% for stream_field in stream.stream_fields %}
    - name: {{stream_field[0]}}
      type: {{stream_field[1]}}{% endfor %}
  storage_backend: {{stream.storage_backend.getStorageType()}}
spec:
  {% if metadata is not none -%}
  stream_fields: {% for stream_field in stream.stream_fields %}
    - name: {{stream_field[0]}}
      type: {{stream_field[1]}}{% endfor %}
  {%- endif -%}
    