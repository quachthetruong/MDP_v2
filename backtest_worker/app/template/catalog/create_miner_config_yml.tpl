kind: miner
metadata:
  name: {{ miner_name }}
  description: 
  version: {{miner_configs.output.version}}
  timestep: {{miner_configs.output.timestep}}
  target_symbols: []
  timestamp: ""
spec:
  input_streams:{% for input_stream_name in input_stream_names %}
    - stream_name: {{ input_stream_name }}{% endfor %}
  output_stream:
    stream_name: {{ output_stream_name }}
