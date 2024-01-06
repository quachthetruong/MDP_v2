SELECT *
FROM (
    SELECT *, ROW_NUMBER() 
    OVER (
      PARTITION BY {{ symbol_field }} 
      ORDER BY {{ timestamp_field }} DESC
    ) AS n FROM {{ table_name }} 
    where {{ symbol_field }} in 
    ('{{ target_symbols | join('\',\'') }}') 
    AND {{ timestamp_field }} between 
    '{{ included_min_timestamp }}' and 
    '{{ included_max_timestamp }}'
) AS x
WHERE n <= {{limit}};