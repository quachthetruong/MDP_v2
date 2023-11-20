INSERT INTO prompt_samples (prompt, gpt_result, indexed_timestamp_, metadata) 
VALUES ('{prompt}', '{gpt_result}', '{runtime_date_format}', '{meta_data_json}');

{% if tables|length>0 %}
with first_table as (
        insert into {{ table_name }}({{columns}}) 
        values (%(name)s) 
        returning player_id
    )
    insert into matches (player_id, match, match_result) 
    select player_id, 1, 'won'
    from player_key
{% endif -%} 