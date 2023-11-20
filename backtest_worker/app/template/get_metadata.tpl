
with columns as(
	SELECT table_schema,table_name,{{colums}} FROM information_schema.columns 
	WHERE table_schema = 'public' and table_name NOT LIKE 'pg%'
),primary_key as (
	SELECT c.column_name, tc.table_name
	FROM information_schema.table_constraints tc 
	JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
	JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
	  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
	WHERE constraint_type = 'PRIMARY KEY' 
	and tc.table_schema='{{schema}}' and tc.table_name NOT LIKE 'pg%'
)
select c.*, case when p.table_name is null then false else true end as is_primary_key
from columns c left join primary_key p on (p.table_name=c.table_name) and p.column_name=c.column_name;