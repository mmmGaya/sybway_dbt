{% macro select_all_columns_macro_new(
    table_name, 
    source_table, 
    pks_source_table, 
    entity_key, 
    row_name, 
    args=( )
) 
%}

select  
    {% if table_name[21:22] == 'E' %}
        '{{ var('run_id') }}' dataflow_id,
        '{{ var('execution_date') }}'::timestamp dataflow_dttm,
        hashdiff_key,
        {{ entity_key }},
        1 delete_flg,
        1 actual_flg, 
        source_system_dk,
        '{{ var('execution_date') }}'::timestamp valid_from_dttm
    {% elif table_name[21:22] == 'M' %}
        '{{ var('run_id') }}' dataflow_id,
        '{{ var('execution_date') }}'::timestamp dataflow_dttm,
        source_system_dk, 
        {{ entity_key }},
	{{ row_name }}, 
        '{{ var('execution_date') }}'::timestamp valid_from_dttm, 
        hashdiff_key,
        1 actual_flg,
        1 delete_flg,
        {% for i in args %}
            {{ i }}
            {% if not loop.last %}, {% endif %}
        {% endfor %}
    {% else %}
        '{{ var('run_id') }}' dataflow_id,
        '{{ var('execution_date') }}'::timestamp dataflow_dttm,
        source_system_dk, 
        {{ entity_key }},
        '{{ var('execution_date') }}'::timestamp valid_from_dttm, 
        hashdiff_key,
        1 actual_flg,
        1 delete_flg,
        {% for i in args %}
            {{ i }}
            {% if not loop.last %}, {% endif %}
        {% endfor %}
    {% endif %}
from 
    {{ table_name }}
where {{ entity_key }} in
(
select 
	{{ entity_key }}
from
	(
	select 
		{{ entity_key }}
	from 
		{{ table_name }}
	 where delete_flg = 0 and actual_flg = 1
    except
    select 
		md5( {% for i in pks_source_table %} {{ i }} || '#' || {% endfor %}  oid) entity_rk 
	from 
		 {{ ref( source_table ) }} )
		)
	and actual_flg = 1

{% endmacro %}

