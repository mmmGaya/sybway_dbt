

{% macro select_modif_m_sat(table_name, source_table, pks_source_table, entity_key, args_source_tab=( ), logical_keys, type_key_field) %}

select 
	'{{ var('run_id') }}' dataflow_id,
    '{{ var('execution_date') }}'::timestamp dataflow_dttm,
    oid source_system_dk, 
    md5( {% for i in pks_source_table %} {{ i }} || '#' || {% endfor %}  oid) {{entity_key}}, 
    {{type_key_field}},
    '{{ var('execution_date') }}'::timestamp valid_from_dttm, 
    md5( string_agg( {% for i in args_source_tab %} {{ i }} || '#' || {% endfor %} {{type_key_field}}, '#' ) 
    over(
    partition by 
    {% for i in logical_keys %} {{ i }} {% if not loop.last %}, {% endif %}{% endfor %}
    order by 
    {% for i in args_source_tab %} {{ i }} {% if not loop.last %}, {% endif %}{% endfor %} 
    rows between unbounded preceding and unbounded following)) hashdiff_key,
    1 actual_flg,
    0 delete_flg,
    {% for i in args_source_tab %}
        {{ i }}
        {% if not loop.last %}, {% endif %}
    {% endfor %}
from 
	(select *,
	row_number () 
    over(partition by 
    	{% for i in logical_keys %} {{ i }} {% if not loop.last %} , {% endif %}{% endfor %} 
    	order by 
    	{% for i in args_source_tab %} {{ i }}{% if not loop.last %} , {% endif %}{% endfor %}
    	) {{type_key_field}}
    from 
	{{ref( source_table )}})
where md5( {% for i in pks_source_table %} {{ i }} || '#' || {% endfor %}  oid) in
(
select 
	{{ entity_key }}
from
	(
	select 
		md5( {% for i in pks_source_table %} {{ i }} || '#' || {% endfor %}  oid) {{entity_key}}, 
		md5( string_agg( {% for i in args_source_tab %} {{ i }} || '#' || {% endfor %} {{type_key_field}}, '#' ) 
	    over(
	    partition by 
	    {% for i in logical_keys %} {{ i }} {% if not loop.last %}, {% endif %}{% endfor %}
	    order by 
	    {% for i in args_source_tab %} {{ i }} {% if not loop.last %}, {% endif %}{% endfor %} 
	    rows between unbounded preceding and unbounded following)) hashdiff_key 
	from 
	    (select *,
		row_number () 
	    over(partition by 
	    	{% for i in logical_keys %} {{ i }} {% if not loop.last %} , {% endif %}{% endfor %} 
	    	order by 
	    	{% for i in args_source_tab %} {{ i }}{% if not loop.last %} , {% endif %}{% endfor %}
	    	) {{type_key_field}} 
	    from 
		{{ref( source_table )}})
	except
	select 
		{{ entity_key }}, 
		hashdiff_key   
	from 
		 {{ table_name }} where actual_flg = 1 and delete_flg = 0)
		)

{% endmacro %}


select * from ods_client_cut

select * from dbt_schema."GPR_RV_M_CLIENT" grmc 

select md5(id || '#' || oid) client_rk,
md5( string_agg( phone || '#' || r_num, '' ) over(partition by name, birthday order by phone rows between unbounded preceding and unbounded following)) hashdiff_key
from 
	(select *, row_number() over(partition by name, birthday order by phone) r_num from ods_client_cut) 
except
select client_rk, hashdiff_key from dbt_schema."GPR_RV_M_CLIENT" grmc 