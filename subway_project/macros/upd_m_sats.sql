
{% macro select_modif_m_sat(table_name, source_table, pks_source_table, entity_key, logical_keys, type_key_field, mass_arg, type_mass_arg, args_source_tab=( )) %}

select 
	'{{ var('run_id') }}' dataflow_id,
    '{{ var('execution_date') }}'::timestamp dataflow_dttm,
    oid source_system_dk, 
    md5( {% for i in pks_source_table %} {{ i }} || '#' || {% endfor %}  oid) {{entity_key}}, 
    {{type_key_field}},
    '{{ var('execution_date') }}'::timestamp valid_from_dttm, 
    md5(string_agg( {{ mass_arg }} || '#' || {{type_key_field}}, '#' ) 
    over(
    partition by 
    {% for i in logical_keys %} {{ i }} {% if not loop.last %}, {% endif %}{% endfor %}
    order by 
    {% for i in args_source_tab %} {{ i }} {% if not loop.last %}, {% endif %}{% endfor %} 
    rows between unbounded preceding and unbounded following) 
    || '#' || {% for i in args_source_tab %} {% if i != mass_arg %} {{i}} {% if not loop.last %} || '#' || {% endif %} {% endif %} {% endfor %}) hashdiff_key,
    1 actual_flg,
    0 delete_flg,
    {% for i in args_source_tab %}
        {{ i }}
        {% if not loop.last %}, {% endif %}
    {% endfor %}
from 
	( select t1.*,
	row_number () 
    over(partition by 
    	{% for i in logical_keys %} {{ i }} {% if not loop.last %} , {% endif %}{% endfor %} 
    	order by 
    	coalesce (t2.dataflow_dttm, current_timestamp), {{ mass_arg }}
    	) {{type_key_field}}
    from 
	{{ref( source_table )}} t1
    left join
    (select {{entity_key}}, dataflow_dttm, {{type_mass_arg}} 
    from {{table_name}} 
    where actual_flg = 1 and delete_flg = 0)
    t2
    on md5( {% for i in pks_source_table %} t1.{{ i }} || '#' || {% endfor %}  t1.oid) = t2.{{entity_key}} and t1.{{mass_arg}} = t2.{{type_mass_arg}})
where md5( {% for i in pks_source_table %} {{ i }} || '#' || {% endfor %}  oid) in
(
select 
	{{ entity_key }}
from
	(
	select 
		md5( {% for i in pks_source_table %} {{ i }} || '#' || {% endfor %}  oid) {{entity_key}}, 
		md5(string_agg( {{ mass_arg }} || '#' || {{type_key_field}}, '#' ) 
	    over(
	    partition by 
	    {% for i in logical_keys %} {{ i }} {% if not loop.last %}, {% endif %}{% endfor %}
	    order by 
	    {% for i in args_source_tab %} {{ i }} {% if not loop.last %}, {% endif %}{% endfor %} 
	    rows between unbounded preceding and unbounded following)
	    || '#' || {% for i in args_source_tab %} {% if i != mass_arg %} {{i}} {% if not loop.last %} || '#' || {% endif %} {% endif %} {% endfor %}) hashdiff_key
	from 
	    ( select t1.*,
	row_number () 
    over(partition by 
    	{% for i in logical_keys %} {{ i }} {% if not loop.last %} , {% endif %}{% endfor %} 
    	order by 
    	coalesce (t2.dataflow_dttm, current_timestamp), {{ mass_arg }}
    	) {{type_key_field}}
    from 
	{{ref( source_table )}} t1
    left join
    (select {{entity_key}}, dataflow_dttm, {{type_mass_arg}} 
    from {{table_name}} 
    where actual_flg = 1 and delete_flg = 0)
    t2
    on md5( {% for i in pks_source_table %} t1.{{ i }} || '#' || {% endfor %}  t1.oid) = t2.{{entity_key}} and t1.{{mass_arg}} = t2.{{type_mass_arg}})
	except
	select 
		{{ entity_key }}, 
		hashdiff_key   
	from 
		 {{ table_name }} where actual_flg = 1 and delete_flg = 0)
		)
{% endmacro %}