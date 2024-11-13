{% macro ins_sal_macros(sat_table, dim_name, key_join, args=()) %}

select 
	'{{ var('run_id') }}' dataflow_id,
    '{{ var('execution_date') }}'::timestamp dataflow_dttm,
    {{ key_join }},
    x_{{ key_join }}
from
	(
	select 
		distinct s1.{{ key_join }} x_{{ key_join }},
		first_value(s2.{{ key_join }}) over(partition by s1.{{ key_join }} order by s2.valid_from_dttm, s2.{{ key_join }}) {{ key_join }}
	from 
		{{sat_table}} s1
		join 
		{{sat_table}} s2
		on {% for i in args %} 
                s1.{{ i }} = s2.{{ i }} 
                {% if not loop.last %} and {% endif %} 
            {% endfor %}
	where s1.{{ key_join }} in 
		(
		select {{ key_join }} from dbt_schema."GPR_RV_H_{{dim_name}}"
		except 
		select x_{{ key_join }} from dbt_schema."GPR_BV_A_{{dim_name}}"
		)
	) 

{% endmacro %}