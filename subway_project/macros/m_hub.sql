
{% macro ins_hub_macros(hub_table, args) %}

SELECT 
    '{{ var('run_id') }}' dataflow_id,
    '{{ var('execution_date') }}'::timestamp dataflow_dttm,
    oid source_system_dk,
    md5( {% for i in args %} {{ i }} || '#' || {% endfor %}  oid) client_rk,
    {% for i in args %} {{ i }} || '#' || {% endfor %}  oid hub_key
 
FROM 
    {{ var('name_source_tab') }}  ods
	LEFT JOIN 
	{{ hub_table }} h_cl
	ON md5( {% for i in args %} ods.{{ i }} || '#' || {% endfor %}  ods.oid) = h_cl.client_rk
WHERE h_cl.client_rk IS NULL

{% endmacro %}