
{% macro ins_hub_macros(hub_table) %}

SELECT 
    '{{ var('run_id') }}' dataflow_id,
    '{{ var('execution_date') }}'::timestamp dataflow_dttm,
    oid source_system_dk,
    md5(id || '#' || oid) client_rk,
    id || '#' || oid hub_key
FROM 
    {{ var('name_source_tab') }}  ods
	LEFT JOIN 
	{{ hub_table }} h_cl
	ON md5(ods.id || '#' || ods.oid) = h_cl.client_rk
WHERE h_cl.client_rk IS NULL

{% endmacro %}