

{% macro select_modif_sals(table_name) %}

select 
    {% if table_name == '"dbt_schema"."GPR_RV_E_CLIENT"' %}
        '{{ var('run_id') }}' dataflow_id,
        '{{ var('execution_date') }}'::timestamp dataflow_dttm,
        md5(name || '#' || phone || '#' || city || '#' || birthday || '#' || age) hashdiff_key,
        md5(id || '#' || oid) client_rk,
        0 delete_flg,
        1 actual_flg, 
        oid source_system_dk,
        '{{ var('execution_date') }}'::timestamp valid_from_dttm
    {% else %}
        '{{ var('run_id') }}' dataflow_id,
        '{{ var('execution_date') }}'::timestamp dataflow_dttm,
        oid source_system_dk, 
        md5(id|| '#' || oid) client_rk, 
        '{{ var('execution_date') }}'::timestamp valid_from_dttm, 
        md5(name || '#' || phone || '#' || city || '#' || birthday || '#' || age) hashdiff_key,
        1 actual_flg,
        0 delete_flg,
        name client_name_desc,
        phone client_phone_desc,
        city client_city_desc,
        birthday client_city_dt,
        age client_age_cnt
    {% endif %}
from 
	{{ref('ods_client_cut')}}
where md5(id || '#' || oid) in
(
select 
	client_rk
from
	(
	select 
		md5(id || '#' || oid) client_rk, 
		md5(name || '#' || phone || '#' || city || '#' || birthday || '#' || age) hashdiff_key 
	from 
	{{ref('ods_client_cut')}}  
	except
	select 
		client_rk, 
		hashdiff_key   
	from 
		 {{ table_name }} where actual_flg = 1 and delete_flg = 0)
		)

{% endmacro %}