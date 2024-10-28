{% macro select_all_columns_macro(table_name) %}

select 
    {% if table_name == 'dbt_schema."GPR_RV_E_CLIENT"' %}
        '{{ var('run_id') }}' dataflow_id,
        '{{ var('execution_date') }}'::timestamp dataflow_dttm,
        hashdiff_key,
        client_rk,
        1 delete_flg,
        1 actual_flg, 
        source_system_dk,
        '{{ var('execution_date') }}'::timestamp valid_from_dttm
    {% else %}
        '{{ var('run_id') }}' dataflow_id,
        '{{ var('execution_date') }}'::timestamp dataflow_dttm,
        source_system_dk, 
        client_rk, 
        '{{ var('execution_date') }}'::timestamp valid_from_dttm, 
        hashdiff_key,
        1 actual_flg,
        1 delete_flg,
        client_name_desc,
        client_phone_desc,
        client_city_desc,
        client_city_dt,
        client_age_cnt
    {% endif %}
from 
    {{ table_name }}
where client_rk in
(
select 
	client_rk
from
	(
	select 
		client_rk
	from 
		{{ table_name }}
	 where delete_flg = 0 and actual_flg = 1
    except
    select 
		md5(id || '#' || oid) client_rk
	from 
		 {{ ref('ods_client_cut') }} )
		)
	and actual_flg = 1

{% endmacro %}