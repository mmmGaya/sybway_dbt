select 
	--ma.run_id  dataflow_id,
    --ma.execution_date dataflow_dttm,
    '{{ var('run_id') }}' dataflow_id,
    '{{ var('execution_date') }}' dataflow_dttm,
    source_system_dk, 
    client_rk, 
    '{{ var('execution_date') }}' valid_from_dttm, 
    hashdiff_key,
    1 actual_flg,
    1 delete_flg,
    client_name_desc,
    client_phone_desc,
    client_city_desc,
    client_city_dt,
    client_age_cnt
from 
	--dbt_schema."GPR_RV_S_CLIENT", (select * from dbt_schema.metadata_airflow_test where source_n = 'csv') ma
    dbt_schema."GPR_RV_S_CLIENT"
where client_rk in
(
select 
	client_rk
from
	(
	select 
		client_rk
	from 
		dbt_schema."GPR_RV_S_CLIENT"
	 where delete_flg = 0 and actual_flg = 1
    except
    select 
		md5(id || '#' || oid) client_rk
	from 
		{{ref('ods_client_cut')}} occ )
		)
	and actual_flg = 1

    --depends on {{ref('ods_client_cut')}}
    --depends on {{ ref('ins_new_or_modif_sat') }}