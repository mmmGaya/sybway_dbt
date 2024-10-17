select 
	ma.run_id  dataflow_id,
	ma.execution_date dataflow_dttm,
	hashdiff_key,
	client_rk,
	1 delete_flg,
	1 actual_flg, 
	source_system_dk,
	ma.execution_date valid_from_dttm
from 
	dbt_schema."GPR_RV_E_CLIENT", (select * from dbt_schema.metadata_airflow_test where source_n = 'csv') ma
where client_rk in
(
select 
	client_rk
from
	(
	select 
		client_rk
	from 
		dbt_schema."GPR_RV_E_CLIENT"
	 where delete_flg = 0 and actual_flg = 1
    except
    select 
		md5(id || '#' || oid) client_rk
	from 
		{{ref('ods_client_cut')}} occ )
		)
	and actual_flg = 1

    --depends on {{ref('ods_client_cut')}}
    --depends on {{ref('ins_new_or_modif_e_sat')}}