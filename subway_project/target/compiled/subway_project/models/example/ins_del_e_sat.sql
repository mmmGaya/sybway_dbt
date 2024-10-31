select 
	'manual__2024-10-30T08:37:14.345265+00:00' dataflow_id,
    '2024-10-30 08:37:14.345265+00:00'::timestamp dataflow_dttm,
	hashdiff_key,
	client_rk,
	1 delete_flg,
	1 actual_flg, 
	source_system_dk,
	'2024-10-30 08:37:14.345265+00:00'::timestamp valid_from_dttm
from 
	dbt_schema."GPR_RV_E_CLIENT"
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
		"postgres"."dbt_schema"."ods_client_cut"  )
		)
	and actual_flg = 1

    --depends on "postgres"."dbt_schema"."ods_client_cut"
    --depends on "postgres"."dbt_schema"."ins_new_or_modif_e_sat"