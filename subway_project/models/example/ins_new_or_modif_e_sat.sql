select 
	'{{ var('run_id') }}' dataflow_id,
    '{{ var('execution_date') }}'::timestamp dataflow_dttm,
	md5(name || '#' || phone || '#' || city || '#' || birthday || '#' || age) hashdiff_key,
	md5(id || '#' || oid) client_rk,
	0 delete_flg,
	1 actual_flg, 
	oid source_system_dk,
	'{{ var('execution_date') }}'::timestamp valid_from_dttm
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
		dbt_schema."GPR_RV_E_CLIENT" where actual_flg = 1 and delete_flg = 0)
		)

--depends on {{ref('ods_client_cut')}}