
  create view "postgres"."dbt_schema"."ins_new_or_modif_e_sat__dbt_tmp"
    
    
  as (
    

select 
    
        'manual__2024-10-31T13:36:46.534879+00:00' dataflow_id,
        '2024-10-31 13:36:46.534879+00:00'::timestamp dataflow_dttm,
        md5(name || '#' || phone || '#' || city || '#' || birthday || '#' || age) hashdiff_key,
        md5(id || '#' || oid) client_rk,
        0 delete_flg,
        1 actual_flg, 
        oid source_system_dk,
        '2024-10-31 13:36:46.534879+00:00'::timestamp valid_from_dttm
    
from 
	"postgres"."dbt_schema"."ods_client_cut"
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
	"postgres"."dbt_schema"."ods_client_cut"  
	except
	select 
		client_rk, 
		hashdiff_key   
	from 
		 "dbt_schema"."GPR_RV_E_CLIENT" where actual_flg = 1 and delete_flg = 0)
		)





--depends on "postgres"."dbt_schema"."ods_client_cut"
  );