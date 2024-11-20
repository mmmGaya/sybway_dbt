
  create view "postgres"."dbt_schema"."ins_to_e_sat_client_client_from_star_orcl__dbt_tmp"
    
    
  as (
    select dataflow_id, dataflow_dttm,
       hashdiff_key, client_rk, 
       delete_flg, actual_flg, source_system_dk, valid_from_dttm
from (
    

select 
    
        'manual__2024-11-20T10:43:48.001325+00:00' dataflow_id,
        '2024-11-20 10:43:48.001325+00:00'::timestamp dataflow_dttm,
        md5(  name || '#' ||  phone || '#' ||  city || '#' ||  birthday || '#' ||  age ) hashdiff_key,
        md5(  id || '#' ||   oid) client_rk,
        0 delete_flg,
        1 actual_flg, 
        oid source_system_dk,
        '2024-11-20 10:43:48.001325+00:00'::timestamp valid_from_dttm
    
from 
	"postgres"."dbt_schema"."ods_cut_client_from_star_orcl"
where md5(  id || '#' ||   oid) in
(
select 
	client_rk
from
	(
	select 
		md5(  id || '#' ||   oid) client_rk, 
		md5(  name || '#' || phone || '#' || city || '#' || birthday || '#' || age ) hashdiff_key 
	from 
	    "postgres"."dbt_schema"."ods_cut_client_from_star_orcl"
	except
	select 
		client_rk, 
		hashdiff_key   
	from 
		 "dbt_schema"."GPR_RV_E_CLIENT" where actual_flg = 1 and delete_flg = 0)
		)


    union all
    


select 
    
        'manual__2024-11-20T10:43:48.001325+00:00' dataflow_id,
        '2024-11-20 10:43:48.001325+00:00'::timestamp dataflow_dttm,
        hashdiff_key,
        client_rk,
        1 delete_flg,
        1 actual_flg, 
        source_system_dk,
        '2024-11-20 10:43:48.001325+00:00'::timestamp valid_from_dttm
    
from 
    "dbt_schema"."GPR_RV_E_CLIENT"
where client_rk in
(
select 
	client_rk
from
	(
	select 
		client_rk
	from 
		"dbt_schema"."GPR_RV_E_CLIENT"
	 where delete_flg = 0 and actual_flg = 1
    except
    select 
		md5(  id || '#' ||   oid) entity_rk 
	from 
		 "postgres"."dbt_schema"."ods_cut_client_from_star_orcl" )
		)
	and actual_flg = 1


    )

--depends on "postgres"."dbt_schema"."ods_cut_client_from_star_orcl"
  );