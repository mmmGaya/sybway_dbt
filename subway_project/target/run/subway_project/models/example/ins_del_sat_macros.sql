
  create view "postgres"."dbt_schema"."ins_del_sat_macros__dbt_tmp"
    
    
  as (
    

select 
    
        'scheduled__1960-01-01T00:00:00+00:00' dataflow_id,
        '1960-01-01 00:00:00'::timestamp dataflow_dttm,
        source_system_dk, 
        client_rk, 
        '1960-01-01 00:00:00'::timestamp valid_from_dttm, 
        hashdiff_key,
        1 actual_flg,
        1 delete_flg,
        client_name_desc,
        client_phone_desc,
        client_city_desc,
        client_city_dt,
        client_age_cnt
    
from 
    "dbt_schema"."GPR_RV_S_CLIENT"
where client_rk in
(
select 
	client_rk
from
	(
	select 
		client_rk
	from 
		"dbt_schema"."GPR_RV_S_CLIENT"
	 where delete_flg = 0 and actual_flg = 1
    except
    select 
		md5(id || '#' || oid) client_rk
	from 
		 "postgres"."dbt_schema"."ods_client_cut" )
		)
	and actual_flg = 1



--depends on "postgres"."dbt_schema"."ods_client_cut"
  );