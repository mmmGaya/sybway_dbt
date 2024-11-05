
  create view "postgres"."dbt_schema"."ins_del_sat_profile_client__dbt_tmp"
    
    
  as (
    


select 
    
        'manual__2024-11-05T11:53:56.016169+00:00' dataflow_id,
        '2024-11-05 11:53:56.016169+00:00'::timestamp dataflow_dttm,
        source_system_dk, 
        client_rk, 
        '2024-11-05 11:53:56.016169+00:00'::timestamp valid_from_dttm, 
        hashdiff_key,
        1 actual_flg,
        1 delete_flg,
        
            fio_desc
            , 
        
            birthday_dttm
            , 
        
            phone_num_desc
            
        
    
from 
    dbt_schema."GPR_RV_S_PROFILE_CLIENT_POST"
where client_rk in
(
select 
	client_rk
from
	(
	select 
		client_rk
	from 
		dbt_schema."GPR_RV_S_PROFILE_CLIENT_POST"
	 where delete_flg = 0 and actual_flg = 1
    except
    select 
		md5(  id || '#' ||   oid) entity_rk 
	from 
		 "postgres"."dbt_schema"."ods_profile_post_cut" )
		)
	and actual_flg = 1



--depends on "postgres"."dbt_schema"."ods_profile_post_cut"
  );