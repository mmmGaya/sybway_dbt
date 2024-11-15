
  create view "postgres"."dbt_schema"."ins_modif_to_m_sat_client_profile_card_post_pg__dbt_tmp"
    
    
  as (
    

select  
    
        'manual__2024-11-15T07:13:08.222605+00:00' dataflow_id,
        '2024-11-15 07:13:08.222605+00:00'::timestamp dataflow_dttm,
        source_system_dk, 
        client_rk,
	row_num, 
        '2024-11-15 07:13:08.222605+00:00'::timestamp valid_from_dttm, 
        hashdiff_key,
        1 actual_flg,
        1 delete_flg,
        
            fio_desc
            , 
        
            phone_desc
            , 
        
            birthday_dt
            , 
        
            valid_from_dttm
            
        
    
from 
    "dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST"
where client_rk in
(
select 
	client_rk
from
	(
	select 
		client_rk
	from 
		"dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST"
	 where delete_flg = 0 and actual_flg = 1
    except
    select 
		md5(  id || '#' ||   oid) entity_rk 
	from 
		 "postgres"."dbt_schema"."ods_cut_client_profile_card_post_pg" )
		)
	and actual_flg = 1




--depends on "postgres"."dbt_schema"."ods_cut_client_profile_card_post_pg"
  );