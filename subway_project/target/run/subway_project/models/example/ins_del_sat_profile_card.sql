
  create view "postgres"."dbt_schema"."ins_del_sat_profile_card__dbt_tmp"
    
    
  as (
    


select 
    
        'manual__2024-11-05T13:39:02.919510+00:00' dataflow_id,
        '2024-11-05 13:39:02.919510+00:00'::timestamp dataflow_dttm,
        source_system_dk, 
        card_rk, 
        '2024-11-05 13:39:02.919510+00:00'::timestamp valid_from_dttm, 
        hashdiff_key,
        1 actual_flg,
        1 delete_flg,
        
            card_num_cnt
            , 
        
            card_service_name_desc
            , 
        
            discount_procent_cnt
            
        
    
from 
    dbt_schema."GPR_RV_S_PROFILE_CARD_POST"
where card_rk in
(
select 
	card_rk
from
	(
	select 
		card_rk
	from 
		dbt_schema."GPR_RV_S_PROFILE_CARD_POST"
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