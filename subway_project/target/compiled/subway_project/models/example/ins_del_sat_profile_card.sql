


select 
    
        'scheduled__1960-01-01T00:00:00+00:00' dataflow_id,
        '1960-01-01 00:00:00'::timestamp dataflow_dttm,
        source_system_dk, 
        card_rk, 
        '1960-01-01 00:00:00'::timestamp valid_from_dttm, 
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