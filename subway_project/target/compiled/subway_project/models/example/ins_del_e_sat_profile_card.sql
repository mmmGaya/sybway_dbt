


select 
    
        'manual__2024-11-05T13:39:02.919510+00:00' dataflow_id,
        '2024-11-05 13:39:02.919510+00:00'::timestamp dataflow_dttm,
        hashdiff_key,
        card_rk,
        1 delete_flg,
        1 actual_flg, 
        source_system_dk,
        '2024-11-05 13:39:02.919510+00:00'::timestamp valid_from_dttm
    
from 
    "dbt_schema"."GPR_RV_E_CARD"
where card_rk in
(
select 
	card_rk
from
	(
	select 
		card_rk
	from 
		"dbt_schema"."GPR_RV_E_CARD"
	 where delete_flg = 0 and actual_flg = 1
    except
    select 
		md5(  id || '#' ||   oid) entity_rk 
	from 
		 "postgres"."dbt_schema"."ods_profile_post_cut" )
		)
	and actual_flg = 1



--depends on "postgres"."dbt_schema"."ods_profile_post_cut"