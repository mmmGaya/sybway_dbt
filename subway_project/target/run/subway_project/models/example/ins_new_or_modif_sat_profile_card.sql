
  create view "postgres"."dbt_schema"."ins_new_or_modif_sat_profile_card__dbt_tmp"
    
    
  as (
    

select 
    
        'manual__2024-11-05T11:53:56.016169+00:00' dataflow_id,
        '2024-11-05 11:53:56.016169+00:00'::timestamp dataflow_dttm,
        oid source_system_dk, 
        md5(  id || '#' ||   oid) card_rk, 
        '2024-11-05 11:53:56.016169+00:00'::timestamp valid_from_dttm, 
        md5(  card_num || '#' ||  service_name || '#' ||  discount ) hashdiff_key,
        1 actual_flg,
        0 delete_flg,
        
            card_num
            , 
        
            service_name
            , 
        
            discount
            
        
    
from 
	"postgres"."dbt_schema"."ods_profile_post_cut"
where md5(  id || '#' ||   oid) in
(
select 
	card_rk
from
	(
	select 
		md5(  id || '#' ||   oid) card_rk, 
		md5(  card_num || '#' || service_name || '#' || discount ) hashdiff_key 
	from 
	    "postgres"."dbt_schema"."ods_profile_post_cut"
	except
	select 
		card_rk, 
		hashdiff_key   
	from 
		 "dbt_schema"."GPR_RV_S_PROFILE_CARD_POST" where actual_flg = 1 and delete_flg = 0)
		)



--depends on "postgres"."dbt_schema"."ods_profile_post_cut"
  );