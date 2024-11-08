
  create view "postgres"."dbt_schema"."ins_new_or_modif_e_sat_profile_card__dbt_tmp"
    
    
  as (
    

select 
    
        'manual__2024-11-05T13:39:02.919510+00:00' dataflow_id,
        '2024-11-05 13:39:02.919510+00:00'::timestamp dataflow_dttm,
        md5(  card_num || '#' ||  service_name || '#' ||  discount ) hashdiff_key,
        md5(  id || '#' ||   oid) card_rk,
        0 delete_flg,
        1 actual_flg, 
        oid source_system_dk,
        '2024-11-05 13:39:02.919510+00:00'::timestamp valid_from_dttm
    
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
		 "dbt_schema"."GPR_RV_E_CARD" where actual_flg = 1 and delete_flg = 0)
		)



--depends on "postgres"."dbt_schema"."ods_profile_post_cut"
  );