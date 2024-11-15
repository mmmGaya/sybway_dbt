
  create view "postgres"."dbt_schema"."ins_to_m_sat_client_client_from_star_orcl__dbt_tmp"
    
    
  as (
    select dataflow_id, dataflow_dttm,
       source_system_dk, client_rk, row_num, valid_from_dttm, hashdiff_key,
       actual_flg, delete_flg,
       name name_desc, phone phone_desc, city city_desc, birthday birthday_dt, age age_cnt
    from (
    

select  
    
        'manual__2024-11-15T08:02:07.257110+00:00' dataflow_id,
        '2024-11-15 08:02:07.257110+00:00'::timestamp dataflow_dttm,
        source_system_dk, 
        client_rk,
	row_num, 
        '2024-11-15 08:02:07.257110+00:00'::timestamp valid_from_dttm, 
        hashdiff_key,
        1 actual_flg,
        1 delete_flg,
        
            name_desc
            , 
        
            phone_desc
            , 
        
            city_desc
            , 
        
            birthday_dt
            , 
        
            age_cnt
            
        
    
from 
    "dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR"
where client_rk in
(
select 
	client_rk
from
	(
	select 
		client_rk
	from 
		"dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR"
	 where delete_flg = 0 and actual_flg = 1
    except
    select 
		md5(  id || '#' ||   oid) entity_rk 
	from 
		 "postgres"."dbt_schema"."ods_cut_client_from_star_orcl" )
		)
	and actual_flg = 1


    union all
    "postgres"."dbt_schema"."ins_modif_to_m_sat_client_client_from_star_orcl"
    )

--depends on "postgres"."dbt_schema"."ods_cut_client_from_star_orcl"
--depends on "postgres"."dbt_schema"."ins_modif_to_m_sat_client_client_from_star_orcl"
  );