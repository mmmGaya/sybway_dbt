
  create view "postgres"."dbt_schema"."insert_delete_m_sat_phones_firebird__dbt_tmp"
    
    
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
        
            phone_number_desc
            , 
        
            question1_cnt
            , 
        
            question2_cnt
            , 
        
            question3_cnt
            
        
    
from 
    dbt_schema."GPR_RV_M_CLIENT_PHONES"
where client_rk in
(
select 
	client_rk
from
	(
	select 
		client_rk
	from 
		dbt_schema."GPR_RV_M_CLIENT_PHONES"
	 where delete_flg = 0 and actual_flg = 1
    except
    select 
		md5(  id_client || '#' ||   oid) entity_rk 
	from 
		 "postgres"."dbt_schema"."firebird_cut_phone_ods" )
		)
	and actual_flg = 1


  );