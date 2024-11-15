
  create view "postgres"."dbt_schema"."tbl_union_client_sat__dbt_tmp"
    
    
  as (
    
	
		select  client_rk  ,  fio_desc  name_desc  ,  birthday_dt  birthday_dt    
		from dbt_schema."GPR_RV_M_CLIENT_PROFILE_POST" 
		 union all 
	
		select  client_rk  ,  name_desc  name_desc  ,  birthday_dt  birthday_dt    
		from dbt_schema."GPR_RV_M_CLIENT_SUBWAY_STAR" 
		
	

  );