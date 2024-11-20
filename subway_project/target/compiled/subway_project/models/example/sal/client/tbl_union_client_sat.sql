
	
		select valid_from_dttm, actual_flg, delete_flg,  client_rk  ,  fio_desc  name_desc  ,  birthday_dt  birthday_dt    
		from dbt_schema."GPR_RV_M_CLIENT_PROFILE_POST" where actual_flg = 1 and delete_flg = 0
		 union all 
	
		select valid_from_dttm, actual_flg, delete_flg,  client_rk  ,  name_desc  name_desc  ,  birthday_dt  birthday_dt    
		from dbt_schema."GPR_RV_M_CLIENT_SUBWAY_STAR" where actual_flg = 1 and delete_flg = 0
		
	
