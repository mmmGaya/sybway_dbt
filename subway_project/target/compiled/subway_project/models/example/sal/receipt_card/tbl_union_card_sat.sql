
	
		select valid_from_dttm, actual_flg, delete_flg,  card_rk  ,  card_num_cnt  card_num_cnt    
		from dbt_schema."GPR_RV_S_PROFILE_CARD_POST" where actual_flg = 1 and delete_flg = 0
		
	
