select '{{ var('run_id') }}' dataflow_id, '{{ var('execution_date') }}'::timestamp dataflow_dttm,
		client_rk, valid_from_dttm, valid_to_dttm, 
	    client_name_desc, client_phone_desc, client_city_desc, client_birthday_dt, client_age_cnt
from
(select distinct 
	   ac.client_rk, pc.valid_from_dttm, pc.valid_to_dttm, 
	   sc.client_name_desc, sc.client_phone_desc, sc.client_city_desc, sc.client_city_dt client_birthday_dt, sc.client_age_cnt,
	   min(sc.client_rk) over(partition by ac.client_rk, sc.valid_from_dttm) mrk, sc.client_rk srk
from dbt_schema."GPR_BV_P_CLIENT" pc
join dbt_schema."GPR_BV_A_CLIENT" ac on pc.client_rk = ac.client_rk 
join dbt_schema."GPR_RV_S_CLIENT" sc on ac.x_client_rk = sc.client_rk and pc.valid_from_dttm = sc.valid_from_dttm 
where extract(year from pc.valid_from_dttm) > 1960 and sc.delete_flg = 0 and sc.actual_flg = 1)
where mrk = srk and (client_rk, valid_from_dttm) not in (select client_rk, valid_from_dttm from dbt_schema."GPR_EM_DIM_CLIENT")