
  create view "postgres"."dbt_schema"."ins_to_sal__dbt_tmp"
    
    
  as (
    select 
	'manual__2024-11-05T10:09:21.817848+00:00' dataflow_id,
    '2024-11-05 10:09:21.817848+00:00'::timestamp dataflow_dttm,
    client_rk,
    x_client_rk
from
	(
	select 
		distinct s1.client_rk x_client_rk,
		first_value(s2.client_rk) over(partition by s1.client_rk order by s2.valid_from_dttm, s2.client_rk) client_rk
	from 
		dbt_schema."GPR_RV_S_CLIENT" s1
		join 
		dbt_schema."GPR_RV_S_CLIENT" s2
		on s1.client_name_desc = s2.client_name_desc and s1.client_phone_desc = s2.client_phone_desc
	where s1.client_rk in 
		(
		select client_rk from dbt_schema."GPR_RV_H_CLIENT"
		except 
		select x_client_rk from dbt_schema."GPR_BV_A_CLIENT"
		)
	)
  );