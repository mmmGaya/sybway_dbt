
  create view "postgres"."dbt_schema"."ins_to_sal_card__dbt_tmp"
    
    
  as (
    

select 
	'manual__2024-11-20T10:43:48.001325+00:00' dataflow_id,
    '2024-11-20 10:43:48.001325+00:00'::timestamp dataflow_dttm,
    card_rk,
    x_card_rk
from
	(
	select 
		distinct s1.card_rk x_card_rk,
		first_value(s2.card_rk) over(partition by s1.card_rk order by s2.valid_from_dttm, s2.card_rk) card_rk
	from 
		tbl_union_card_sat s1
		join 
		tbl_union_card_sat s2
		on  
                s1.card_num_cnt = s2.card_num_cnt 
                 
            
	where s1.card_rk in 
		(
		select card_rk from dbt_schema."GPR_RV_H_CARD"
		except 
		select x_card_rk from dbt_schema."GPR_BV_A_CARD"
		)
	) 



--depends on "postgres"."dbt_schema"."tbl_union_card_sat"
  );