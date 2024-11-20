

with pit_new as (
    select 'manual__2024-11-20T10:43:48.001325+00:00' dataflow_id, '2024-11-20 10:43:48.001325+00:00'::timestamp dataflow_dttm, t1.client_rk, 
    '2024-11-20 10:43:48.001325+00:00'::timestamp valid_from_dttm,
    '5999-01-01 00:00:00'::timestamp valid_to_dttm,
    
        coalesce(max(t2.valid_from_dttm), '1960-01-01 00:00:00'::timestamp) client_subway_star_vf_dttm 
     ,  
        coalesce(max(t3.valid_from_dttm), '1960-01-01 00:00:00'::timestamp) profile_client_post_vf_dttm 
     
    from dbt_schema."GPR_BV_A_CLIENT" t1 
    left join 
        
        (
            select client_rk, valid_from_dttm, source_system_dk 
            from "dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR"
            where 
             row_num = 1 and 
                actual_flg = 1 
                and delete_flg = 0 
                and valid_from_dttm  = 
                (select max(valid_from_dttm) from "dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR") 
        ) t2 on t1.x_client_rk = t2.client_rk
         left join  
        
        (
            select client_rk, valid_from_dttm, source_system_dk 
            from "dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST"
            where 
             row_num = 1 and 
                actual_flg = 1 
                and delete_flg = 0 
                and valid_from_dttm  = 
                (select max(valid_from_dttm) from "dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST") 
        ) t3 on t1.x_client_rk = t3.client_rk
         
        
    where t1.client_rk not in (select client_rk from dbt_schema."GPR_BV_P_CLIENT")
    group by t1.client_rk
)

select * from pit_new
union all
select 'manual__2024-11-20T10:43:48.001325+00:00' dataflow_id, '2024-11-20 10:43:48.001325+00:00'::timestamp dataflow_dttm, client_rk, 
       '1960-01-01 00:00:00'::timestamp valid_from_dttm, valid_from_dttm - interval '1 minute' valid_to_dttm, 
        
            '1960-01-01 00:00:00'::timestamp client_subway_star_vf_dttm 
         ,  
            '1960-01-01 00:00:00'::timestamp profile_client_post_vf_dttm 
         
from pit_new

