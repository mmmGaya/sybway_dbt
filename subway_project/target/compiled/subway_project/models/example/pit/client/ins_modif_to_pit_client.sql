

select 'scheduled__1960-01-01T00:00:00+00:00' dataflow_id, '1960-01-01 00:00:00'::timestamp dataflow_dttm, t1.client_rk, 
'1960-01-01 00:00:00'::timestamp valid_from_dttm,
'5999-01-01 00:00:00'::timestamp valid_to_dttm,

    coalesce(max(t2.valid_from_dttm), '1960-01-01 00:00:00'::timestamp) client_subway_star_vf_dttm 
 ,  
    coalesce(max(t3.valid_from_dttm), '1960-01-01 00:00:00'::timestamp) profile_client_post_vf_dttm 
 
from dbt_schema."GPR_BV_A_CLIENT" t1 
left join 
    
    (
        select client_rk, valid_from_dttm, source_system_dk 
        from tables_field_m_name[tables][0]
        where 
         row_num = 1 and 
            and actual_flg = 1 
            and delete_flg = 0 
            and valid_from_dttm  = 
            (select max(valid_from_dttm) from tables_field_m_name[tables][0]) 
    ) t2 on t1.x_client_rk = t2.client_rk
     left join  
    
    (
        select client_rk, valid_from_dttm, source_system_dk 
        from tables_field_m_name[tables][0]
        where 
         row_num = 1 and 
            and actual_flg = 1 
            and delete_flg = 0 
            and valid_from_dttm  = 
            (select max(valid_from_dttm) from tables_field_m_name[tables][0]) 
    ) t3 on t1.x_client_rk = t3.client_rk
     
    
where t1.client_rk in (select client_rk from dbt_schema."GPR_BV_P_CLIENT")
group by t1.client_rk


