select 'manual__2024-11-08T11:17:22.124220+00:00' dataflow_id,
       '2024-11-08 11:17:22.124220+00:00'::timestamp dataflow_dttm, 
       client_rk, 
       mx_dt valid_from_dttm,
       to_timestamp( '5999-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss') valid_to_dttm,
       mx_dt client_subway_star_vf_dttm
from (
select ac.client_rk client_rk, max(sc.valid_from_dttm) mx_dt
from dbt_schema."GPR_BV_A_CLIENT" ac join dbt_schema."GPR_RV_S_CLIENT" sc on ac.x_client_rk = sc.client_rk and sc.delete_flg <> 1 and sc.actual_flg = 1
group by  ac.client_rk) tb
where client_rk in (select client_rk from dbt_schema."GPR_BV_P_CLIENT") 
  and mx_dt > (select max (client_subway_star_vf_dttm) from dbt_schema."GPR_BV_P_CLIENT" where client_rk = tb.client_rk)