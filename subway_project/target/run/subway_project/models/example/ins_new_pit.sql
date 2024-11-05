
  create view "postgres"."dbt_schema"."ins_new_pit__dbt_tmp"
    
    
  as (
    select dataflow_id, dataflow_dttm, 
       client_rk, valid_from_dttm, valid_to_dttm,
       client_subway_star_vf_dttm
from
    (select 'manual__2024-11-05T10:09:21.817848+00:00' dataflow_id, '2024-11-05 10:09:21.817848+00:00'::timestamp dataflow_dttm, 
        client_rk, 
        to_timestamp( '1960-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss') valid_from_dttm,
        mx_dt - interval '1 minute' valid_to_dttm,
        to_timestamp( '1960-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss') client_subway_star_vf_dttm
    from (select ac.client_rk client_rk, max(sc.valid_from_dttm) mx_dt
    from dbt_schema."GPR_BV_A_CLIENT" ac join dbt_schema."GPR_RV_S_CLIENT" sc on ac.x_client_rk = sc.client_rk
    group by ac.client_rk)
    where client_rk not in (select client_rk from dbt_schema."GPR_BV_P_CLIENT")
    union
    select 'manual__2024-11-05T10:09:21.817848+00:00' dataflow_id, '2024-11-05 10:09:21.817848+00:00'::timestamp dataflow_dttm,
        client_rk, 
        mx_dt valid_from_dttm,
        to_timestamp( '5999-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss') valid_to_dttm,
        mx_dt client_subway_star_vf_dttm
    from (select ac.client_rk client_rk, max(sc.valid_from_dttm) mx_dt
    from dbt_schema."GPR_BV_A_CLIENT" ac join dbt_schema."GPR_RV_S_CLIENT" sc on ac.x_client_rk = sc.client_rk
    group by ac.client_rk)
    where client_rk not in (select client_rk from dbt_schema."GPR_BV_P_CLIENT")
    )

--depends on "postgres"."dbt_schema"."ins_modif_pit"
  );