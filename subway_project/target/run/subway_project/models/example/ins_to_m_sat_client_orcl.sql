
  create view "postgres"."dbt_schema"."ins_to_m_sat_client_orcl__dbt_tmp"
    
    
  as (
    select dataflow_id, dataflow_dttm,
       source_system_dk, client_rk, row_num, valid_from_dttm, hashdiff_key,
       actual_flg, delete_flg,
       phone phone_num
    from (
    select dataflow_id, dataflow_dttm,
        source_system_dk, client_rk, row_num, valid_from_dttm, hashdiff_key,
        actual_flg, delete_flg,
        phone
    from "postgres"."dbt_schema"."ins_new_or_modif_m_sat_client_orcl"
    union all
    select dataflow_id, dataflow_dttm,
        source_system_dk, client_rk, row_num, valid_from_dttm, hashdiff_key,
        actual_flg, delete_flg,
        phone
    from "postgres"."dbt_schema"."ins_del_m_sat_client_orcl"
    )

--depends on  "postgres"."dbt_schema"."ins_new_or_modif_m_sat_client_orcl"
--depends on "postgres"."dbt_schema"."ins_del_m_sat_client_orcl"
  );