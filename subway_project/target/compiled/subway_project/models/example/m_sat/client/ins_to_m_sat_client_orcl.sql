select dataflow_id, dataflow_dttm,
       source_system_dk, client_rk, row_num, valid_from_dttm, hashdiff_key,
       actual_flg, delete_flg,
       name name_desc, phone phone_desc, city city_desc, birthday birthday_dt, age age_cnt
    from (
    select dataflow_id, dataflow_dttm,
        source_system_dk, client_rk, row_num, valid_from_dttm, hashdiff_key,
        actual_flg, delete_flg,
        name, phone, city, birthday, age
    from "postgres"."dbt_schema"."ins_new_or_modif_m_sat_client_orcl"
    union all
    select dataflow_id, dataflow_dttm,
        source_system_dk, client_rk, row_num, valid_from_dttm, hashdiff_key,
        actual_flg, delete_flg,
        name_desc, phone_desc, city_desc, birthday_dt, age_cnt
    from "postgres"."dbt_schema"."ins_del_m_sat_client_orcl"
    )

--depends on  "postgres"."dbt_schema"."ins_new_or_modif_m_sat_client_orcl"
--depends on "postgres"."dbt_schema"."ins_del_m_sat_client_orcl"