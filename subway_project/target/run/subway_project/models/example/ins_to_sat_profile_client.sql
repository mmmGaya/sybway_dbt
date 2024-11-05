
  create view "postgres"."dbt_schema"."ins_to_sat_profile_client__dbt_tmp"
    
    
  as (
    select dataflow_id, dataflow_dttm,
       source_system_dk, client_rk, valid_from_dttm, hashdiff_key,
       actual_flg, delete_flg,
       fio_desc, phone_num_desc, birthday_dttm
from (
    select dataflow_id, dataflow_dttm,
        source_system_dk, client_rk, valid_from_dttm, hashdiff_key,
        actual_flg, delete_flg,
        fio fio_desc, birthday birthday_dttm, phone_num phone_num_desc
    from "postgres"."dbt_schema"."ins_new_or_modif_sat_profile_client"
    union all
    select dataflow_id, dataflow_dttm,
        source_system_dk, client_rk, valid_from_dttm, hashdiff_key,
        actual_flg, delete_flg,
        fio_desc, birthday_dttm, phone_num_desc
    from "postgres"."dbt_schema"."ins_del_sat_profile_client"
    )

--depends on  "postgres"."dbt_schema"."ins_new_or_modif_sat_profile_client"
--depends on "postgres"."dbt_schema"."ins_del_sat_profile_client"
  );