select dataflow_id, dataflow_dttm,
       hashdiff_key, client_rk, 
       delete_flg, actual_flg, source_system_dk, valid_from_dttm
from (
    select dataflow_id, dataflow_dttm,
           hashdiff_key, client_rk, 
           delete_flg, actual_flg, source_system_dk, valid_from_dttm
    from "postgres"."dbt_schema"."ins_new_or_modif_e_sat"
    union all
    select dataflow_id, dataflow_dttm,
           hashdiff_key, client_rk, 
           delete_flg, actual_flg, source_system_dk, valid_from_dttm
    from "postgres"."dbt_schema"."ins_del_e_sat_macros"
    )

--depends on  "postgres"."dbt_schema"."ins_new_or_modif_e_sat"
--depends on "postgres"."dbt_schema"."ins_del_e_sat_macros"