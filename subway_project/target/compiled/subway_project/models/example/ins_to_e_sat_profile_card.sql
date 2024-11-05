select dataflow_id, dataflow_dttm,
       hashdiff_key, card_rk, 
       delete_flg, actual_flg, source_system_dk, valid_from_dttm
from (
    select dataflow_id, dataflow_dttm,
           hashdiff_key, card_rk, 
           delete_flg, actual_flg, source_system_dk, valid_from_dttm
    from "postgres"."dbt_schema"."ins_new_or_modif_e_sat_profile_card"
    union all
    select dataflow_id, dataflow_dttm,
           hashdiff_key, card_rk, 
           delete_flg, actual_flg, source_system_dk, valid_from_dttm
    from "postgres"."dbt_schema"."ins_del_e_sat_profile_card"
    )

--depends on  "postgres"."dbt_schema"."ins_new_or_modif_e_sat_profile_card"
--depends on "postgres"."dbt_schema"."ins_del_e_sat_profile_card"