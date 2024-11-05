select dataflow_id, dataflow_dttm,
       hashdiff_key, card_rk, 
       delete_flg, actual_flg, source_system_dk, valid_from_dttm
from (
    select dataflow_id, dataflow_dttm,
           hashdiff_key, card_rk, 
           delete_flg, actual_flg, source_system_dk, valid_from_dttm
    from {{ ref('ins_new_or_modif_e_sat_profile_card') }}
    union all
    select dataflow_id, dataflow_dttm,
           hashdiff_key, card_rk, 
           delete_flg, actual_flg, source_system_dk, valid_from_dttm
    from {{ ref('ins_del_e_sat_profile_card') }}
    )

--depends on  {{ ref('ins_new_or_modif_e_sat_profile_card') }}
--depends on {{ ref('ins_del_e_sat_profile_card') }}