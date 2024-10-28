select dataflow_id, dataflow_dttm,
       source_system_dk, client_rk, valid_from_dttm, hashdiff_key,
       actual_flg, delete_flg,
       client_name_desc, client_phone_desc, client_city_desc, client_city_dt, client_age_cnt
from (
    select dataflow_id, dataflow_dttm,
        source_system_dk, client_rk, valid_from_dttm, hashdiff_key,
        actual_flg, delete_flg,
        client_name_desc, client_phone_desc, client_city_desc, client_city_dt, client_age_cnt
    from {{ ref('ins_new_or_modif_sat') }}
    union all
    select dataflow_id, dataflow_dttm,
        source_system_dk, client_rk, valid_from_dttm, hashdiff_key,
        actual_flg, delete_flg,
        client_name_desc, client_phone_desc, client_city_desc, client_city_dt, client_age_cnt
    from {{ ref('ins_del_sat') }}
    )

--depends on  {{ ref('ins_new_or_modif_sat') }}
--depends on {{ ref('ins_del_sat_macros') }}