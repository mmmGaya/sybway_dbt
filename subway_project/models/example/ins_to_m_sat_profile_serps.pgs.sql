select dataflow_id, dataflow_dttm,
       source_system_dk, client_rk, row_num, valid_from_dttm, hashdiff_key,
       actual_flg, delete_flg,
       fio fio_desc, phone_num phone_desc, birthday birthday_dt
    from (
    select dataflow_id, dataflow_dttm,
        source_system_dk, client_rk, row_num, valid_from_dttm, hashdiff_key,
        actual_flg, delete_flg,
        fio, phone_num, birthday
    from {{ ref('ins_new_or_modif_m_sat_profile_serps.pgs') }}
    union all
    select dataflow_id, dataflow_dttm,
        source_system_dk, client_rk, row_num, valid_from_dttm, hashdiff_key,
        actual_flg, delete_flg,
        fio_desc, phone_desc, birthday_dt
    from {{ ref('ins_del_m_sat_profile_serps.pgs') }}
    )

--depends on  {{ ref('ins_new_or_modif_m_sat_profile_serps.pgs') }}
--depends on {{ ref('ins_del_m_sat_profile_serps.pgs') }}