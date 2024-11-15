select dataflow_id, dataflow_dttm,
       source_system_dk, client_rk, row_num, valid_from_dttm, hashdiff_key,
       actual_flg, delete_flg,
       fio fio_desc, phone_num phone_desc, birthday birthday_dt
    from (
    {{ ref('ins_modif_to_m_sat_client_profile_card_post_pg') }}
    union all
    {{ select_modif_m_sat(
    '"dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST"',
    'ods_cut_client_profile_card_post_pg',
    ("id", ),
    "client_rk",
    ("birthday", "fio"),
    "row_num",
    ("fio", "phone_num", "birthday"),
    ("fio_desc", "phone_desc", "birthday_dt", "valid_from_dttm")
) }}
    )

--depends on {{ ref('ods_cut_client_profile_card_post_pg') }}
--depends on {{ ref('ins_modif_to_m_sat_client_profile_card_post_pg') }}