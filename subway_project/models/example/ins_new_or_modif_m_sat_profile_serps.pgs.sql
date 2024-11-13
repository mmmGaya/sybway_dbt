{{ select_modif_m_sat('"dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST"', 'ods_profile_post_cut', ("id", ), "client_rk", ("birthday", "fio"), "row_num", "phone_num", "phone_desc", ("fio", "phone_num", "birthday"))}}

--depends on {{ref('ods_profile_post_cut')}}