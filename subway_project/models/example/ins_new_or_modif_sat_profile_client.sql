{{ select_modif_sals('"dbt_schema"."GPR_RV_S_PROFILE_CLIENT_POST"', 'ods_profile_post_cut', ("id", ), "client_rk", ("fio", "birthday", "phone_num")) }}

--depends on {{ ref('ods_profile_post_cut') }}