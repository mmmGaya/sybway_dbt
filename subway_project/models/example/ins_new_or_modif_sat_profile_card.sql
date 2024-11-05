{{ select_modif_sals('"dbt_schema"."GPR_RV_S_PROFILE_CARD_POST"', 'ods_profile_post_cut', ("id", ), "client_rk", ("card_num", "service_name", "discount")) }}

--depends on {{ ref('ods_profile_post_cut') }}