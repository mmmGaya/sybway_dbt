{{ select_modif_sals('"dbt_schema"."GPR_RV_E_CARD"', 'ods_profile_post_cut', ("id", ), "card_rk", ("card_num", "service_name", "discount") ) }}

--depends on {{ ref('ods_profile_post_cut') }}