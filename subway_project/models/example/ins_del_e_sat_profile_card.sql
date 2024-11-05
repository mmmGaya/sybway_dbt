{{ select_all_columns_macro('"dbt_schema"."GPR_RV_E_CARD"', 'ods_profile_post_cut', ("id", ), "card_rk") }}

--depends on {{ ref('ods_profile_post_cut') }}