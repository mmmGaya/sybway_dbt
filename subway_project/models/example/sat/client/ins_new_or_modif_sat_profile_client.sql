{{ select_modif_sals('"dbt_schema"."GPR_RV_S_PROFILE_CLIENT_POST"', 'ods_cut_client_profile_card_post_pg', ("id", ), "client_rk", ("fio", "birthday", "phone_num")) }}

--depends on {{ ref('ods_cut_client_profile_card_post_pg') }}