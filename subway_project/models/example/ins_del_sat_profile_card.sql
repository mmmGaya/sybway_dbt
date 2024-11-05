{{ select_all_columns_macro('dbt_schema."GPR_RV_S_PROFILE_CARD_POST"', 'ods_profile_post_cut', ("id", ), "card_rk" ,
("card_num_cnt", "card_service_name_desc", "discount_procent_cnt")) }}

--depends on {{ ref('ods_profile_post_cut') }}