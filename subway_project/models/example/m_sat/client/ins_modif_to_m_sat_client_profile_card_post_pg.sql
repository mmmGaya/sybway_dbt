{{ select_all_columns_macro_new('"dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST"', 'ods_cut_client_profile_card_post_pg', ("id", ), "client_rk" , "row_num",
("fio_desc", "phone_desc", "birthday_dt", "valid_from_dttm")) }}


--depends on {{ ref('ods_cut_client_profile_card_post_pg') }}