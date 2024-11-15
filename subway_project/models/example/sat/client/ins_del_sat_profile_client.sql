{{ select_all_columns_macro('dbt_schema."GPR_RV_S_PROFILE_CLIENT_POST"', 'ods_cut_client_profile_card_post_pg', ("id", ), "client_rk" ,
("fio_desc", "birthday_dttm", "phone_num_desc")) }}

--depends on {{ ref('ods_cut_client_profile_card_post_pg') }}