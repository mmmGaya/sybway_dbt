{{ select_all_columns_macro_new('"dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST"', 'ods_profile_post_cut', ("id", ), "client_rk" , "row_num",
("fio_desc", "phone_desc", "birthday_dt")) }}

--depends on {{ ref('ods_profile_post_cut') }}