
{{ select_all_columns_macro_new('"dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR"', 'ods_client_cut', ("id", ), "client_rk" , "row_num",
("name_desc", "phone_desc", "city_desc", "birthday_dt", "age_cnt",)) }}

--depends on {{ ref('ods_client_cut') }}
