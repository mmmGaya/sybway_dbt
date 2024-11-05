

{{ select_all_columns_macro('"dbt_schema"."GPR_RV_S_CLIENT"', 'ods_client_cut', ("id", ), "client_rk" ,
("client_name_desc", "client_phone_desc", "client_city_desc", "client_city_dt", "client_age_cnt")) }}

--depends on {{ ref('ods_client_cut') }}
