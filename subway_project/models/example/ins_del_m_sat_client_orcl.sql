
{{ select_all_columns_macro_new('"dbt_schema"."GPR_RV_M_CLIENT"', 'ods_client_cut', ("id", ), "client_rk" ,
("phone"), "row_num") }}

--depends on {{ ref('ods_client_cut') }}
