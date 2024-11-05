
{{ select_modif_sals('"dbt_schema"."GPR_RV_E_CLIENT"', 'ods_client_cut', ("id", ), "client_rk", ("name", "phone", "city", "birthday", "age") ) }}

--depends on {{ref('ods_client_cut')}}