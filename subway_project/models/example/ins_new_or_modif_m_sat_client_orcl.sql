
{{ select_modif_m_sat('"dbt_schema"."GPR_RV_M_CLIENT"', 'ods_client_cut', ("id", ), "client_rk", ("phone"), ("birthday", "name", ), "row_num" ) }}

--depends on {{ref('ods_client_cut')}}