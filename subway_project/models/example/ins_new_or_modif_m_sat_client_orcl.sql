
{{ select_modif_m_sat('"dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR"', 'ods_client_cut', ("id", ), "client_rk", ("birthday", "name"), "row_num", "phone", "phone_desc", ("name", "phone", "city", "birthday", "age"))}}

--depends on {{ref('ods_client_cut')}}