
select_all_columns_macro('dbt_schema."GPR_RV_S_CLIENT"') 

--depends on {{ ref('ods_client_cut') }}
--depends on {{ ref('ins_new_or_modif_sat') }}