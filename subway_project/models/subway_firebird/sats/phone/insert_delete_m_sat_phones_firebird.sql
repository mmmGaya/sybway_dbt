{{ select_all_columns_macro_new(
    'dbt_schema."GPR_RV_M_CLIENT_PHONES"', 
    'firebird_cut_phone_ods', 
    ('id_client', ),
    'client_rk', 
    ('id_client', ), 
    ('phone_number_desc', 'question1_cnt', 'question2_cnt', 'question3_cnt'), 
) }}