
  
    

  create  table "postgres"."dbt_schema"."ods_client_cut__dbt_tmp"
  
  
    as
  
  (
    

select * from ods_client_csv where execution_date = '2024-11-02 11:57:26.310307+00:00'
  );
  