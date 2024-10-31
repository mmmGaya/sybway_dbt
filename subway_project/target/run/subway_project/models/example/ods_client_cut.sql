
  
    

  create  table "postgres"."dbt_schema"."ods_client_cut__dbt_tmp"
  
  
    as
  
  (
    

select * from ods_client_csv where execution_date = '2024-10-31 13:36:46.534879+00:00'
  );
  