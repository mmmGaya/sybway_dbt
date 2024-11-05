
  
    

  create  table "postgres"."dbt_schema"."ods_client_cut__dbt_tmp"
  
  
    as
  
  (
    

select * from ods_client_csv where execution_date = '1960-01-01 00:00:00'
  );
  