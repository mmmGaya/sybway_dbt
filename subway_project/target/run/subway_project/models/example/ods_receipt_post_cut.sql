
  
    

  create  table "postgres"."dbt_schema"."ods_receipt_post_cut__dbt_tmp"
  
  
    as
  
  (
    

select * from ods_receipt_post where execution_date = '1960-01-01 00:00:00'
  );
  