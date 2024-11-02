
  
    

  create  table "postgres"."dbt_schema"."ods_receipt_post_cut__dbt_tmp"
  
  
    as
  
  (
    

select * from ods_receipt_post where execution_date = '2024-11-02 10:58:34.089053+00:00'
  );
  