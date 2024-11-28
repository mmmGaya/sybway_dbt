
  
    

  create  table "postgres"."dbt_schema"."ods_cut_client_from_star_orcl__dbt_tmp"
  
  
    as
  
  (
    

select * from "postgres"."dbt_schema"."ods_client_csv"  where execution_date = '2024-11-28 11:19:25.011076+00:00'

--depends on "postgres"."dbt_schema"."ods_client_csv"
  );
  