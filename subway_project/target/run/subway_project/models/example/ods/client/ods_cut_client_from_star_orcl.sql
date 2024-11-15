
  
    

  create  table "postgres"."dbt_schema"."ods_cut_client_from_star_orcl__dbt_tmp"
  
  
    as
  
  (
    

select * from ods_client_csv where execution_date = '2024-11-15 08:02:07.257110+00:00'
  );
  