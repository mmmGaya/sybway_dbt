
  create view "postgres"."dbt_schema"."firebird_cut_client_ods__dbt_tmp"
    
    
  as (
    select * from dbt_schema.ods_firebird_client where dttm = '2024-11-18 13:54:03.436282+00:00'
  );