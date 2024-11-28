
  create view "postgres"."dbt_schema"."firebird_cut_client_ods__dbt_tmp"
    
    
  as (
    select * from dbt_schema.ods_firebird_client where dttm = '2024-11-27 11:50:25.649364+00:00'
  );