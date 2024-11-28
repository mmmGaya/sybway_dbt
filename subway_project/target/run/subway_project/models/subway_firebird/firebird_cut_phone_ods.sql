
  create view "postgres"."dbt_schema"."firebird_cut_phone_ods__dbt_tmp"
    
    
  as (
    select * from dbt_schema.ods_firebird_phone where dttm = '2024-11-27 11:50:25.649364+00:00'
  );