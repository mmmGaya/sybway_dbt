
  create view "postgres"."dbt_schema"."firebird_cut_phone_ods__dbt_tmp"
    
    
  as (
    select * from dbt_schema.ods_firebird_phone where dttm = '2024-11-13 11:17:03.858379+00:00'
  );