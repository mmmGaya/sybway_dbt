
  create view "postgres"."dbt_schema"."firebird_cut_phone_ods__dbt_tmp"
    
    
  as (
    select * from dbt_schema.ods_firebird_phone where dttm = '2024-11-13 12:40:39.905320+00:00'
  );