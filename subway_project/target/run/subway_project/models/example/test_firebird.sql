
  create view "postgres"."dbt_schema"."test_firebird__dbt_tmp"
    
    
  as (
    select * from dbt_schema.ods_firebird_client --where dttm = '1960-01-01 00:00:00'
  );