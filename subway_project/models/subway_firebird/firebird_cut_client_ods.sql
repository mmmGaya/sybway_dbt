{{ config(materialized='table') }}

select * from dbt_schema.ods_firebird_client where dttm = '{{ var('execution_date') }}'