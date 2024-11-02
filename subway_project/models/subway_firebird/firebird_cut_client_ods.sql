{{ config(materialized='table') }}

select * from dbt_schema.ods_firebird_client where execution_date = '{{ var('execution_date') }}'