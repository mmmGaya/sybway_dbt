{{ config(materialized='table') }}

select * from dbt_schema.ods_firebird_phone where execution_date = '{{ var('execution_date') }}'