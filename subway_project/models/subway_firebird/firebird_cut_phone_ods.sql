{{ config(materialized='table') }}

select * from dbt_schema.ods_firebird_phone where dttm = '{{ var('execution_date') }}'