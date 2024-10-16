{{ config(materialized='table') }}

select * from raw_source_subway where execution_date = '{{ var('execution_date') }}'

-- select '{{ var('execution_date') }}'
