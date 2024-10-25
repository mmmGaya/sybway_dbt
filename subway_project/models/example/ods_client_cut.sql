{{ config(materialized='table') }}

select * from ods_client_csv where execution_date = '{{ var('execution_date') }}'

