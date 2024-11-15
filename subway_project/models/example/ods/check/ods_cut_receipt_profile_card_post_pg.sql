{{ config(materialized='table') }}

select * from ods_receipt_post where execution_date = '{{ var('execution_date') }}'