{{ config(materialized='table') }}

select * from ods_profile_card_post where execution_date = '{{ var('execution_date') }}'