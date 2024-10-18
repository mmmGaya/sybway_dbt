{{ config(materialized='table') }}

select * from ods_client_csv where execution_date = '{{ var('execution_date') }}'
-- select * from ods_client_csv where execution_date = (select execution_date from metadata_airflow_test where source_n = 'csv')
-- select '{{ var('execution_date') }}'
