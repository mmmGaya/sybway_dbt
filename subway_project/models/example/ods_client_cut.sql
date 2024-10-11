{{ config(materialized='table') }}

select * from raw_source_subway where execution_date = (select execution_date from metadata_airflow)