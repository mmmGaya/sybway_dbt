
  create view "postgres"."dbt_schema"."insert_delete_update_m_sat_phones_firebird__dbt_tmp"
    
    
  as (
    select * from "postgres"."dbt_schema"."insert_delete_m_sat_phones_firebird"
union all
select * from "postgres"."dbt_schema"."insert_update_m_sat_phones_firebird"
  );