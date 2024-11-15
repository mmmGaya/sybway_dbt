select 
    dataflow_id, 
    dataflow_dttm,
    card_tech_key, 
    valid_from_dttm, 
    client_name_desc, 
    client_phone_desc, 
    client_city_dt, 
    client_birthday_dt,
    client_age_cnt
from  dual
    

--depends on  "postgres"."dbt_schema"."ins_to_new_b_receip_card"
--depends on "postgres"."dbt_schema"."ins_to_modif_b_receip_card"