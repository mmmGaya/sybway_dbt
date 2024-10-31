select dataflow_id, dataflow_dttm, 
       client_rk, valid_from_dttm, valid_to_dttm,
       client_subway_star_vf_dttm
from (
    select dataflow_id, dataflow_dttm, 
           client_rk, valid_from_dttm, valid_to_dttm,
           client_subway_star_vf_dttm
    from "postgres"."dbt_schema"."ins_modif_pit"
    union all
    select dataflow_id, dataflow_dttm, 
           client_rk, valid_from_dttm, valid_to_dttm,
           client_subway_star_vf_dttm
    from "postgres"."dbt_schema"."ins_new_pit"
    )

--depends on  "postgres"."dbt_schema"."ins_modif_pit"
--depends on "postgres"."dbt_schema"."ins_new_pit"