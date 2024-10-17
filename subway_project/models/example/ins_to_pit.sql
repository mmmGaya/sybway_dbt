select dataflow_id, dataflow_dttm, 
       client_rk, valid_from_dttm, valid_to_dttm,
       client_subway_star_vf_dttm
from (
    select dataflow_id, dataflow_dttm, 
           client_rk, valid_from_dttm, valid_to_dttm,
           client_subway_star_vf_dttm
    from {{ ref('ins_modif_pit') }}
    union all
    select dataflow_id, dataflow_dttm, 
           client_rk, valid_from_dttm, valid_to_dttm,
           client_subway_star_vf_dttm
    from {{ ref('ins_new_pit') }}
    )

--depends on  {{ ref('ins_modif_pit') }}
--depends on {{ ref('ins_new_pit') }}