select dataflow_id, dataflow_dttm,
       client_rk, valid_from_dttm, valid_to_dttm,
       client_subway_star_vf_dttm,
       profile_client_post_vf_dttm, '1000-10-10'::timestamp client_phones_vf_dttm -- заглушка
    from (
    {{ ins_pit_modif_macros('CLIENT', 
                       ((source("dbt_schema", "GPR_RV_M_CLIENT_SUBWAY_STAR"), 'client_subway_star_vf_dttm', "M"), 
                        (source("dbt_schema", "GPR_RV_M_CLIENT_PROFILE_POST"), 'profile_client_post_vf_dttm', "M")), 
                       'client_rk') }}
    union all
    select * from {{ ref('ins_new_to_pit_client') }}
    )

--depends on {{ ref('ins_new_to_pit_client') }}
--depends on {{ source('dbt_schema', 'GPR_RV_M_CLIENT_SUBWAY_STAR') }}
--depends on {{ source('dbt_schema', 'GPR_RV_M_CLIENT_PROFILE_POST') }}