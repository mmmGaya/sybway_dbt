{{ ins_hub_macros( '"dbt_schema"."GPR_RV_H_CLIENT"', 'ods_profile_post_cut', "client_rk", ("id", )  )  }}

--depends on {{ ref('ods_profile_post_cut') }}