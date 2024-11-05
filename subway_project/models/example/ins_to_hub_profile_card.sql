{{ ins_hub_macros( '"dbt_schema"."GPR_RV_H_CARD"', 'ods_profile_post_cut', "card_rk", ("id", )  )  }}

--depends on {{ ref('ods_profile_post_cut') }}