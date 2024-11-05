{{ ins_hub_macros( '"dbt_schema"."GPR_RV_H_CARD"', 'ods_receipt_post_cut', "card_rk", ("id_disc_card", )  )  }}

--depends on {{ ref('ods_receipt_post_cut') }}