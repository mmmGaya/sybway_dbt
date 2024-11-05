select 'scheduled__1960-01-01T00:00:00+00:00' run_id, '1960-01-01 00:00:00'::timestamp execution_date, 
	    md5(id_operation || '#' || id_product || '#' || sel_dttm || '#' || tovar_group || '#' || oid) receip_rk,
	    id_seller shop_cnt, id_buyer client_cnt, md5(id_product || '#' || oid) plu_rk, 
	    id_product_connection plu_x_plu_cnt, id_disc_card card_cnt, 
	    sel_dttm, cnt, price, combo_group, tovar_group
from "postgres"."dbt_schema"."ods_receipt_post_cut"

--depends on "postgres"."dbt_schema"."ods_receipt_post_cut"