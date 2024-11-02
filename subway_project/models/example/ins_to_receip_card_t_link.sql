select '{{ var('run_id') }}' run_id, '{{ var('execution_date') }}'::timestamp execution_date, 
	    md5(id_operation || '#' || id_product || '#' || sel_dttm || '#' || oid) receip_rk,
	    id_seller shop_cnt, id_buyer client_cnt, md5(id_product || '#' || oid) plu_rk, 
	    id_product_connection plu_x_plu_cnt, id_disc_card card_cnt, 
	    sel_dttm, cnt, price, combo_group, tovar_group
from {{ref('ods_receipt_post_cut')}}

--depends on {{ref('ods_receipt_post_cut')}}