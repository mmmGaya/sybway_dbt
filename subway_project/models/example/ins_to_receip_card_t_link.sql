select '{{ var('run_id') }}', '{{ var('execution_date') }}'::timestamp, 
	    md5(id_operation || '#' || id_seller || '#' || id_buyer || '#' || id_product || '#' || coalesce(id_product_connection, 0) || '#' || sel_dttm || '#' || oid) receip_rk,
	    md5(id_seller || '#' || oid) shop_rk, md5(id_buyer || '#' || oid) client_rk, md5(id_product || '#' || oid) plu_rk, 
	    md5(id_product_connection || '#' || oid) plu_x_plu_rk, md5(id_disc_card || '#' || oid) card_rk, 
	    sel_dttm, cnt, price, combo_group, tovar_group
from {{ref('ods_receipt_post_cut')}}

--depends on {{ref('ods_receipt_post_cut')}}