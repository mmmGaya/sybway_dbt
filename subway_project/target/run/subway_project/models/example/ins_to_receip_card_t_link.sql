
  create view "postgres"."dbt_schema"."ins_to_receip_card_t_link__dbt_tmp"
    
    
  as (
    select 'manual__2024-11-08T12:06:37.788085+00:00' run_id, '2024-11-08 12:06:37.788085+00:00'::timestamp execution_date, 
	    md5(id_operation || '#' || id_seller || '#' || client_rk || '#' || id_product || '#' || coalesce(id_product_connection, -1) || '#' || sel_dttm || '#' || tovar_group || '#' || oid) receip_rk,
	    md5(id_seller || '#' || oid) shop_rk, -- Заглушка, пока нет сущности в проекте
		client_rk, 
		md5(id_product || '#' || oid) plu_rk, -- Заглушка, пока нет сущности в проекте
	    md5(id_product_connection || '#' || oid) plu_x_plu_rk, -- Заглушка, пока нет сущности в проекте
		id_disc_card card_cnt, 
		id_operation receip_num_cnt,
	    sel_dttm, cnt, price, combo_group, tovar_group
from "postgres"."dbt_schema"."ods_receipt_post_cut" tt
join dbt_schema."GPR_RV_H_CLIENT" hc on regexp_instr(hc.hub_key, tt.id_buyer || '#') = 1
where hc.hub_key like '%#3515641477'

--depends on "postgres"."dbt_schema"."ods_receipt_post_cut"
  );