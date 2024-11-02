
  create view "postgres"."dbt_schema"."ins_to_hub_receip_card__dbt_tmp"
    
    
  as (
    

SELECT 
    'manual__2024-11-02T12:49:46.788873+00:00' dataflow_id,
    '2024-11-02 12:49:46.788873+00:00'::timestamp dataflow_dttm,
    oid source_system_dk,
    md5(  id_disc_card || '#' ||   oid) client_rk,
     id_disc_card || '#' ||   oid hub_key
 
FROM 
    "postgres"."dbt_schema"."ods_receipt_post_cut"  ods
	LEFT JOIN 
	"dbt_schema"."GPR_RV_H_CARD" h_cl
	ON md5(  ods.id_disc_card || '#' ||   ods.oid) = h_cl.card_rk
WHERE h_cl.card_rk IS NULL


  );