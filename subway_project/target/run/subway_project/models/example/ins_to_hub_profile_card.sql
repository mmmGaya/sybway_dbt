
  create view "postgres"."dbt_schema"."ins_to_hub_profile_card__dbt_tmp"
    
    
  as (
    

SELECT 
    'manual__2024-11-05T11:53:56.016169+00:00' dataflow_id,
    '2024-11-05 11:53:56.016169+00:00'::timestamp dataflow_dttm,
    oid source_system_dk,
    md5(  id || '#' ||   oid) client_rk,
     id || '#' ||   oid hub_key
 
FROM 
    "postgres"."dbt_schema"."ods_profile_post_cut"  ods
	LEFT JOIN 
	"dbt_schema"."GPR_RV_H_CARD" h_cl
	ON md5(  ods.id || '#' ||   ods.oid) = h_cl.card_rk
WHERE h_cl.card_rk IS NULL



--depends on "postgres"."dbt_schema"."ods_profile_post_cut"
  );