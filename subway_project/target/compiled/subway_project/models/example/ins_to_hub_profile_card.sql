

SELECT 
    'scheduled__1960-01-01T00:00:00+00:00' dataflow_id,
    '1960-01-01 00:00:00'::timestamp dataflow_dttm,
    oid source_system_dk,
    md5(  card_num || '#' ||   oid) card_rk,
     card_num || '#' ||   oid hub_key
FROM 
    "postgres"."dbt_schema"."ods_profile_post_cut"  ods
	LEFT JOIN 
	"dbt_schema"."GPR_RV_H_CARD" h_cl
	ON md5(  ods.card_num || '#' ||   ods.oid) = h_cl.card_rk
WHERE h_cl.card_rk IS NULL



--depends on "postgres"."dbt_schema"."ods_profile_post_cut"