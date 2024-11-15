

SELECT 
    'manual__2024-11-15T07:38:12.779997+00:00' dataflow_id,
    '2024-11-15 07:38:12.779997+00:00'::timestamp dataflow_dttm,
    oid source_system_dk,
    md5(  id || '#' ||   oid) client_rk,
     id || '#' ||   oid hub_key
FROM 
    "postgres"."dbt_schema"."ods_cut_client_from_star_orcl"  ods
	LEFT JOIN 
	"dbt_schema"."GPR_RV_H_CLIENT" h_cl
	ON md5(  ods.id || '#' ||   ods.oid) = h_cl.client_rk
WHERE h_cl.client_rk IS NULL

