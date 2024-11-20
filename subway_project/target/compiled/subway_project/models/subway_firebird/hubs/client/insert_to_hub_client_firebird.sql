-- вставляем новые rk в объект hub клиента


SELECT 
    'scheduled__1960-01-01T00:00:00+00:00' dataflow_id,
    '2024-11-20 10:43:48.001325+00:00'::timestamp dataflow_dttm,
    oid source_system_dk,
    md5(  id || '#' ||   oid) client_rk,
     id || '#' ||   oid hub_key
FROM 
    "postgres"."dbt_schema"."firebird_cut_client_ods"  ods
	LEFT JOIN 
	"dbt_schema"."GPR_RV_H_CLIENT" h_cl
	ON md5(  ods.id || '#' ||   ods.oid) = h_cl.client_rk
WHERE h_cl.client_rk IS NULL

