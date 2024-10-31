

SELECT 
    'scheduled__1960-01-01T00:00:00+00:00' dataflow_id,
    '1960-01-01 00:00:00'::timestamp dataflow_dttm,
    oid source_system_dk,
    md5(id || '#' || oid) client_rk,
    id || '#' || oid hub_key
FROM 
    "postgres"."dbt_schema"."ods_client_cut"  ods
	LEFT JOIN 
	"dbt_schema"."GPR_RV_H_CLIENT" h_cl
	ON md5(ods.id || '#' || ods.oid) = h_cl.client_rk
WHERE h_cl.client_rk IS NULL





-- SELECT 
--     'scheduled__1960-01-01T00:00:00+00:00' dataflow_id,
--     '1960-01-01 00:00:00'::timestamp dataflow_dttm,
--     oid source_system_dk,
--     md5(id || '#' || oid) client_rk,
--     id || '#' || oid hub_key
-- FROM 
--     "postgres"."dbt_schema"."ods_client_cut" ods
-- 	LEFT JOIN 
-- 	dbt_schema."GPR_RV_H_CLIENT" h_cl
-- 	ON md5(ods.id || '#' || ods.oid) = h_cl.client_rk
-- WHERE h_cl.client_rk IS NULL


    -- (select 
    --     *
    -- from 
    --    "postgres"."dbt_schema"."ods_client_cut"
    -- where 
    --     (oid, id) in  
    --         (select oid, id from  "postgres"."dbt_schema"."ods_client_cut"
    --         except
    --         select source_system_dk, client_rk from dbt_schema."GPR_RV_H_CLIENT"
    --         )
    -- )

    --depends on "postgres"."dbt_schema"."ods_client_cut"