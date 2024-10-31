
  create view "postgres"."dbt_schema"."ins_to_hub__dbt_tmp"
    
    
  as (
    

SELECT 
    'manual__2024-10-31T13:36:46.534879+00:00' dataflow_id,
    '2024-10-31 13:36:46.534879+00:00'::timestamp dataflow_dttm,
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
--     'manual__2024-10-31T13:36:46.534879+00:00' dataflow_id,
--     '2024-10-31 13:36:46.534879+00:00'::timestamp dataflow_dttm,
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
  );