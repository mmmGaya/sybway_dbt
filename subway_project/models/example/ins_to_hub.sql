

{{ ins_hub_macros( '"dbt_schema"."GPR_RV_H_CLIENT"' ) }}



-- SELECT 
--     '{{ var('run_id') }}' dataflow_id,
--     '{{ var('execution_date') }}'::timestamp dataflow_dttm,
--     oid source_system_dk,
--     md5(id || '#' || oid) client_rk,
--     id || '#' || oid hub_key
-- FROM 
--     {{ ref('ods_client_cut') }} ods
-- 	LEFT JOIN 
-- 	dbt_schema."GPR_RV_H_CLIENT" h_cl
-- 	ON md5(ods.id || '#' || ods.oid) = h_cl.client_rk
-- WHERE h_cl.client_rk IS NULL


    -- (select 
    --     *
    -- from 
    --    {{ ref('ods_client_cut') }}
    -- where 
    --     (oid, id) in  
    --         (select oid, id from  {{ ref('ods_client_cut') }}
    --         except
    --         select source_system_dk, client_rk from dbt_schema."GPR_RV_H_CLIENT"
    --         )
    -- )

    --depends on {{ ref('ods_client_cut') }}