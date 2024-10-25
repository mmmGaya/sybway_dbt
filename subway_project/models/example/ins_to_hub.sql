SELECT 
    '{{ var('run_id') }}' dataflow_id,
    '{{ var('execution_date') }}'::timestamp dataflow_dttm,
    oid source_system_dk,
    id client_rk,
    md5(id || '#' || oid) hub_key
FROM 
    (select 
        *
    from 
       {{ ref('ods_client_cut') }}
    where 
        (oid, id) in  
            (select oid, id from  {{ ref('ods_client_cut') }}
            except
            select source_system_dk, client_rk from dbt_schema."GPR_RV_H_CLIENT"
            )
    )

    --depends on {{ ref('ods_client_cut') }}