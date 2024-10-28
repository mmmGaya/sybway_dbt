SELECT
    '{{ var('run_id') }}' dataflow_id,
    '{{ var('execution_date') }}'::timestamp dataflow_dttm,
    oid source_system_dk, 
    md5(id|| '#' || oid) client_rk, 
    '{{ var('execution_date') }}'::timestamp valid_from_dttm, 
    md5(name || '#' || phone || '#' || city || '#' || birthday || '#' || age) hashdiff_key,
    1 actual_flg,
    0 delete_flg,
    name client_name_desc,
    phone client_phone_desc,
    city client_city_desc,
    birthday client_city_dt,
    age client_age_cnt
FROM 
     {{ ref('ods_client_cut') }}
WHERE md5(id || '#' || oid) IN 
            (SELECT
                hub_key
            FROM 
                (SELECT md5(id || '#' || oid) hub_key, md5(name || '#' || phone || '#' || city || '#' || birthday || '#' || age) hashdiff_key FROM  {{ ref('ods_client_cut') }}
                except
                SELECT client_rk, hashdiff_key FROM dbt_schema."GPR_RV_S_CLIENT" where actual_flg = 1 and delete_flg = 0
                 )
            )

--depends on {{ ref('ods_client_cut') }}