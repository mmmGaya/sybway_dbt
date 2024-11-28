
  create view "postgres"."dbt_schema"."insert_to_hub_client_firebird__dbt_tmp"
    
    
  as (
    -- вставляем новые rk в объект hub клиента


SELECT 
    'manual__2024-11-27T11:50:25.649364+00:00' dataflow_id,
    '2024-11-27 11:50:25.649364+00:00'::timestamp dataflow_dttm,
    oid source_system_dk,
    md5(  id || '#' ||   oid) client_rk,
     id || '#' ||   oid hub_key
FROM 
    "postgres"."dbt_schema"."firebird_cut_client_ods"  ods
	LEFT JOIN 
	"dbt_schema"."GPR_RV_H_CLIENT" h_cl
	ON md5(  ods.id || '#' ||   ods.oid) = h_cl.client_rk
WHERE h_cl.client_rk IS NULL


  );