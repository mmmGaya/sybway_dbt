

select 
    
        'manual__2024-11-05T10:09:21.817848+00:00' dataflow_id,
        '2024-11-05 10:09:21.817848+00:00'::timestamp dataflow_dttm,
        oid source_system_dk, 
        md5(  id || '#' ||   oid) client_rk, 
        '2024-11-05 10:09:21.817848+00:00'::timestamp valid_from_dttm, 
        md5(  name || '#' ||  phone || '#' ||  city || '#' ||  birthday || '#' ||  age ) hashdiff_key,
        1 actual_flg,
        0 delete_flg,
        
            name
            , 
        
            phone
            , 
        
            city
            , 
        
            birthday
            , 
        
            age
            
        
    
from 
	"postgres"."dbt_schema"."ods_client_cut"
where md5(  id || '#' ||   oid) in
(
select 
	client_rk
from
	(
	select 
		md5(  id || '#' ||   oid) client_rk, 
		md5(  name || '#' || phone || '#' || city || '#' || birthday || '#' || age ) hashdiff_key 
	from 
	    "postgres"."dbt_schema"."ods_client_cut"
	except
	select 
		client_rk, 
		hashdiff_key   
	from 
		 "dbt_schema"."GPR_RV_S_CLIENT" where actual_flg = 1 and delete_flg = 0)
		)




--depends on "postgres"."dbt_schema"."ods_client_cut"