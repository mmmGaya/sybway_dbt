

select 
	'scheduled__1960-01-01T00:00:00+00:00' dataflow_id,
    '1960-01-01 00:00:00'::timestamp dataflow_dttm,
    oid source_system_dk, 
    md5(  id || '#' ||   oid) client_rk, 
    row_num,
    '1960-01-01 00:00:00'::timestamp valid_from_dttm, 
    md5(string_agg( phone || '#' || row_num, '#' ) 
    over(
    partition by 
     birthday ,  name 
    order by 
     name ,  phone ,  city ,  birthday ,  age  
    rows between unbounded preceding and unbounded following) 
    || '#' ||   name  || '#' ||       city  || '#' ||     birthday  || '#' ||     age   ) hashdiff_key,
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
	( select t1.*,
	row_number () 
    over(partition by 
    	 birthday  ,  name  
    	order by 
    	coalesce (t2.dataflow_dttm, current_timestamp), phone
    	) row_num
    from 
	"postgres"."dbt_schema"."ods_client_cut" t1
    left join
    (select client_rk, dataflow_dttm, phone_desc 
    from "dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR" 
    where actual_flg = 1 and delete_flg = 0)
    t2
    on md5(  t1.id || '#' ||   t1.oid) = t2.client_rk and t1.phone = t2.phone_desc)
where md5(  id || '#' ||   oid) in
(
select 
	client_rk
from
	(
	select 
		md5(  id || '#' ||   oid) client_rk, 
		md5(string_agg( phone || '#' || row_num, '#' ) 
	    over(
	    partition by 
	     birthday ,  name 
	    order by 
	     name ,  phone ,  city ,  birthday ,  age  
	    rows between unbounded preceding and unbounded following)
	    || '#' ||   name  || '#' ||       city  || '#' ||     birthday  || '#' ||     age   ) hashdiff_key
	from 
	    ( select t1.*,
	row_number () 
    over(partition by 
    	 birthday  ,  name  
    	order by 
    	coalesce (t2.dataflow_dttm, current_timestamp), phone
    	) row_num
    from 
	"postgres"."dbt_schema"."ods_client_cut" t1
    left join
    (select client_rk, dataflow_dttm, phone_desc 
    from "dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR" 
    where actual_flg = 1 and delete_flg = 0)
    t2
    on md5(  t1.id || '#' ||   t1.oid) = t2.client_rk and t1.phone = t2.phone_desc)
	except
	select 
		client_rk, 
		hashdiff_key   
	from 
		 "dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR" where actual_flg = 1 and delete_flg = 0)
		)


--depends on "postgres"."dbt_schema"."ods_client_cut"