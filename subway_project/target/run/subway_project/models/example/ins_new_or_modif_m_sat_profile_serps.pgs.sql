
  create view "postgres"."dbt_schema"."ins_new_or_modif_m_sat_profile_serps.pgs__dbt_tmp"
    
    
  as (
    

select 
	'manual__2024-11-12T13:50:10.437173+00:00' dataflow_id,
    '2024-11-12 13:50:10.437173+00:00'::timestamp dataflow_dttm,
    oid source_system_dk, 
    md5(  id || '#' ||   oid) client_rk, 
    row_num,
    '2024-11-12 13:50:10.437173+00:00'::timestamp valid_from_dttm, 
    md5(string_agg( phone_num || '#' || row_num, '#' ) 
    over(
    partition by 
     birthday ,  fio 
    order by 
     fio ,  phone_num ,  birthday  
    rows between unbounded preceding and unbounded following) 
    || '#' ||   fio  || '#' ||       birthday   ) hashdiff_key,
    1 actual_flg,
    0 delete_flg,
    
        fio
        , 
    
        phone_num
        , 
    
        birthday
        
    
from 
	( select t1.*,
	row_number () 
    over(partition by 
    	 birthday  ,  fio  
    	order by 
    	coalesce (t2.dataflow_dttm, current_timestamp), phone_num
    	) row_num
    from 
	"postgres"."dbt_schema"."ods_profile_post_cut" t1
    left join
    (select client_rk, dataflow_dttm, phone_desc 
    from "dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST" 
    where actual_flg = 1 and delete_flg = 0)
    t2
    on md5(  t1.id || '#' ||   t1.oid) = t2.client_rk and t1.phone_num = t2.phone_desc)
where md5(  id || '#' ||   oid) in
(
select 
	client_rk
from
	(
	select 
		md5(  id || '#' ||   oid) client_rk, 
		md5(string_agg( phone_num || '#' || row_num, '#' ) 
	    over(
	    partition by 
	     birthday ,  fio 
	    order by 
	     fio ,  phone_num ,  birthday  
	    rows between unbounded preceding and unbounded following)
	    || '#' ||   fio  || '#' ||       birthday   ) hashdiff_key
	from 
	    ( select t1.*,
	row_number () 
    over(partition by 
    	 birthday  ,  fio  
    	order by 
    	coalesce (t2.dataflow_dttm, current_timestamp), phone_num
    	) row_num
    from 
	"postgres"."dbt_schema"."ods_profile_post_cut" t1
    left join
    (select client_rk, dataflow_dttm, phone_desc 
    from "dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST" 
    where actual_flg = 1 and delete_flg = 0)
    t2
    on md5(  t1.id || '#' ||   t1.oid) = t2.client_rk and t1.phone_num = t2.phone_desc)
	except
	select 
		client_rk, 
		hashdiff_key   
	from 
		 "dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST" where actual_flg = 1 and delete_flg = 0)
		)


--depends on "postgres"."dbt_schema"."ods_profile_post_cut"
  );