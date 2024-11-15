

/*
    тек реализация:
    mass_arg, 
    type_mass_arg, 
    args_source_tab=( )

    mass_arg - строка - название поля, которое храним как группу
  
    type_mass_arg - строка - название поля, как mass_arg, 
    но в м сате может называться подругому, поэтому явно указываем поле

    args_source_tab=( ) - tuple из строк - все остальные названия полей из источника 
*/

-- нумеруем строки внутри группы из источника
with rn_hash_diff_from_source as ( 
    select 
        -- выбираем все строки из источника
        t1.*
	    -- нумируем строки - поле row_num
        , row_number () over(
            -- нумируем в пределах партиции по логическим ключам, 
            -- например id client`а в случае с номерами
            partition by  birthday  ,  name  
            -- сортируем по дате dttm или по текущему времени если в сате такой записи нет
            -- также сортируем по стобцам атрибутов - TODO
            order by coalesce (t2.dataflow_dttm, current_timestamp), phone
        ) as row_num
    from
	    "postgres"."dbt_schema"."ods_cut_client_from_star_orcl" t1
    -- джойним источник и сателит
    left join (
        select 
            -- выбираем rk сущности
            client_rk
            -- выбираем тех поле - дату и время выполнения dttm
            , dataflow_dttm
            -- выбираем поля из таблица сата
            , phone_desc 
        -- таблица сателита
        from "dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR" 
        where actual_flg = 1 and delete_flg = 0
    ) as t2
        on md5(  t1.id || '#' ||   t1.oid) = t2.client_rk 
        and t1.phone = t2.phone_desc
)
-- формируем md5 для rk и hash_diff для новых и измененных 
, form_md5 as (
    select 
        md5(  id || '#' ||   oid) client_rk -- формируем rk на связанную сущность
        , row_num
        , '1960-01-01 00:00:00'::timestamp valid_from_dttm
        , md5(
            string_agg( phone || '#' || row_num, '#' ) over(
                partition by  birthday ,  name 
                order by  name ,  phone ,  city ,  birthday ,  age  
                rows between unbounded preceding and unbounded following
            )
            || '#' ||   name  
            || '#' ||       city  
            || '#' ||     birthday  
            || '#' ||     age   
        ) hashdiff_key
    from 
	    rn_hash_diff_from_source
)
select
	'scheduled__1960-01-01T00:00:00+00:00' dataflow_id
    , '1960-01-01 00:00:00'::timestamp dataflow_dttm
    , oid source_system_dk
    , client_rk
    , row_num
    , hashdiff_key
    , 1 actual_flg
    , 0 delete_flg
    , 
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
	form_md5
where md5(  id || '#' ||   oid) in
(
select 
	client_rk
from
	(
	select
		client_rk
        , hashdiff_key
	from 
	    form_md5
	except
	select 
		client_rk
        , hashdiff_key   
	from 
		 "dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR" where actual_flg = 1 and delete_flg = 0)
		)


--depends on "postgres"."dbt_schema"."ods_cut_client_from_star_orcl"