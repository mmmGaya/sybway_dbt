

-- нумеруем строки внутри группы из источника
with rn_hash_diff_from_source as ( 
    select 
        -- выбираем все строки из источника
        source.*
	    -- нумируем строки - поле row_num
        , row_number () over(
            -- нумируем в пределах партиции по логическим ключам, 
            -- например id client`а в случае с номерами
            partition by 
                 
                    birthday  , 
                 
                    fio 
                 
            -- сортируем по дате dttm или по текущему времени если в сате такой записи нет
            -- также сортируем по стобцам атрибутов 
            -- TODO - REVIEW AND TESTS
            order by 
                coalesce (m_sat.dataflow_dttm, current_timestamp)
                ,  fio ,  phone_num ,  birthday 
        ) as row_num
    from
	    "postgres"."dbt_schema"."ods_cut_client_profile_card_post_pg" source
    -- джойним источник и сателит
    left join (
        select 
            -- выбираем rk сущности
            client_rk
            -- выбираем тех поле - дату и время выполнения dttm
            , dataflow_dttm
            -- выбираем поля из таблица сата
            ,  
        -- таблица сателита
        from "dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST" 
        where actual_flg = 1 and delete_flg = 0
    ) as m_sat
        -- join по rk
        on md5( 
             
                source.id || '#' || 
             source.oid
        ) = m_sat.client_rk 
        -- и по всем остальным полям
        -- TODO - REVIEW AND TESTS
         and source.fio = m_sat.fio_desc  and source.phone_num = m_sat.phone_desc  and source.birthday = m_sat.birthday_dt 
        -- and source. = m_sat.
)
-- формируем md5 для rk и hash_diff для новых и измененных 
, form_md5 as (
    select 
        -- формируем rk на связанную сущность
        md5(  id || '#' ||   oid) client_rk 
        -- выводим номера строк в группе
        , row_num
        -- получаем значение для поля valid_from_dttm
        , '1960-01-01 00:00:00'::timestamp valid_from_dttm
        -- формируем hash_diff из всех значений всех столбцов в группе
        -- TODO
        , md5(
            -- вычисляем hash_diff
            -- TODO - REVIEW AND TESTS
            
                string_agg( fio, '#' ) over(
                    -- разбиваем окно на партиции по ключам разбиения
                    partition by  birthday ,  fio 
                    -- сортируем строки по row_num
                    -- TODO - REVIEW AND TESTS
                    order by row_num
                    rows between unbounded preceding and unbounded following
                ) || '#' ||
            
                string_agg( phone_num, '#' ) over(
                    -- разбиваем окно на партиции по ключам разбиения
                    partition by  birthday ,  fio 
                    -- сортируем строки по row_num
                    -- TODO - REVIEW AND TESTS
                    order by row_num
                    rows between unbounded preceding and unbounded following
                ) || '#' ||
            
                string_agg( birthday, '#' ) over(
                    -- разбиваем окно на партиции по ключам разбиения
                    partition by  birthday ,  fio 
                    -- сортируем строки по row_num
                    -- TODO - REVIEW AND TESTS
                    order by row_num
                    rows between unbounded preceding and unbounded following
                ) || '#' ||
             row_num            
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
        fio
        , 
    
        phone_num
        , 
    
        birthday
        
    
from 
	form_md5
-- фильтруем rk - выбираем только новые или измененные (hash_diff) по атрибутам строки
where client_rk in (
    select 
        client_rk
    from (
    -- вычитаем строки из источника строками таблицей м сата
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
            -- выбираем только актуальные не удаленные записи
            "dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST" where actual_flg = 1 and delete_flg = 0
    )
)


--depends on "postgres"."dbt_schema"."ods_cut_client_profile_card_post_pg"