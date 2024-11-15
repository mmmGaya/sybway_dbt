
  
    

  create  table "postgres"."dbt_schema"."ods_cut_client_profile_card_post_pg__dbt_tmp"
  
  
    as
  
  (
    

select * from ods_profile_card_post where execution_date = '2024-11-15 07:27:24.467128+00:00'
  );
  