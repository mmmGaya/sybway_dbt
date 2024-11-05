
  
    

  create  table "postgres"."dbt_schema"."ods_profile_post_cut__dbt_tmp"
  
  
    as
  
  (
    

select * from ods_profile_card_post where execution_date = '2024-11-05 11:53:56.016169+00:00'
  );
  