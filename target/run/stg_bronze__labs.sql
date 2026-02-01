
  create or replace   view CHOP_ANALYTICS.CORE_CORE_STAGING.stg_bronze__labs
  
    
    
(
  
    "MEMBER_ID" COMMENT $$$$, 
  
    "PLAN_ID" COMMENT $$$$, 
  
    "PROVIDER_ID" COMMENT $$$$, 
  
    "DOB" COMMENT $$$$, 
  
    "GENDER" COMMENT $$$$, 
  
    "COVERAGE_START" COMMENT $$$$, 
  
    "COVERAGE_END" COMMENT $$$$, 
  
    "LOAD_TS_UTC" COMMENT $$$$, 
  
    "DBT_INVOCATION_ID" COMMENT $$$$
  
)

   as (
    with src as (
  select * from CHOP_ANALYTICS.BRONZE.members
)

select
  member_id,
  plan_id,
  provider_id,
  date_of_birth::date as dob,
  gender,
  coverage_start::date as coverage_start,
  coverage_end::date as coverage_end,

  -- canonical timestamp name across the project
  current_timestamp() as load_ts_utc,

  
  '019c1673-a977-77a3-b34a-3321813063d3' as dbt_invocation_id

from src
  );

