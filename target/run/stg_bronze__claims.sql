
  create or replace   view CHOP_ANALYTICS.CORE_CORE_STAGING.stg_bronze__claims
  
    
    
(
  
    "CLAIM_ID" COMMENT $$$$, 
  
    "MEMBER_ID" COMMENT $$$$, 
  
    "PROVIDER_ID" COMMENT $$$$, 
  
    "SERVICE_DATE" COMMENT $$$$, 
  
    "CPT_CODE" COMMENT $$$$, 
  
    "ICD10_CODE" COMMENT $$$$, 
  
    "PAID_AMOUNT" COMMENT $$$$, 
  
    "LOAD_TS_UTC" COMMENT $$$$
  
)

   as (
    with src as (
  select * from CHOP_ANALYTICS.BRONZE.claims
)
select
  claim_id,
  member_id,
  provider_id,
  service_date::date as service_date,
  cpt_code,
  icd10_code,
  paid_amount::number(18,2) as paid_amount,
  load_ts_utc::timestamp as load_ts_utc
from src
  );

