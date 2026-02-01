
  create or replace   view CHOP_ANALYTICS.CORE_CORE_STAGING.stg_bronze__rx
  
    
    
(
  
    "RX_CLAIM_ID" COMMENT $$$$, 
  
    "MEMBER_ID" COMMENT $$$$, 
  
    "NDC" COMMENT $$$$, 
  
    "FILL_DATE" COMMENT $$$$, 
  
    "DAYS_SUPPLY" COMMENT $$$$, 
  
    "LOAD_TS_UTC" COMMENT $$$$
  
)

   as (
    with src as (
  select * from CHOP_ANALYTICS.BRONZE.rx
)
select
  rx_claim_id,
  member_id,
  ndc,
  fill_date::date as fill_date,
  days_supply::number as days_supply,
  load_ts_utc::timestamp as load_ts_utc
from src
  );

