

with claims as (
  select
    member_id,
    provider_id,
    service_date as event_date,
    cpt_code,
    load_ts_utc
  from CHOP_ANALYTICS.CORE_CORE_STAGING.stg_bronze__claims
  
  
    where load_ts_utc >= (
      select coalesce(max(load_ts_utc), '1900-01-01'::timestamp)
      from CHOP_ANALYTICS.CORE_SILVER.silver_events__immunization
    )
  

),

immunization_claim_events as (
  select
    
  md5(
    concat_ws('||', coalesce(cast(member_id as varchar), ''), coalesce(cast(provider_id as varchar), ''), coalesce(cast(event_date as varchar), ''), coalesce(cast(cpt_code as varchar), ''))
  )
 as event_id,
    member_id,
    provider_id,
    event_date,
    'IMMUNIZATION' as event_type,
    cpt_code as event_code,
    load_ts_utc
  from claims
  where cpt_code in ('90686','90756','90658')
)

select
  event_id,
  member_id,
  provider_id,
  event_date,
  event_type,
  event_code,
  load_ts_utc,
  
  '019c1673-a977-77a3-b34a-3321813063d3' as dbt_invocation_id

from immunization_claim_events