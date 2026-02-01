

with base as (

  select
    member_id,
    plan_id,
    provider_id,
    dob,
    gender,
    coverage_start,
    coverage_end,
    load_ts_utc
  from CHOP_ANALYTICS.CORE_CORE_STAGING.stg_bronze__members

  
    where load_ts_utc >= (
      select coalesce(max(load_ts_utc), '1900-01-01'::timestamp)
      from CHOP_ANALYTICS.CORE_SILVER.silver_member_dim
    )
  

),

dedup as (
  select
    *,
    row_number() over (
      partition by member_id
      order by load_ts_utc desc
    ) as rn
  from base
)

select
  member_id,
  plan_id,
  provider_id,
  dob,
  gender,
  coverage_start,
  coverage_end,
  load_ts_utc,
  
  '019c1673-a977-77a3-b34a-3321813063d3' as dbt_invocation_id

from dedup
qualify rn = 1