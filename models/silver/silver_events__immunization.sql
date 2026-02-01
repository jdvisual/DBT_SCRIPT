{{ config(
  unique_key='event_id',
  incremental_strategy='merge'
) }}

with claims as (
  select
    member_id,
    provider_id,
    service_date as event_date,
    cpt_code,
    load_ts_utc
  from {{ ref('stg_bronze__claims') }}
  {{ incremental_where('load_ts_utc') }}
),

immunization_claim_events as (
  select
    {{ hash_key(["member_id", "provider_id", "event_date", "cpt_code"]) }} as event_id,
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
  {{ add_audit_columns() }}
from immunization_claim_events
