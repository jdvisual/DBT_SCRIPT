{{ config(
  unique_key='member_id',
  incremental_strategy='merge'
) }}

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
  from {{ ref('stg_bronze__members') }}

  {% if is_incremental() %}
    where load_ts_utc >= (
      select coalesce(max(load_ts_utc), '1900-01-01'::timestamp)
      from {{ this }}
    )
  {% endif %}

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
  {{ add_audit_columns() }}
from dedup
qualify rn = 1
