with src as (
  select * from {{ source('bronze', 'members') }}
)

select
  member_id,
  plan_id,
  provider_id,
  date_of_birth::date as dob,
  gender,
  coverage_start::date as coverage_start,
  coverage_end::date as coverage_end,

  -- staging build timestamp (exists even if bronze table has no load_ts)
  current_timestamp() as load_ts_utc,

  -- dbt run id
  {{ add_audit_columns() }}
from src
