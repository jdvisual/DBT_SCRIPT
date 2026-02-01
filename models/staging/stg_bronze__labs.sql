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

  -- canonical timestamp name across the project
  current_timestamp() as load_ts_utc,

  {{ add_audit_columns() }}
from src
