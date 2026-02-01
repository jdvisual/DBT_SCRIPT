with src as (
  select * from {{ source('bronze', 'claims') }}
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
