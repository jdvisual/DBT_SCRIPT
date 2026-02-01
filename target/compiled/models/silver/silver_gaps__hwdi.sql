



with members as (
  select
    member_id,
    plan_id,
    provider_id,
    dob,
    gender
  from CHOP_ANALYTICS.CORE_SILVER.silver_member_dim
),

eligible as (
  select
    m.*,
    datediff('year', dob, to_date('2025-12-31')) as age_end_of_year
  from members m
  where datediff('year', dob, to_date('2025-12-31')) >= 18
),

events as (
  select
    member_id,
    max(case
          when event_date between to_date('2025-01-01') and to_date('2025-12-31')
          then 1 else 0 end
    ) as has_immunization_event
  from CHOP_ANALYTICS.CORE_SILVER.silver_events__immunization
  group by 1
),

final as (
  select
    
  md5(
    concat_ws('||', coalesce(cast(e.member_id as varchar), ''), coalesce(cast('2025' as varchar), ''), coalesce(cast('HWD_IMMUNIZATION' as varchar), ''))
  )
 as gap_id,
    e.member_id,
    e.plan_id,
    e.provider_id,
    2025::number as measurement_year,
    'HWD_IMMUNIZATION'::varchar as measure_id,
    case
      when coalesce(ev.has_immunization_event, 0) = 1 then 'CLOSED'
      else 'OPEN'
    end as gap_status,
    current_date() as as_of_date,
    
  '019c1673-a977-77a3-b34a-3321813063d3' as dbt_invocation_id

  from eligible e
  left join events ev
    on e.member_id = ev.member_id
)

select * from final