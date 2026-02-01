
  
    

create or replace transient  table CHOP_ANALYTICS.CORE_GOLD.mart_hwdi_plan_rollup
    
    
    
    as (with g as (
  select * from CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi
)
select
  plan_id,
  measurement_year,
  measure_id,
  count(*) as eligible_members,
  sum(case when gap_status = 'CLOSED' then 1 else 0 end) as closed_members,
  (sum(case when gap_status = 'CLOSED' then 1 else 0 end) / nullif(count(*),0))::float as closure_rate,
  current_timestamp() as mart_load_ts_utc
from g
group by 1,2,3
    )
;


  