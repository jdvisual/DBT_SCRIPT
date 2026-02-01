-- created_at: 2026-01-31T23:46:45.775008300+00:00
-- finished_at: 2026-01-31T23:46:45.923239800+00:00
-- elapsed: 148ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c21c12-0001-83ea-0000-118d0089083a
-- desc: list_relations_in_parallel
SHOW OBJECTS IN SCHEMA "CHOP_ANALYTICS"."CORE_SILVER" LIMIT 10000;
-- created_at: 2026-01-31T23:46:46.907434300+00:00
-- finished_at: 2026-01-31T23:46:47.047157100+00:00
-- elapsed: 139ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c21c12-0001-848d-0000-118d008933b6
-- desc: Get table schema
describe table "CHOP_ANALYTICS"."BRONZE"."CLAIMS";
-- created_at: 2026-01-31T23:46:47.055183100+00:00
-- finished_at: 2026-01-31T23:46:47.157629500+00:00
-- elapsed: 102ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c21c12-0001-8449-0000-118d00892772
-- desc: Get table schema
describe table "CHOP_ANALYTICS"."BRONZE"."RX";
-- created_at: 2026-01-31T23:46:48.104360200+00:00
-- finished_at: 2026-01-31T23:46:48.200534200+00:00
-- elapsed: 96ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c21c12-0001-8449-0000-118d00892776
-- desc: execute adapter call
show terse schemas in database CHOP_ANALYTICS
    limit 10000
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","connection_name":""} */;
-- created_at: 2026-01-31T23:46:48.558405800+00:00
-- finished_at: 2026-01-31T23:46:48.721271+00:00
-- elapsed: 162ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.stg_bronze__rx
-- query_id: 01c21c12-0001-848d-0000-118d008933ba
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "CHOP_ANALYTICS"."CORE_CORE_STAGING" LIMIT 10000;
-- created_at: 2026-01-31T23:46:48.723710200+00:00
-- finished_at: 2026-01-31T23:46:48.842281500+00:00
-- elapsed: 118ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.stg_bronze__rx
-- query_id: 01c21c12-0001-8449-0000-118d0089277a
-- desc: execute adapter call
select * from (
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
    ) as __dbt_sbq
    where false
    limit 0
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.stg_bronze__rx"} */;
-- created_at: 2026-01-31T23:46:48.848780800+00:00
-- finished_at: 2026-01-31T23:46:48.981947900+00:00
-- elapsed: 133ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.stg_bronze__claims
-- query_id: 01c21c12-0001-848d-0000-118d008933be
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "CHOP_ANALYTICS"."CORE_CORE_STAGING" LIMIT 10000;
-- created_at: 2026-01-31T23:46:48.845501100+00:00
-- finished_at: 2026-01-31T23:46:49.055722500+00:00
-- elapsed: 210ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.stg_bronze__rx
-- query_id: 01c21c12-0001-83ea-0000-118d0089083e
-- desc: execute adapter call
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
  )
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.stg_bronze__rx"} */;
-- created_at: 2026-01-31T23:46:48.983120800+00:00
-- finished_at: 2026-01-31T23:46:49.089642600+00:00
-- elapsed: 106ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.stg_bronze__claims
-- query_id: 01c21c12-0001-8449-0000-118d0089277e
-- desc: execute adapter call
select * from (
        with src as (
  select * from CHOP_ANALYTICS.BRONZE.claims
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
    ) as __dbt_sbq
    where false
    limit 0
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.stg_bronze__claims"} */;
-- created_at: 2026-01-31T23:46:49.174629800+00:00
-- finished_at: 2026-01-31T23:46:49.286557300+00:00
-- elapsed: 111ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.stg_bronze__labs
-- query_id: 01c21c12-0001-8449-0000-118d00892782
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "CHOP_ANALYTICS"."CORE_CORE_STAGING" LIMIT 10000;
-- created_at: 2026-01-31T23:46:49.091592400+00:00
-- finished_at: 2026-01-31T23:46:49.551100600+00:00
-- elapsed: 459ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.stg_bronze__claims
-- query_id: 01c21c12-0001-83ea-0000-118d00890842
-- desc: execute adapter call
create or replace   view CHOP_ANALYTICS.CORE_CORE_STAGING.stg_bronze__claims
  
    
    
(
  
    "CLAIM_ID" COMMENT $$$$, 
  
    "MEMBER_ID" COMMENT $$$$, 
  
    "PROVIDER_ID" COMMENT $$$$, 
  
    "SERVICE_DATE" COMMENT $$$$, 
  
    "CPT_CODE" COMMENT $$$$, 
  
    "ICD10_CODE" COMMENT $$$$, 
  
    "PAID_AMOUNT" COMMENT $$$$, 
  
    "LOAD_TS_UTC" COMMENT $$$$
  
)

   as (
    with src as (
  select * from CHOP_ANALYTICS.BRONZE.claims
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
  )
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.stg_bronze__claims"} */;
-- created_at: 2026-01-31T23:46:49.289063+00:00
-- finished_at: 2026-01-31T23:46:49.661398300+00:00
-- elapsed: 372ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.stg_bronze__labs
-- query_id: 01c21c12-0001-83ea-0000-118d00890846
-- desc: execute adapter call
select * from (
        with src as (
  select * from CHOP_ANALYTICS.BRONZE.members
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

  
  '019c1673-a977-77a3-b34a-3321813063d3' as dbt_invocation_id

from src
    ) as __dbt_sbq
    where false
    limit 0
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.stg_bronze__labs"} */;
-- created_at: 2026-01-31T23:46:49.663305400+00:00
-- finished_at: 2026-01-31T23:46:49.926873600+00:00
-- elapsed: 263ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.stg_bronze__labs
-- query_id: 01c21c12-0001-848d-0000-118d008933c2
-- desc: execute adapter call
create or replace   view CHOP_ANALYTICS.CORE_CORE_STAGING.stg_bronze__labs
  
    
    
(
  
    "MEMBER_ID" COMMENT $$$$, 
  
    "PLAN_ID" COMMENT $$$$, 
  
    "PROVIDER_ID" COMMENT $$$$, 
  
    "DOB" COMMENT $$$$, 
  
    "GENDER" COMMENT $$$$, 
  
    "COVERAGE_START" COMMENT $$$$, 
  
    "COVERAGE_END" COMMENT $$$$, 
  
    "LOAD_TS_UTC" COMMENT $$$$, 
  
    "DBT_INVOCATION_ID" COMMENT $$$$
  
)

   as (
    with src as (
  select * from CHOP_ANALYTICS.BRONZE.members
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

  
  '019c1673-a977-77a3-b34a-3321813063d3' as dbt_invocation_id

from src
  )
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.stg_bronze__labs"} */;
-- created_at: 2026-01-31T23:46:50.159017100+00:00
-- finished_at: 2026-01-31T23:46:50.296254+00:00
-- elapsed: 137ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.stg_bronze__members
-- query_id: 01c21c12-0001-83ea-0000-118d0089084a
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "CHOP_ANALYTICS"."CORE_CORE_STAGING" LIMIT 10000;
-- created_at: 2026-01-31T23:46:50.297487100+00:00
-- finished_at: 2026-01-31T23:46:50.532105500+00:00
-- elapsed: 234ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.stg_bronze__members
-- query_id: 01c21c12-0001-848d-0000-118d008933c6
-- desc: execute adapter call
select * from (
        with src as (
  select * from CHOP_ANALYTICS.BRONZE.members
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
  
  '019c1673-a977-77a3-b34a-3321813063d3' as dbt_invocation_id

from src
    ) as __dbt_sbq
    where false
    limit 0
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.stg_bronze__members"} */;
-- created_at: 2026-01-31T23:46:50.525740700+00:00
-- finished_at: 2026-01-31T23:46:50.648456200+00:00
-- elapsed: 122ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_member_dim
-- query_id: 01c21c12-0001-83ea-0000-118d0089084e
-- desc: Get table schema
describe table "CHOP_ANALYTICS"."CORE_SILVER"."SILVER_MEMBER_DIM";
-- created_at: 2026-01-31T23:46:50.533881700+00:00
-- finished_at: 2026-01-31T23:46:50.727687200+00:00
-- elapsed: 193ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.stg_bronze__members
-- query_id: 01c21c12-0001-8449-0000-118d00892786
-- desc: execute adapter call
create or replace   view CHOP_ANALYTICS.CORE_CORE_STAGING.stg_bronze__members
  
    
    
(
  
    "MEMBER_ID" COMMENT $$$$, 
  
    "PLAN_ID" COMMENT $$$$, 
  
    "PROVIDER_ID" COMMENT $$$$, 
  
    "DOB" COMMENT $$$$, 
  
    "GENDER" COMMENT $$$$, 
  
    "COVERAGE_START" COMMENT $$$$, 
  
    "COVERAGE_END" COMMENT $$$$, 
  
    "LOAD_TS_UTC" COMMENT $$$$, 
  
    "DBT_INVOCATION_ID" COMMENT $$$$
  
)

   as (
    with src as (
  select * from CHOP_ANALYTICS.BRONZE.members
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
  
  '019c1673-a977-77a3-b34a-3321813063d3' as dbt_invocation_id

from src
  )
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.stg_bronze__members"} */;
-- created_at: 2026-01-31T23:46:50.837141900+00:00
-- finished_at: 2026-01-31T23:46:50.922205800+00:00
-- elapsed: 85ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_events__immunization
-- query_id: 01c21c12-0001-8449-0000-118d0089278a
-- desc: Get table schema
describe table "CHOP_ANALYTICS"."CORE_SILVER"."SILVER_EVENTS__IMMUNIZATION";
-- created_at: 2026-01-31T23:46:51.979731500+00:00
-- finished_at: 2026-01-31T23:46:52.257611500+00:00
-- elapsed: 277ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_member_dim
-- query_id: 01c21c12-0001-83ea-0000-118d00890852
-- desc: execute adapter call
select * from (
        

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
    ) as __dbt_sbq
    where false
    limit 0
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_member_dim"} */;
-- created_at: 2026-01-31T23:46:52.281464900+00:00
-- finished_at: 2026-01-31T23:46:52.462508200+00:00
-- elapsed: 181ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_events__immunization
-- query_id: 01c21c12-0001-8449-0000-118d0089278e
-- desc: execute adapter call
select * from (
        

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
    ) as __dbt_sbq
    where false
    limit 0
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_events__immunization"} */;
-- created_at: 2026-01-31T23:46:52.258535400+00:00
-- finished_at: 2026-01-31T23:46:52.634616400+00:00
-- elapsed: 376ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_member_dim
-- query_id: 01c21c12-0001-848d-0000-118d008933ca
-- desc: execute adapter call
create or replace  temporary view CHOP_ANALYTICS.CORE_SILVER.silver_member_dim__dbt_tmp
  
    
    
(
  
    "MEMBER_ID" COMMENT $$$$, 
  
    "PLAN_ID" COMMENT $$$$, 
  
    "PROVIDER_ID" COMMENT $$$$, 
  
    "DOB" COMMENT $$$$, 
  
    "GENDER" COMMENT $$$$, 
  
    "COVERAGE_START" COMMENT $$$$, 
  
    "COVERAGE_END" COMMENT $$$$, 
  
    "LOAD_TS_UTC" COMMENT $$$$, 
  
    "DBT_INVOCATION_ID" COMMENT $$$$
  
)

   as (
    

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
  )
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_member_dim"} */;
-- created_at: 2026-01-31T23:46:52.463748500+00:00
-- finished_at: 2026-01-31T23:46:52.773549900+00:00
-- elapsed: 309ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_events__immunization
-- query_id: 01c21c12-0001-848d-0000-118d008933ce
-- desc: execute adapter call
create or replace  temporary view CHOP_ANALYTICS.CORE_SILVER.silver_events__immunization__dbt_tmp
  
    
    
(
  
    "EVENT_ID" COMMENT $$$$, 
  
    "MEMBER_ID" COMMENT $$$$, 
  
    "PROVIDER_ID" COMMENT $$$$, 
  
    "EVENT_DATE" COMMENT $$$$, 
  
    "EVENT_TYPE" COMMENT $$$$, 
  
    "EVENT_CODE" COMMENT $$$$, 
  
    "LOAD_TS_UTC" COMMENT $$$$, 
  
    "DBT_INVOCATION_ID" COMMENT $$$$
  
)

   as (
    

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
  )
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_events__immunization"} */;
-- created_at: 2026-01-31T23:46:52.635310800+00:00
-- finished_at: 2026-01-31T23:46:52.774429700+00:00
-- elapsed: 139ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_member_dim
-- query_id: 01c21c12-0001-848d-0000-118d008933d2
-- desc: execute adapter call
describe table CHOP_ANALYTICS.CORE_SILVER.silver_member_dim__dbt_tmp
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_member_dim"} */;
-- created_at: 2026-01-31T23:46:52.775345900+00:00
-- finished_at: 2026-01-31T23:46:52.884573+00:00
-- elapsed: 109ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_member_dim
-- query_id: 01c21c12-0001-83ea-0000-118d00890856
-- desc: execute adapter call
describe table CHOP_ANALYTICS.CORE_SILVER.silver_member_dim
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_member_dim"} */;
-- created_at: 2026-01-31T23:46:52.774427300+00:00
-- finished_at: 2026-01-31T23:46:52.892986900+00:00
-- elapsed: 118ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_events__immunization
-- query_id: 01c21c12-0001-848d-0000-118d008933d6
-- desc: execute adapter call
describe table CHOP_ANALYTICS.CORE_SILVER.silver_events__immunization__dbt_tmp
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_events__immunization"} */;
-- created_at: 2026-01-31T23:46:52.893689+00:00
-- finished_at: 2026-01-31T23:46:53.080192200+00:00
-- elapsed: 186ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_events__immunization
-- query_id: 01c21c12-0001-83ea-0000-118d0089085a
-- desc: execute adapter call
describe table CHOP_ANALYTICS.CORE_SILVER.silver_events__immunization
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_events__immunization"} */;
-- created_at: 2026-01-31T23:46:52.886047200+00:00
-- finished_at: 2026-01-31T23:46:53.080678400+00:00
-- elapsed: 194ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_member_dim
-- query_id: 01c21c12-0001-848d-0000-118d008933da
-- desc: execute adapter call
describe table CHOP_ANALYTICS.CORE_SILVER.SILVER_MEMBER_DIM
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_member_dim"} */;
-- created_at: 2026-01-31T23:46:53.081143900+00:00
-- finished_at: 2026-01-31T23:46:53.171136600+00:00
-- elapsed: 89ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_events__immunization
-- query_id: 01c21c12-0001-8449-0000-118d00892792
-- desc: execute adapter call
describe table CHOP_ANALYTICS.CORE_SILVER.SILVER_EVENTS__IMMUNIZATION
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_events__immunization"} */;
-- created_at: 2026-01-31T23:46:53.083411600+00:00
-- finished_at: 2026-01-31T23:46:53.255946600+00:00
-- elapsed: 172ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_member_dim
-- query_id: 01c21c12-0001-83ea-0000-118d0089085e
-- desc: execute adapter call
-- back compat for old kwarg name
  
  begin
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_member_dim"} */;
-- created_at: 2026-01-31T23:46:53.173300500+00:00
-- finished_at: 2026-01-31T23:46:53.274201800+00:00
-- elapsed: 100ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_events__immunization
-- query_id: 01c21c12-0001-83ea-0000-118d00890862
-- desc: execute adapter call
-- back compat for old kwarg name
  
  begin
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_events__immunization"} */;
-- created_at: 2026-01-31T23:46:53.256162700+00:00
-- finished_at: 2026-01-31T23:46:53.618952+00:00
-- elapsed: 362ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_member_dim
-- query_id: 01c21c12-0001-83ea-0000-118d00890866
-- desc: execute adapter call

    
        
            
	    
	    
            
        
    

    

    merge into CHOP_ANALYTICS.CORE_SILVER.silver_member_dim as DBT_INTERNAL_DEST
        using CHOP_ANALYTICS.CORE_SILVER.silver_member_dim__dbt_tmp as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.member_id = DBT_INTERNAL_DEST.member_id))

    
    when matched then update set
        "MEMBER_ID" = DBT_INTERNAL_SOURCE."MEMBER_ID","PLAN_ID" = DBT_INTERNAL_SOURCE."PLAN_ID","PROVIDER_ID" = DBT_INTERNAL_SOURCE."PROVIDER_ID","DOB" = DBT_INTERNAL_SOURCE."DOB","GENDER" = DBT_INTERNAL_SOURCE."GENDER","COVERAGE_START" = DBT_INTERNAL_SOURCE."COVERAGE_START","COVERAGE_END" = DBT_INTERNAL_SOURCE."COVERAGE_END","LOAD_TS_UTC" = DBT_INTERNAL_SOURCE."LOAD_TS_UTC","DBT_INVOCATION_ID" = DBT_INTERNAL_SOURCE."DBT_INVOCATION_ID"
    

    when not matched then insert
        ("MEMBER_ID", "PLAN_ID", "PROVIDER_ID", "DOB", "GENDER", "COVERAGE_START", "COVERAGE_END", "LOAD_TS_UTC", "DBT_INVOCATION_ID")
    values
        ("MEMBER_ID", "PLAN_ID", "PROVIDER_ID", "DOB", "GENDER", "COVERAGE_START", "COVERAGE_END", "LOAD_TS_UTC", "DBT_INVOCATION_ID")


/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_member_dim"} */;
-- created_at: 2026-01-31T23:46:53.274343700+00:00
-- finished_at: 2026-01-31T23:46:53.671857500+00:00
-- elapsed: 397ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_events__immunization
-- query_id: 01c21c12-0001-848d-0000-118d008933de
-- desc: execute adapter call

    
        
            
	    
	    
            
        
    

    

    merge into CHOP_ANALYTICS.CORE_SILVER.silver_events__immunization as DBT_INTERNAL_DEST
        using CHOP_ANALYTICS.CORE_SILVER.silver_events__immunization__dbt_tmp as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.event_id = DBT_INTERNAL_DEST.event_id))

    
    when matched then update set
        "EVENT_ID" = DBT_INTERNAL_SOURCE."EVENT_ID","MEMBER_ID" = DBT_INTERNAL_SOURCE."MEMBER_ID","PROVIDER_ID" = DBT_INTERNAL_SOURCE."PROVIDER_ID","EVENT_DATE" = DBT_INTERNAL_SOURCE."EVENT_DATE","EVENT_TYPE" = DBT_INTERNAL_SOURCE."EVENT_TYPE","EVENT_CODE" = DBT_INTERNAL_SOURCE."EVENT_CODE","LOAD_TS_UTC" = DBT_INTERNAL_SOURCE."LOAD_TS_UTC","DBT_INVOCATION_ID" = DBT_INTERNAL_SOURCE."DBT_INVOCATION_ID"
    

    when not matched then insert
        ("EVENT_ID", "MEMBER_ID", "PROVIDER_ID", "EVENT_DATE", "EVENT_TYPE", "EVENT_CODE", "LOAD_TS_UTC", "DBT_INVOCATION_ID")
    values
        ("EVENT_ID", "MEMBER_ID", "PROVIDER_ID", "EVENT_DATE", "EVENT_TYPE", "EVENT_CODE", "LOAD_TS_UTC", "DBT_INVOCATION_ID")


/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_events__immunization"} */;
-- created_at: 2026-01-31T23:46:53.619152600+00:00
-- finished_at: 2026-01-31T23:46:53.757498200+00:00
-- elapsed: 138ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_member_dim
-- query_id: 01c21c12-0001-8449-0000-118d00892796
-- desc: execute adapter call

    commit
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_member_dim"} */;
-- created_at: 2026-01-31T23:46:53.672007300+00:00
-- finished_at: 2026-01-31T23:46:53.789332600+00:00
-- elapsed: 117ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_events__immunization
-- query_id: 01c21c12-0001-848d-0000-118d008933e2
-- desc: execute adapter call

    commit
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_events__immunization"} */;
-- created_at: 2026-01-31T23:46:53.758416600+00:00
-- finished_at: 2026-01-31T23:46:53.895788900+00:00
-- elapsed: 137ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_member_dim
-- query_id: 01c21c12-0001-848d-0000-118d008933e6
-- desc: execute adapter call
drop view if exists CHOP_ANALYTICS.CORE_SILVER.silver_member_dim__dbt_tmp cascade
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_member_dim"} */;
-- created_at: 2026-01-31T23:46:53.790436700+00:00
-- finished_at: 2026-01-31T23:46:53.948650600+00:00
-- elapsed: 158ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_events__immunization
-- query_id: 01c21c12-0001-83ea-0000-118d0089086a
-- desc: execute adapter call
drop view if exists CHOP_ANALYTICS.CORE_SILVER.silver_events__immunization__dbt_tmp cascade
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_events__immunization"} */;
-- created_at: 2026-01-31T23:46:54.289860500+00:00
-- finished_at: 2026-01-31T23:46:54.484502900+00:00
-- elapsed: 194ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_gaps__hwdi
-- query_id: 01c21c12-0001-83ea-0000-118d0089086e
-- desc: execute adapter call
select * from (
        



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
    ) as __dbt_sbq
    where false
    limit 0
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_gaps__hwdi"} */;
-- created_at: 2026-01-31T23:46:54.485584400+00:00
-- finished_at: 2026-01-31T23:46:54.776389400+00:00
-- elapsed: 290ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_gaps__hwdi
-- query_id: 01c21c12-0001-848d-0000-118d008933ea
-- desc: execute adapter call
create or replace  temporary view CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi__dbt_tmp
  
    
    
(
  
    "GAP_ID" COMMENT $$$$, 
  
    "MEMBER_ID" COMMENT $$$$, 
  
    "PLAN_ID" COMMENT $$$$, 
  
    "PROVIDER_ID" COMMENT $$$$, 
  
    "MEASUREMENT_YEAR" COMMENT $$$$, 
  
    "MEASURE_ID" COMMENT $$$$, 
  
    "GAP_STATUS" COMMENT $$$$, 
  
    "AS_OF_DATE" COMMENT $$$$, 
  
    "DBT_INVOCATION_ID" COMMENT $$$$
  
)

   as (
    



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
  )
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_gaps__hwdi"} */;
-- created_at: 2026-01-31T23:46:54.777291900+00:00
-- finished_at: 2026-01-31T23:46:54.868728900+00:00
-- elapsed: 91ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_gaps__hwdi
-- query_id: 01c21c12-0001-8449-0000-118d0089279a
-- desc: execute adapter call
describe table CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi__dbt_tmp
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_gaps__hwdi"} */;
-- created_at: 2026-01-31T23:46:54.869472900+00:00
-- finished_at: 2026-01-31T23:46:54.964413600+00:00
-- elapsed: 94ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_gaps__hwdi
-- query_id: 01c21c12-0001-83ea-0000-118d00890872
-- desc: execute adapter call
describe table CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_gaps__hwdi"} */;
-- created_at: 2026-01-31T23:46:54.965192300+00:00
-- finished_at: 2026-01-31T23:46:55.086175600+00:00
-- elapsed: 120ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_gaps__hwdi
-- query_id: 01c21c12-0001-848d-0000-118d008933ee
-- desc: execute adapter call
describe table CHOP_ANALYTICS.CORE_SILVER.SILVER_GAPS__HWDI
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_gaps__hwdi"} */;
-- created_at: 2026-01-31T23:46:55.088927+00:00
-- finished_at: 2026-01-31T23:46:55.202254+00:00
-- elapsed: 113ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_gaps__hwdi
-- query_id: 01c21c12-0001-8449-0000-118d0089279e
-- desc: execute adapter call
-- back compat for old kwarg name
  
  begin
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_gaps__hwdi"} */;
-- created_at: 2026-01-31T23:46:55.202414200+00:00
-- finished_at: 2026-01-31T23:46:55.553625200+00:00
-- elapsed: 351ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_gaps__hwdi
-- query_id: 01c21c12-0001-83ea-0000-118d00890876
-- desc: execute adapter call

    
        
            
	    
	    
            
        
    

    

    merge into CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi as DBT_INTERNAL_DEST
        using CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi__dbt_tmp as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.gap_id = DBT_INTERNAL_DEST.gap_id))

    
    when matched then update set
        "GAP_ID" = DBT_INTERNAL_SOURCE."GAP_ID","MEMBER_ID" = DBT_INTERNAL_SOURCE."MEMBER_ID","PLAN_ID" = DBT_INTERNAL_SOURCE."PLAN_ID","PROVIDER_ID" = DBT_INTERNAL_SOURCE."PROVIDER_ID","MEASUREMENT_YEAR" = DBT_INTERNAL_SOURCE."MEASUREMENT_YEAR","MEASURE_ID" = DBT_INTERNAL_SOURCE."MEASURE_ID","GAP_STATUS" = DBT_INTERNAL_SOURCE."GAP_STATUS","AS_OF_DATE" = DBT_INTERNAL_SOURCE."AS_OF_DATE","DBT_INVOCATION_ID" = DBT_INTERNAL_SOURCE."DBT_INVOCATION_ID"
    

    when not matched then insert
        ("GAP_ID", "MEMBER_ID", "PLAN_ID", "PROVIDER_ID", "MEASUREMENT_YEAR", "MEASURE_ID", "GAP_STATUS", "AS_OF_DATE", "DBT_INVOCATION_ID")
    values
        ("GAP_ID", "MEMBER_ID", "PLAN_ID", "PROVIDER_ID", "MEASUREMENT_YEAR", "MEASURE_ID", "GAP_STATUS", "AS_OF_DATE", "DBT_INVOCATION_ID")


/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_gaps__hwdi"} */;
-- created_at: 2026-01-31T23:46:55.553770600+00:00
-- finished_at: 2026-01-31T23:46:55.739107800+00:00
-- elapsed: 185ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_gaps__hwdi
-- query_id: 01c21c12-0001-848d-0000-118d008933f2
-- desc: execute adapter call

    commit
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_gaps__hwdi"} */;
-- created_at: 2026-01-31T23:46:55.739891700+00:00
-- finished_at: 2026-01-31T23:46:55.864369800+00:00
-- elapsed: 124ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_gaps__hwdi
-- query_id: 01c21c12-0001-8449-0000-118d008927a2
-- desc: execute adapter call
drop view if exists CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi__dbt_tmp cascade
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_gaps__hwdi"} */;
-- created_at: 2026-01-31T23:46:55.865209100+00:00
-- finished_at: 2026-01-31T23:46:55.980030300+00:00
-- elapsed: 114ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_gaps__hwdi
-- query_id: 01c21c12-0001-83ea-0000-118d0089087a
-- desc: execute adapter call
comment on table CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi IS $$Member-level Stars gaps for HWD&I-style measures (example).$$
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_gaps__hwdi"} */;
-- created_at: 2026-01-31T23:46:55.980731200+00:00
-- finished_at: 2026-01-31T23:46:56.112652400+00:00
-- elapsed: 131ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_gaps__hwdi
-- query_id: 01c21c12-0001-848d-0000-118d008933f6
-- desc: execute adapter call
describe table CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_gaps__hwdi"} */;
-- created_at: 2026-01-31T23:46:56.113786900+00:00
-- finished_at: 2026-01-31T23:46:56.246189200+00:00
-- elapsed: 132ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.silver_gaps__hwdi
-- query_id: 01c21c12-0001-8449-0000-118d008927a6
-- desc: execute adapter call
alter  table CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi alter
    
        "GAP_ID" COMMENT $$$$,
    
        "MEMBER_ID" COMMENT $$$$,
    
        "PLAN_ID" COMMENT $$$$,
    
        "PROVIDER_ID" COMMENT $$$$,
    
        "MEASUREMENT_YEAR" COMMENT $$$$,
    
        "MEASURE_ID" COMMENT $$$$,
    
        "GAP_STATUS" COMMENT $$$$,
    
        "AS_OF_DATE" COMMENT $$$$,
    
        "DBT_INVOCATION_ID" COMMENT $$$$
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.silver_gaps__hwdi"} */;
-- created_at: 2026-01-31T23:46:56.250279600+00:00
-- finished_at: 2026-01-31T23:46:56.382467100+00:00
-- elapsed: 132ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.stars_hwdi_dbt.accepted_values_silver_gaps__hwdi_gap_status__OPEN__CLOSED.b253f7cbca
-- query_id: 01c21c12-0001-8449-0000-118d008927aa
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        gap_status as value_field,
        count(*) as n_records

    from CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi
    group by gap_status

)

select *
from all_values
where value_field not in (
    'OPEN','CLOSED'
)



  
  
      
    ) dbt_internal_test
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"test.stars_hwdi_dbt.accepted_values_silver_gaps__hwdi_gap_status__OPEN__CLOSED.b253f7cbca"} */;
-- created_at: 2026-01-31T23:46:56.249820200+00:00
-- finished_at: 2026-01-31T23:46:56.654574300+00:00
-- elapsed: 404ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.stars_hwdi_dbt.not_null_silver_gaps__hwdi_gap_status.9f5583611e
-- query_id: 01c21c12-0001-83ea-0000-118d0089087e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select gap_status
from CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi
where gap_status is null



  
  
      
    ) dbt_internal_test
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"test.stars_hwdi_dbt.not_null_silver_gaps__hwdi_gap_status.9f5583611e"} */;
-- created_at: 2026-01-31T23:46:56.748983300+00:00
-- finished_at: 2026-01-31T23:46:56.911849600+00:00
-- elapsed: 162ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.stars_hwdi_dbt.unique_silver_gaps__hwdi_gap_id.c18cf8b6ea
-- query_id: 01c21c12-0001-848d-0000-118d008933fa
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    gap_id as unique_field,
    count(*) as n_records

from CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi
where gap_id is not null
group by gap_id
having count(*) > 1



  
  
      
    ) dbt_internal_test
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"test.stars_hwdi_dbt.unique_silver_gaps__hwdi_gap_id.c18cf8b6ea"} */;
-- created_at: 2026-01-31T23:46:57.061325+00:00
-- finished_at: 2026-01-31T23:46:57.198815600+00:00
-- elapsed: 137ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.stars_hwdi_dbt.not_null_silver_gaps__hwdi_member_id.4616aa7738
-- query_id: 01c21c12-0001-848d-0000-118d008933fe
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select member_id
from CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi
where member_id is null



  
  
      
    ) dbt_internal_test
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"test.stars_hwdi_dbt.not_null_silver_gaps__hwdi_member_id.4616aa7738"} */;
-- created_at: 2026-01-31T23:46:57.389817600+00:00
-- finished_at: 2026-01-31T23:46:57.513518900+00:00
-- elapsed: 123ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.stars_hwdi_dbt.not_null_silver_gaps__hwdi_gap_id.3068362be9
-- query_id: 01c21c12-0001-8449-0000-118d008927ae
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select gap_id
from CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi
where gap_id is null



  
  
      
    ) dbt_internal_test
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"test.stars_hwdi_dbt.not_null_silver_gaps__hwdi_gap_id.3068362be9"} */;
-- created_at: 2026-01-31T23:46:57.515557200+00:00
-- finished_at: 2026-01-31T23:46:57.631829100+00:00
-- elapsed: 116ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.mart_hwdi_provider_rollup
-- query_id: 01c21c12-0001-8449-0000-118d008927b2
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "CHOP_ANALYTICS"."CORE_GOLD" LIMIT 10000;
-- created_at: 2026-01-31T23:46:57.515556600+00:00
-- finished_at: 2026-01-31T23:46:57.640412300+00:00
-- elapsed: 124ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.mart_hwdi_plan_rollup
-- query_id: 01c21c12-0001-83ea-0000-118d00890882
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "CHOP_ANALYTICS"."CORE_GOLD" LIMIT 10000;
-- created_at: 2026-01-31T23:46:57.641979700+00:00
-- finished_at: 2026-01-31T23:46:58.093892100+00:00
-- elapsed: 451ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.mart_hwdi_plan_rollup
-- query_id: 01c21c12-0001-8449-0000-118d008927b6
-- desc: execute adapter call
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

/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.mart_hwdi_plan_rollup"} */;
-- created_at: 2026-01-31T23:46:57.633757900+00:00
-- finished_at: 2026-01-31T23:46:58.097389500+00:00
-- elapsed: 463ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.stars_hwdi_dbt.mart_hwdi_provider_rollup
-- query_id: 01c21c12-0001-83ea-0000-118d00890886
-- desc: execute adapter call
create or replace transient  table CHOP_ANALYTICS.CORE_GOLD.mart_hwdi_provider_rollup
    
    
    
    as (with g as (
  select * from CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi
)
select
  provider_id,
  measurement_year,
  measure_id,
  count(*) as eligible_members,
  sum(case when gap_status = 'CLOSED' then 1 else 0 end) as closed_members,
  (sum(case when gap_status = 'CLOSED' then 1 else 0 end) / nullif(count(*),0))::float as closure_rate,
  current_timestamp() as mart_load_ts_utc
from g
group by 1,2,3
    )

/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"stars_hwdi","target_name":"dev","node_id":"model.stars_hwdi_dbt.mart_hwdi_provider_rollup"} */;
