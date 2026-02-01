# stars_hwdi_dbt

Bronze → Silver → Gold dbt Core project (Snowflake) for Stars HWD&I-style measures.

## Quickstart
1) Set env vars for Snowflake creds (see profiles.yml.example)
2) Install dbt-snowflake
3) Run:
   - dbt deps
   - dbt debug
   - dbt build --vars '{"measurement_year": 2025}'
