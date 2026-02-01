-- back compat for old kwarg name
  
  begin;
    
        
            
	    
	    
            
        
    

    

    merge into CHOP_ANALYTICS.CORE_SILVER.silver_member_dim as DBT_INTERNAL_DEST
        using CHOP_ANALYTICS.CORE_SILVER.silver_member_dim__dbt_tmp as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.member_id = DBT_INTERNAL_DEST.member_id))

    
    when matched then update set
        "MEMBER_ID" = DBT_INTERNAL_SOURCE."MEMBER_ID","PLAN_ID" = DBT_INTERNAL_SOURCE."PLAN_ID","PROVIDER_ID" = DBT_INTERNAL_SOURCE."PROVIDER_ID","DOB" = DBT_INTERNAL_SOURCE."DOB","GENDER" = DBT_INTERNAL_SOURCE."GENDER","COVERAGE_START" = DBT_INTERNAL_SOURCE."COVERAGE_START","COVERAGE_END" = DBT_INTERNAL_SOURCE."COVERAGE_END","LOAD_TS_UTC" = DBT_INTERNAL_SOURCE."LOAD_TS_UTC","DBT_INVOCATION_ID" = DBT_INTERNAL_SOURCE."DBT_INVOCATION_ID"
    

    when not matched then insert
        ("MEMBER_ID", "PLAN_ID", "PROVIDER_ID", "DOB", "GENDER", "COVERAGE_START", "COVERAGE_END", "LOAD_TS_UTC", "DBT_INVOCATION_ID")
    values
        ("MEMBER_ID", "PLAN_ID", "PROVIDER_ID", "DOB", "GENDER", "COVERAGE_START", "COVERAGE_END", "LOAD_TS_UTC", "DBT_INVOCATION_ID")

;
    commit;