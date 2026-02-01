-- back compat for old kwarg name
  
  begin;
    
        
            
	    
	    
            
        
    

    

    merge into CHOP_ANALYTICS.CORE_SILVER.silver_events__immunization as DBT_INTERNAL_DEST
        using CHOP_ANALYTICS.CORE_SILVER.silver_events__immunization__dbt_tmp as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.event_id = DBT_INTERNAL_DEST.event_id))

    
    when matched then update set
        "EVENT_ID" = DBT_INTERNAL_SOURCE."EVENT_ID","MEMBER_ID" = DBT_INTERNAL_SOURCE."MEMBER_ID","PROVIDER_ID" = DBT_INTERNAL_SOURCE."PROVIDER_ID","EVENT_DATE" = DBT_INTERNAL_SOURCE."EVENT_DATE","EVENT_TYPE" = DBT_INTERNAL_SOURCE."EVENT_TYPE","EVENT_CODE" = DBT_INTERNAL_SOURCE."EVENT_CODE","LOAD_TS_UTC" = DBT_INTERNAL_SOURCE."LOAD_TS_UTC","DBT_INVOCATION_ID" = DBT_INTERNAL_SOURCE."DBT_INVOCATION_ID"
    

    when not matched then insert
        ("EVENT_ID", "MEMBER_ID", "PROVIDER_ID", "EVENT_DATE", "EVENT_TYPE", "EVENT_CODE", "LOAD_TS_UTC", "DBT_INVOCATION_ID")
    values
        ("EVENT_ID", "MEMBER_ID", "PROVIDER_ID", "EVENT_DATE", "EVENT_TYPE", "EVENT_CODE", "LOAD_TS_UTC", "DBT_INVOCATION_ID")

;
    commit;