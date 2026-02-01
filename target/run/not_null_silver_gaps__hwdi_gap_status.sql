
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select gap_status
from CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi
where gap_status is null



  
  
      
    ) dbt_internal_test