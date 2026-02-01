
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select gap_id
from CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi
where gap_id is null



  
  
      
    ) dbt_internal_test