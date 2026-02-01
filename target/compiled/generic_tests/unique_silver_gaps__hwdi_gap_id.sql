
    
    

select
    gap_id as unique_field,
    count(*) as n_records

from CHOP_ANALYTICS.CORE_SILVER.silver_gaps__hwdi
where gap_id is not null
group by gap_id
having count(*) > 1


