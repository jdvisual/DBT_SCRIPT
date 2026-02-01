
    
    

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


