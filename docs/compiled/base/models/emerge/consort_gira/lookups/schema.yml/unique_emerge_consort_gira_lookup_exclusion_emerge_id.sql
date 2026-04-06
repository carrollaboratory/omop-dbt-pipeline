
    
    

select
    emerge_id as unique_field,
    count(*) as n_records

from "dbt"."main"."emerge_consort_gira_lookup_exclusion"
where emerge_id is not null
group by emerge_id
having count(*) > 1


