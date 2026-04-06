
    
    

select
    site_id as unique_field,
    count(*) as n_records

from "dbt"."main_seeds"."care_site_seed"
where site_id is not null
group by site_id
having count(*) > 1


