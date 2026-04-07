
with source as (
    select
    null::integer as "cohort_definition_id",
    null::integer as "subject_id",
    null::text as "cohort_start_date",
    null::text as "cohort_end_date"
    FROM "dbt"."main"."hidden"
    
)
select 
    * 
from source
limit 0