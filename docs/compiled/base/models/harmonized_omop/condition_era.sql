
with source as (
    select
    null::integer as "condition_era_id",
    null::integer as "person_id",
    null::integer as "condition_concept_id",
    null::text as "condition_era_start_date",
    null::text as "condition_era_end_date",
    null::integer as "condition_occurrence_count"
    FROM "dbt"."main"."hidden"
    
)
select 
    * 
from source
limit 0