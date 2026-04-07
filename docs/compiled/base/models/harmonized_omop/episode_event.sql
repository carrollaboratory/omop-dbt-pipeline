
with source as (
    select
    null::integer as "episode_id",
    null::integer as "event_id",
    null::integer as "episode_event_field_concept_id"
    FROM "dbt"."main"."hidden"
    
)
select 
    * 
from source
limit 0