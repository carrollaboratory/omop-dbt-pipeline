
    

    create  table
      "dbt"."dev_202609_omop"."episode_event__dbt_tmp"
  
    
    as (
      
with source as (
    select
    null::integer as "episode_id",
    null::integer as "event_id",
    null::integer as "episode_event_field_concept_id"
    FROM "dbt"."dev_202609_vocab"."hidden"
    
)
select 
    * 
from source
limit 0
    );
    
  