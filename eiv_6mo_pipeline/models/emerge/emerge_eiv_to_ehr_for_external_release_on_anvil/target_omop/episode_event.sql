{{ config(materialized='table', schema = 'omop') }}
with source as (
    select
    null::integer as "episode_id",
    null::integer as "event_id",
    null::integer as "episode_event_field_concept_id"
    FROM {{ ref('hidden') }}
    
)
select 
    * 
from source
limit 0