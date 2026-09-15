{{ config(materialized='table', schema = 'omop') }}
with source as (
    select
    null::integer as "domain_concept_id_1",
    null::integer as "fact_id_1",
    null::integer as "domain_concept_id_2",
    null::integer as "fact_id_2",
    null::integer as "relationship_concept_id"
    FROM {{ ref('hidden') }}
    
)
select 
    * 
from source
limit 0    