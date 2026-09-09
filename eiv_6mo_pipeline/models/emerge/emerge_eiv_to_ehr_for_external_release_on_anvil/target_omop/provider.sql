{{ config(materialized='table', schema = 'omop') }}
with source as (
    select
    null::integer as "provider_id",
    null::text as "provider_name",
    null::text as "npi",
    null::text as "dea",
    null::integer as "specialty_concept_id",
    null::integer as "care_site_id",
    null::integer as "year_of_birth",
    null::integer as "gender_concept_id",
    null::text as "provider_source_value",
    null::text as "specialty_source_value",
    null::integer as "specialty_source_concept_id",
    null::text as "gender_source_value",
    null::integer as "gender_source_concept_id"
    FROM {{ ref('hidden') }}
    
)
select 
    * 
from source
limit 0