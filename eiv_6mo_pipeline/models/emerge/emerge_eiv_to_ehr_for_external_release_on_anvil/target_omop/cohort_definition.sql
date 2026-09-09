{{ config(materialized='table', schema = 'omop') }}

with source as (
    select
    null::integer as "cohort_definition_id",
    null::text as "cohort_definition_name",
    null::text as "cohort_definition_description",
    null::integer as "definition_type_concept_id",
    null::text as "cohort_definition_syntax",
    null::integer as "subject_concept_id",
    null::text as "cohort_initiation_date"
    FROM {{ ref('hidden') }}
    
)
select 
    * 
from source
limit 0