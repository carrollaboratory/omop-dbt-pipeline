{{ config(materialized='table', schema = 'omop') }}
with source as (
    select
    null::text as "source_code",
    null::integer as "source_concept_id",
    null::text as "source_vocabulary_id",
    null::text as "source_code_description",
    null::integer as "target_concept_id",
    null::text as "target_vocabulary_id",
    null::text as "valid_start_date",
    null::text as "valid_end_date",
    null::text as "invalid_reason"
    FROM {{ ref('hidden') }}
    
)
select 
    * 
from source
limit 0