{{ config(materialized='table', schema = 'omop') }}
with source as (
    select
    null::integer as "metadata_concept_id",
    null::integer as "metadata_type_concept_id",
    null::text as "name",
    null::text as "value_as_string",
    null::integer as "value_as_concept_id",
    null::text as "metadata_date",
    null::timestamp as "metadata_datetime"
    FROM {{ ref('hidden') }}
    
)
select 
    * 
from source
limit 0    