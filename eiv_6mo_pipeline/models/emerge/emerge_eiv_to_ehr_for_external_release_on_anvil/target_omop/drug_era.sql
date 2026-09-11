{{ config(materialized='table', schema = 'omop') }}
with source as (
    select
    null::integer as "drug_era_id",
    null::integer as "person_id",
    null::integer as "drug_concept_id",
    null::text as "drug_era_start_date",
    null::text as "drug_era_end_date",
    null::integer as "drug_exposure_count",
    null::integer as "gap_days"
    FROM {{ ref('hidden') }}
    
)
select 
    * 
from source
limit 0
    