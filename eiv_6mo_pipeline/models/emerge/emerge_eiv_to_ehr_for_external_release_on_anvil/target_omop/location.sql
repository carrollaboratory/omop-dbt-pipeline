{{ config(materialized='table', schema = 'omop') }}
with source as (
    select
    null::integer as "location_id",
    null::text as "address_1",
    null::text as "address_2",
    null::text as "city",
    null::text as "state",
    null::text as "zip",
    null::text as "county",
    null::text as "location_source_value",
    null::integer as "country_concept_id",
    null::text as "country_source_value",
    null::float as "latitude",
    null::float as "longitude"
    FROM {{ ref('hidden') }}
    
)
select 
    * 
from source
limit 0  