{{ config(materialized='table') }}

    select
    location_id::integer as "location_id",
    address_1::text as "address_1",
    address_2::text as "address_2",
    city::text as "city",
    state::text as "state",
    zip::text as "zip",
    county::text as "county",
    location_source_value::text as "location_source_value",
    country_concept_id::integer as "country_concept_id",
    country_source_value::text as "country_source_value",
    latitude::float as "latitude",
    longitude::float as "longitude"
    from {{ ref('omop_54_location') }}
    