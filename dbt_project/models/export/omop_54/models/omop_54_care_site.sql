{{ config(materialized='table') }}

    select
    care_site_id::integer as "care_site_id",
    care_site_name::text as "care_site_name",
    place_of_service_concept_id::integer as "place_of_service_concept_id",
    location_id::integer as "location_id",
    care_site_source_value::text as "care_site_source_value",
    place_of_service_source_value::text as "place_of_service_source_value"
    from {{ ref('omop_54_care_site') }}
    