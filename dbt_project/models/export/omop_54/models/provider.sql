{{ config(materialized='table') }}

    select
    provider_id::integer as "provider_id",
    provider_name::text as "provider_name",
    npi::text as "npi",
    dea::text as "dea",
    specialty_concept_id::integer as "specialty_concept_id",
    care_site_id::integer as "care_site_id",
    year_of_birth::integer as "year_of_birth",
    gender_concept_id::integer as "gender_concept_id",
    provider_source_value::text as "provider_source_value",
    specialty_source_value::text as "specialty_source_value",
    specialty_source_concept_id::integer as "specialty_source_concept_id",
    gender_source_value::text as "gender_source_value",
    gender_source_concept_id::integer as "gender_source_concept_id"
    from {{ ref('omop_54_provider') }}
    