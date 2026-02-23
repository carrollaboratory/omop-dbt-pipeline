{{ config(materialized='table') }}

    select
    person_id::integer as "person_id",
    gender_concept_id::integer as "gender_concept_id",
    year_of_birth::integer as "year_of_birth",
    month_of_birth::integer as "month_of_birth",
    day_of_birth::integer as "day_of_birth",
    birth_datetime::timestamp as "birth_datetime",
    race_concept_id::integer as "race_concept_id",
    ethnicity_concept_id::integer as "ethnicity_concept_id",
    location_id::integer as "location_id",
    provider_id::integer as "provider_id",
    care_site_id::integer as "care_site_id",
    person_source_value::text as "person_source_value",
    gender_source_value::text as "gender_source_value",
    gender_source_concept_id::integer as "gender_source_concept_id",
    race_source_value::text as "race_source_value",
    race_source_concept_id::integer as "race_source_concept_id",
    ethnicity_source_value::text as "ethnicity_source_value",
    ethnicity_source_concept_id::integer as "ethnicity_source_concept_id"
    from {{ ref('omop_54_person') }}
    