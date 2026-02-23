{{ config(materialized='table') }}

    select
    null::integer as "person_id",
    null::integer as "gender_concept_id",
    null::integer as "year_of_birth",
    null::integer as "month_of_birth",
    null::integer as "day_of_birth",
    null::timestamp as "birth_datetime",
    null::integer as "race_concept_id",
    null::integer as "ethnicity_concept_id",
    null::integer as "location_id",
    null::integer as "provider_id",
    null::integer as "care_site_id",
    null::text as "person_source_value",
    null::text as "gender_source_value",
    null::integer as "gender_source_concept_id",
    null::text as "race_source_value",
    null::integer as "race_source_concept_id",
    null::text as "ethnicity_source_value",
    null::integer as "ethnicity_source_concept_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    