{{ config(materialized='table') }}

    select
    emerge_id::integer as "person_id",
    s_gender_concept_id::integer as "gender_concept_id",
    year_of_birth::integer as "year_of_birth",
    '06'::integer as "month_of_birth",
    '15'::integer as "day_of_birth",
    null::timestamp as "birth_datetime",
    s_race_concept_id::integer as "race_concept_id",
    s_ethnicity_concept_id::integer as "ethnicity_concept_id",
    null::integer as "location_id",
    null::integer as "provider_id",
    {{ generate_key('none', 'consort_gira', "substring(emerge_id, 1, 2)") }}::integer as "care_site_id",
    null::text as "person_source_value",
    gender_concept_id::text as "gender_source_value",
    gender_concept_id::integer as "gender_source_concept_id",
    null::text as "race_source_value",
    null::integer as "race_source_concept_id",
    null::text as "ethnicity_source_value",
    null::integer as "ethnicity_source_concept_id",
    from {{ ref('emerge_consort_gira_int_person_persons') }}