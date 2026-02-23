{{ config(materialized='table') }}

    select
    ROW_NUMBER() OVER () AS "emerge_index",
    "EMERGE_ID"::text as "emerge_id",
    "WITHDRAWAL_STATUS"::text as "withdrawal_status",
    "YEAR_OF_BIRTH"::text as "year_of_birth",
    "GENDER_CONCEPT_ID"::text as "gender_concept_id",
    "RACE_CONCEPT_ID"::text as "race_concept_id",
    "ETHNICITY_CONCEPT_ID"::text as "ethnicity_concept_id"
    from {{ source('consort_gira', 'emerge_person_ex_release_20260123') }}
    