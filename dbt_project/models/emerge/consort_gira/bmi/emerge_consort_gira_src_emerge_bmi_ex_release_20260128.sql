{{ config(materialized='table') }}

    select
    ROW_NUMBER() OVER () AS "emerge_index",
    "EMERGE_ID"::text as "emerge_id",
    "AGE_AT_EVENT"::text as "age_at_event",
    "MEASUREMENT_CONCEPT_ID"::text as "measurement_concept_id",
    "MEASUREMENT_CONCEPT_NAME"::text as "measurement_concept_name",
    "VALUE_AS_NUMBER"::text as "value_as_number",
    "UNIT_CONCEPT_ID"::text as "unit_concept_id",
    "UNIT_CONCEPT_NAME"::text as "unit_concept_name",
    "BMI_Z_SCORE"::text as "bmi_z_score",
    "ROW_ID"::text as "row_id",
    "ENCOUNTER_ID"::text as "encounter_id",
    "GIRA_ROR"::text as "gira_ror"
    from {{ source('consort_gira', 'emerge_bmi_ex_release_20260128') }}
    