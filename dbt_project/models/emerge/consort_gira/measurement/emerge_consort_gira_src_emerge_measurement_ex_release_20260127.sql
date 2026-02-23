{{ config(materialized='table') }}

    select
    ROW_NUMBER() OVER () AS "emerge_index",
    "EMERGE_ID"::text as "emerge_id",
    "MEASUREMENT_CONCEPT_ID"::text as "measurement_concept_id",
    "VALUE_AS_NUMBER"::text as "value_as_number",
    "VALUE_AS_TEXT"::text as "value_as_text",
    "RANGE_LOW"::text as "range_low",
    "RANGE_HIGH"::text as "range_high",
    "RANGE_FLAG"::text as "range_flag",
    "UNIT_CONCEPT_ID"::text as "unit_concept_id",
    "UNIT_CONCEPT_AS_TEXT"::text as "unit_concept_as_text",
    "ROW_ID"::text as "row_id",
    "ENCOUNTER_ID"::text as "encounter_id",
    "GIRA_ROR"::text as "gira_ror"
    from {{ source('consort_gira', 'emerge_measurement_ex_release_20260127') }}
    