{{ config(materialized='table') }}

    select
    ROW_NUMBER() OVER () AS "emerge_index",
    "EMERGE_ID"::text as "emerge_id",
    "AGE_AT_EVENT"::text as "age_at_event",
    "ICD_CODE"::text as "icd_code",
    "ICD_FLAG"::text as "icd_flag",
    "ROW_ID"::text as "row_id",
    "ENCOUNTER_ID"::text as "encounter_id",
    "GIRA_ROR"::text as "gira_ror"
    from {{ source('consort_gira', 'emerge_icd_ex_release_20260129') }}
    