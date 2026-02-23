{{ config(materialized='table') }}

    select
    ROW_NUMBER() OVER () AS "emerge_index",
    "EMERGE_ID"::text as "emerge_id",
    "AGE_AT_EVENT"::text as "age_at_event",
    "CPT_CODE"::text as "cpt_code",
    "ROW_ID"::text as "row_id",
    "ENCOUNTER_ID"::text as "encounter_id",
    "GIRA_ROR"::text as "gira_ror",
    "None"::text as "none",
    "None"::text as "none",
    "Redacted Codes List"::text as "redacted_codes_list"
    from {{ source('consort_gira', 'emerge_cpt_ex_release_20260129') }}
    