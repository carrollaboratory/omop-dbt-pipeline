{{ config(materialized='table') }}

    select
    ROW_NUMBER() OVER () AS "emerge_index",
    "EMERGE_ID"::text as "emerge_id",
    "AGE_AT_EVENT"::text as "age_at_event",
    "CPT_CODE"::text as "cpt_code",
    "ROW_ID"::text as "row_id",
    "ENCOUNTER_ID"::text as "encounter_id",
    "GIRA_ROR"::text as "gira_ror",
    from read_csv('../_study_data/consort_gira/emerge_cpt_ex_release_20260129.csv')