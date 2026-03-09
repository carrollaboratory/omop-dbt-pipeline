{{ config(materialized='table') }}

    select
    null::integer as "cohort_definition_id",
    null::integer as "subject_id",
    null::text as "cohort_start_date",
    null::text as "cohort_end_date"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    