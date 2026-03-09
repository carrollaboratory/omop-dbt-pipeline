{{ config(materialized='table') }}

    select
    null::integer as "condition_era_id",
    null::integer as "person_id",
    null::integer as "condition_concept_id",
    null::text as "condition_era_start_date",
    null::text as "condition_era_end_date",
    null::integer as "condition_occurrence_count"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    