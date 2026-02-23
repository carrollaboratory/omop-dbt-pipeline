{{ config(materialized='table') }}

    select
    null::text as "vocabulary_id",
    null::text as "vocabulary_name",
    null::text as "vocabulary_reference",
    null::text as "vocabulary_version",
    null::integer as "vocabulary_concept_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    