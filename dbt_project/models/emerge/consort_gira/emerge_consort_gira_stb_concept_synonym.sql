{{ config(materialized='table') }}

    select
    null::integer as "concept_id",
    null::text as "concept_synonym_name",
    null::integer as "language_concept_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    