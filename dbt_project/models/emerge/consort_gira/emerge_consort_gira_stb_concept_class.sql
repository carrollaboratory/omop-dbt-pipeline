{{ config(materialized='table') }}

    select
    null::text as "concept_class_id",
    null::text as "concept_class_name",
    null::integer as "concept_class_concept_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    