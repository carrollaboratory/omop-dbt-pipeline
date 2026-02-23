{{ config(materialized='table') }}

    select
    null::text as "relationship_id",
    null::text as "relationship_name",
    null::text as "is_hierarchical",
    null::text as "defines_ancestry",
    null::text as "reverse_relationship_id",
    null::integer as "relationship_concept_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    