{{ config(materialized='table') }}

    select
    null::integer as "ancestor_concept_id",
    null::integer as "descendant_concept_id",
    null::integer as "min_levels_of_separation",
    null::integer as "max_levels_of_separation"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    