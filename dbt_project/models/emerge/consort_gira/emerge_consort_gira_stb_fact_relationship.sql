{{ config(materialized='table') }}

    select
    null::integer as "domain_concept_id_1",
    null::integer as "fact_id_1",
    null::integer as "domain_concept_id_2",
    null::integer as "fact_id_2",
    null::integer as "relationship_concept_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    