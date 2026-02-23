{{ config(materialized='table') }}

    select
    null::text as "domain_id",
    null::text as "domain_name",
    null::integer as "domain_concept_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    