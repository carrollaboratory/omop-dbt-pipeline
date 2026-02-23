{{ config(materialized='table') }}

    select
    null::integer as "concept_id_1",
    null::integer as "concept_id_2",
    null::text as "relationship_id",
    null::text as "valid_start_date",
    null::text as "valid_end_date",
    null::text as "invalid_reason"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    