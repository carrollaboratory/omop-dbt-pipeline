{{ config(materialized='table') }}

    select
    null::integer as "concept_id",
    null::text as "concept_name",
    null::text as "domain_id",
    null::text as "vocabulary_id",
    null::text as "concept_class_id",
    null::text as "standard_concept",
    null::text as "concept_code",
    null::text as "valid_start_date",
    null::text as "valid_end_date",
    null::text as "invalid_reason"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    