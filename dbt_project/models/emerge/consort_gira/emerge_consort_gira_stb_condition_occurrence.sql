{{ config(materialized='table') }}

    select
    null::integer as "condition_occurrence_id",
    null::integer as "person_id",
    null::integer as "condition_concept_id",
    null::text as "condition_start_date",
    null::timestamp as "condition_start_datetime",
    null::text as "condition_end_date",
    null::timestamp as "condition_end_datetime",
    null::integer as "condition_type_concept_id",
    null::integer as "condition_status_concept_id",
    null::text as "stop_reason",
    null::integer as "provider_id",
    null::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "condition_source_value",
    null::integer as "condition_source_concept_id",
    null::text as "condition_status_source_value"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    