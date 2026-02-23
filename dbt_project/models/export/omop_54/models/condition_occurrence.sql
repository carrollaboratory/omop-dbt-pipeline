{{ config(materialized='table') }}

    select
    condition_occurrence_id::integer as "condition_occurrence_id",
    person_id::integer as "person_id",
    condition_concept_id::integer as "condition_concept_id",
    condition_start_date::text as "condition_start_date",
    condition_start_datetime::timestamp as "condition_start_datetime",
    condition_end_date::text as "condition_end_date",
    condition_end_datetime::timestamp as "condition_end_datetime",
    condition_type_concept_id::integer as "condition_type_concept_id",
    condition_status_concept_id::integer as "condition_status_concept_id",
    stop_reason::text as "stop_reason",
    provider_id::integer as "provider_id",
    visit_occurrence_id::integer as "visit_occurrence_id",
    visit_detail_id::integer as "visit_detail_id",
    condition_source_value::text as "condition_source_value",
    condition_source_concept_id::integer as "condition_source_concept_id",
    condition_status_source_value::text as "condition_status_source_value"
    from {{ ref('omop_54_condition_occurrence') }}
    