{{ config(materialized='table') }}

    select
    null::integer as "note_id",
    null::integer as "person_id",
    null::text as "note_date",
    null::timestamp as "note_datetime",
    null::integer as "note_type_concept_id",
    null::integer as "note_class_concept_id",
    null::text as "note_title",
    null::text as "note_text",
    null::integer as "encoding_concept_id",
    null::integer as "language_concept_id",
    null::integer as "provider_id",
    null::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "note_source_value",
    null::integer as "note_event_id",
    null::integer as "note_event_field_concept_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    