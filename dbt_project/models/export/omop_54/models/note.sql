{{ config(materialized='table') }}

    select
    note_id::integer as "note_id",
    person_id::integer as "person_id",
    note_date::text as "note_date",
    note_datetime::timestamp as "note_datetime",
    note_type_concept_id::integer as "note_type_concept_id",
    note_class_concept_id::integer as "note_class_concept_id",
    note_title::text as "note_title",
    note_text::text as "note_text",
    encoding_concept_id::integer as "encoding_concept_id",
    language_concept_id::integer as "language_concept_id",
    provider_id::integer as "provider_id",
    visit_occurrence_id::integer as "visit_occurrence_id",
    visit_detail_id::integer as "visit_detail_id",
    note_source_value::text as "note_source_value",
    note_event_id::integer as "note_event_id",
    note_event_field_concept_id::integer as "note_event_field_concept_id"
    from {{ ref('omop_54_note') }}
    