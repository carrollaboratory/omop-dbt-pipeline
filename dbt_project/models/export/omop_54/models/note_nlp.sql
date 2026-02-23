{{ config(materialized='table') }}

    select
    note_nlp_id::integer as "note_nlp_id",
    note_id::integer as "note_id",
    section_concept_id::integer as "section_concept_id",
    snippet::text as "snippet",
    offset::text as "offset",
    lexical_variant::text as "lexical_variant",
    note_nlp_concept_id::integer as "note_nlp_concept_id",
    note_nlp_source_concept_id::integer as "note_nlp_source_concept_id",
    nlp_system::text as "nlp_system",
    nlp_date::text as "nlp_date",
    nlp_datetime::timestamp as "nlp_datetime",
    term_exists::text as "term_exists",
    term_temporal::text as "term_temporal",
    term_modifiers::text as "term_modifiers"
    from {{ ref('omop_54_note_nlp') }}
    