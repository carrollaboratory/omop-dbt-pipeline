{{ config(materialized='table') }}

    select
    null::integer as "note_nlp_id",
    null::integer as "note_id",
    null::integer as "section_concept_id",
    null::text as "snippet",
    null::text as "offset",
    null::text as "lexical_variant",
    null::integer as "note_nlp_concept_id",
    null::integer as "note_nlp_source_concept_id",
    null::text as "nlp_system",
    null::text as "nlp_date",
    null::timestamp as "nlp_datetime",
    null::text as "term_exists",
    null::text as "term_temporal",
    null::text as "term_modifiers"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    