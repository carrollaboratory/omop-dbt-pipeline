{{ config(materialized='table') }}

    select
    source_code::text as "source_code",
    source_concept_id::integer as "source_concept_id",
    source_vocabulary_id::text as "source_vocabulary_id",
    source_code_description::text as "source_code_description",
    target_concept_id::integer as "target_concept_id",
    target_vocabulary_id::text as "target_vocabulary_id",
    valid_start_date::text as "valid_start_date",
    valid_end_date::text as "valid_end_date",
    invalid_reason::text as "invalid_reason"
    from {{ ref('omop_54_source_to_concept_map') }}
    