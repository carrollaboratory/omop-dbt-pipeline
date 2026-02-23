{{ config(materialized='table') }}

    select
    concept_id::integer as "concept_id",
    concept_name::text as "concept_name",
    domain_id::text as "domain_id",
    vocabulary_id::text as "vocabulary_id",
    concept_class_id::text as "concept_class_id",
    standard_concept::text as "standard_concept",
    concept_code::text as "concept_code",
    valid_start_date::text as "valid_start_date",
    valid_end_date::text as "valid_end_date",
    invalid_reason::text as "invalid_reason"
    from {{ ref('omop_54_concept') }}
    