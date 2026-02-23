{{ config(materialized='table') }}

    select
    vocabulary_id::text as "vocabulary_id",
    vocabulary_name::text as "vocabulary_name",
    vocabulary_reference::text as "vocabulary_reference",
    vocabulary_version::text as "vocabulary_version",
    vocabulary_concept_id::integer as "vocabulary_concept_id"
    from {{ ref('omop_54_vocabulary') }}
    