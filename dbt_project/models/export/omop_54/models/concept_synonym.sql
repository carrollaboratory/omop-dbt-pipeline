{{ config(materialized='table') }}

    select
    concept_id::integer as "concept_id",
    concept_synonym_name::text as "concept_synonym_name",
    language_concept_id::integer as "language_concept_id"
    from {{ ref('omop_54_concept_synonym') }}
    