{{ config(materialized='table') }}

    select
    concept_id_1::integer as "concept_id_1",
    concept_id_2::integer as "concept_id_2",
    relationship_id::text as "relationship_id",
    valid_start_date::text as "valid_start_date",
    valid_end_date::text as "valid_end_date",
    invalid_reason::text as "invalid_reason"
    from {{ ref('omop_54_concept_relationship') }}
    