{{ config(materialized='table') }}

    select
    concept_class_id::text as "concept_class_id",
    concept_class_name::text as "concept_class_name",
    concept_class_concept_id::integer as "concept_class_concept_id"
    from {{ ref('omop_54_concept_class') }}
    