{{ config(materialized='table') }}

    select
    relationship_id::text as "relationship_id",
    relationship_name::text as "relationship_name",
    is_hierarchical::text as "is_hierarchical",
    defines_ancestry::text as "defines_ancestry",
    reverse_relationship_id::text as "reverse_relationship_id",
    relationship_concept_id::integer as "relationship_concept_id"
    from {{ ref('omop_54_relationship') }}
    