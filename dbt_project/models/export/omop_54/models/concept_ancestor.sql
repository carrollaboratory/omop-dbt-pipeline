{{ config(materialized='table') }}

    select
    ancestor_concept_id::integer as "ancestor_concept_id",
    descendant_concept_id::integer as "descendant_concept_id",
    min_levels_of_separation::integer as "min_levels_of_separation",
    max_levels_of_separation::integer as "max_levels_of_separation"
    from {{ ref('omop_54_concept_ancestor') }}
    