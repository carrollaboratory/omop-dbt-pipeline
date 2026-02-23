{{ config(materialized='table') }}

    select
    condition_era_id::integer as "condition_era_id",
    person_id::integer as "person_id",
    condition_concept_id::integer as "condition_concept_id",
    condition_era_start_date::text as "condition_era_start_date",
    condition_era_end_date::text as "condition_era_end_date",
    condition_occurrence_count::integer as "condition_occurrence_count"
    from {{ ref('omop_54_condition_era') }}
    