{{ config(materialized='table') }}

    select
    cohort_definition_id::integer as "cohort_definition_id",
    cohort_definition_name::text as "cohort_definition_name",
    cohort_definition_description::text as "cohort_definition_description",
    definition_type_concept_id::integer as "definition_type_concept_id",
    cohort_definition_syntax::text as "cohort_definition_syntax",
    subject_concept_id::integer as "subject_concept_id",
    cohort_initiation_date::text as "cohort_initiation_date"
    from {{ ref('omop_54_cohort_definition') }}
    