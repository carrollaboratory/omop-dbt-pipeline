{{ config(materialized='table') }}

    select
    cohort_definition_id::integer as "cohort_definition_id",
    subject_id::integer as "subject_id",
    cohort_start_date::text as "cohort_start_date",
    cohort_end_date::text as "cohort_end_date"
    from {{ ref('omop_54_cohort') }}
    