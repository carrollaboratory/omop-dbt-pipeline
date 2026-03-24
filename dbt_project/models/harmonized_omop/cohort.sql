{{ config(materialized='table', schema = 'omop') }}

    select
    null::integer as "cohort_definition_id",
    null::integer as "subject_id",
    null::text as "cohort_start_date",
    null::text as "cohort_end_date"
    