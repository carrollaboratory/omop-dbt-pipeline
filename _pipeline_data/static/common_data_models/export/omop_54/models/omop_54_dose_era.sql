{{ config(materialized='table') }}

    select
    dose_era_id::integer as "dose_era_id",
    person_id::integer as "person_id",
    drug_concept_id::integer as "drug_concept_id",
    unit_concept_id::integer as "unit_concept_id",
    dose_value::float as "dose_value",
    dose_era_start_date::text as "dose_era_start_date",
    dose_era_end_date::text as "dose_era_end_date"
    from {{ ref('omop_54_dose_era') }}
    