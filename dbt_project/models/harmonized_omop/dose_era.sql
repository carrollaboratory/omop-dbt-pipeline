{{ config(materialized='table', schema = 'omop') }}

    select
    null::integer as "dose_era_id",
    null::integer as "person_id",
    null::integer as "drug_concept_id",
    null::integer as "unit_concept_id",
    null::float as "dose_value",
    null::text as "dose_era_start_date",
    null::text as "dose_era_end_date"
    