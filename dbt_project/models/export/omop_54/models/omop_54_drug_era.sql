{{ config(materialized='table') }}

    select
    drug_era_id::integer as "drug_era_id",
    person_id::integer as "person_id",
    drug_concept_id::integer as "drug_concept_id",
    drug_era_start_date::text as "drug_era_start_date",
    drug_era_end_date::text as "drug_era_end_date",
    drug_exposure_count::integer as "drug_exposure_count",
    gap_days::integer as "gap_days"
    from {{ ref('omop_54_drug_era') }}
    