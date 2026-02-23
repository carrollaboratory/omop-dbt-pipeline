{{ config(materialized='table') }}

    select
    drug_concept_id::integer as "drug_concept_id",
    ingredient_concept_id::integer as "ingredient_concept_id",
    amount_value::float as "amount_value",
    amount_unit_concept_id::integer as "amount_unit_concept_id",
    numerator_value::float as "numerator_value",
    numerator_unit_concept_id::integer as "numerator_unit_concept_id",
    denominator_value::float as "denominator_value",
    denominator_unit_concept_id::integer as "denominator_unit_concept_id",
    box_size::integer as "box_size",
    valid_start_date::text as "valid_start_date",
    valid_end_date::text as "valid_end_date",
    invalid_reason::text as "invalid_reason"
    from {{ ref('omop_54_drug_strength') }}
    