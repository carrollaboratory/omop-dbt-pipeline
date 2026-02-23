{{ config(materialized='table') }}

    select
    null::integer as "drug_concept_id",
    null::integer as "ingredient_concept_id",
    null::float as "amount_value",
    null::integer as "amount_unit_concept_id",
    null::float as "numerator_value",
    null::integer as "numerator_unit_concept_id",
    null::float as "denominator_value",
    null::integer as "denominator_unit_concept_id",
    null::integer as "box_size",
    null::text as "valid_start_date",
    null::text as "valid_end_date",
    null::text as "invalid_reason"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    