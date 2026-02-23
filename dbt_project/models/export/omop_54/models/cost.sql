{{ config(materialized='table') }}

    select
    cost_id::integer as "cost_id",
    cost_event_id::integer as "cost_event_id",
    cost_domain_id::text as "cost_domain_id",
    cost_type_concept_id::integer as "cost_type_concept_id",
    currency_concept_id::integer as "currency_concept_id",
    total_charge::float as "total_charge",
    total_cost::float as "total_cost",
    total_paid::float as "total_paid",
    paid_by_payer::float as "paid_by_payer",
    paid_by_patient::float as "paid_by_patient",
    paid_patient_copay::float as "paid_patient_copay",
    paid_patient_coinsurance::float as "paid_patient_coinsurance",
    paid_patient_deductible::float as "paid_patient_deductible",
    paid_by_primary::float as "paid_by_primary",
    paid_ingredient_cost::float as "paid_ingredient_cost",
    paid_dispensing_fee::float as "paid_dispensing_fee",
    payer_plan_period_id::integer as "payer_plan_period_id",
    amount_allowed::float as "amount_allowed",
    revenue_code_concept_id::integer as "revenue_code_concept_id",
    revenue_code_source_value::text as "revenue_code_source_value",
    drg_concept_id::integer as "drg_concept_id",
    drg_source_value::text as "drg_source_value"
    from {{ ref('omop_54_cost') }}
    