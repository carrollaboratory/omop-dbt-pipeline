{{ config(materialized='table') }}

    select
    null::integer as "cost_id",
    null::integer as "cost_event_id",
    null::text as "cost_domain_id",
    null::integer as "cost_type_concept_id",
    null::integer as "currency_concept_id",
    null::float as "total_charge",
    null::float as "total_cost",
    null::float as "total_paid",
    null::float as "paid_by_payer",
    null::float as "paid_by_patient",
    null::float as "paid_patient_copay",
    null::float as "paid_patient_coinsurance",
    null::float as "paid_patient_deductible",
    null::float as "paid_by_primary",
    null::float as "paid_ingredient_cost",
    null::float as "paid_dispensing_fee",
    null::integer as "payer_plan_period_id",
    null::float as "amount_allowed",
    null::integer as "revenue_code_concept_id",
    null::text as "revenue_code_source_value",
    null::integer as "drg_concept_id",
    null::text as "drg_source_value"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    