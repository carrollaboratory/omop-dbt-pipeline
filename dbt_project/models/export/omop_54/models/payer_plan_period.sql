{{ config(materialized='table') }}

    select
    payer_plan_period_id::integer as "payer_plan_period_id",
    person_id::integer as "person_id",
    payer_plan_period_start_date::text as "payer_plan_period_start_date",
    payer_plan_period_end_date::text as "payer_plan_period_end_date",
    payer_concept_id::integer as "payer_concept_id",
    payer_source_value::text as "payer_source_value",
    payer_source_concept_id::integer as "payer_source_concept_id",
    plan_concept_id::integer as "plan_concept_id",
    plan_source_value::text as "plan_source_value",
    plan_source_concept_id::integer as "plan_source_concept_id",
    sponsor_concept_id::integer as "sponsor_concept_id",
    sponsor_source_value::text as "sponsor_source_value",
    sponsor_source_concept_id::integer as "sponsor_source_concept_id",
    family_source_value::text as "family_source_value",
    stop_reason_concept_id::integer as "stop_reason_concept_id",
    stop_reason_source_value::text as "stop_reason_source_value",
    stop_reason_source_concept_id::integer as "stop_reason_source_concept_id"
    from {{ ref('omop_54_payer_plan_period') }}
    