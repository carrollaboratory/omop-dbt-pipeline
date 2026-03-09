{{ config(materialized='table') }}

    select
    null::integer as "payer_plan_period_id",
    null::integer as "person_id",
    null::text as "payer_plan_period_start_date",
    null::text as "payer_plan_period_end_date",
    null::integer as "payer_concept_id",
    null::text as "payer_source_value",
    null::integer as "payer_source_concept_id",
    null::integer as "plan_concept_id",
    null::text as "plan_source_value",
    null::integer as "plan_source_concept_id",
    null::integer as "sponsor_concept_id",
    null::text as "sponsor_source_value",
    null::integer as "sponsor_source_concept_id",
    null::text as "family_source_value",
    null::integer as "stop_reason_concept_id",
    null::text as "stop_reason_source_value",
    null::integer as "stop_reason_source_concept_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    