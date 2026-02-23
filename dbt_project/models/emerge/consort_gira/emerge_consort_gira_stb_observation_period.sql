{{ config(materialized='table') }}

    select
    null::integer as "observation_period_id",
    null::integer as "person_id",
    null::text as "observation_period_start_date",
    null::text as "observation_period_end_date",
    null::integer as "period_type_concept_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    