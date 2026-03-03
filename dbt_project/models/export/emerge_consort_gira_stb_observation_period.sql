{{ config(materialized='table') }}

    select
    null::integer as "observation_period_id",
    null::integer as "person_id",
    null::text as "observation_period_start_date",
    null::text as "observation_period_end_date",
    null::integer as "period_type_concept_id"
    from (SELECT * FROM {{ ref('emerge_consort_gira_int_bmi') }} WHERE domain_id = 'Observation')

    union all

    select
    null::integer as "observation_period_id",
    null::integer as "person_id",
    null::text as "observation_period_start_date",
    null::text as "observation_period_end_date",
    null::integer as "period_type_concept_id"
    from (SELECT * FROM {{ ref('emerge_consort_gira_int_cpt') }} WHERE domain_id = 'Observation')
    