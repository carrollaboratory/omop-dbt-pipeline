{{ config(materialized='table', schema = 'omop') }}


-- BMI don't have observations at this time. We can decide whether we want to plan for them.
--     select
--     null::integer as "observation_period_id",
--     null::integer as "person_id",
--     null::text as "observation_period_start_date",
--     null::text as "observation_period_end_date",
--     null::integer as "period_type_concept_id"
--     from (SELECT * FROM  WHERE domain_id = 'Observation') 

--     union all

    select
    null::integer as "observation_period_id",
    null::integer as "person_id",
    null::text as "observation_period_start_date",
    null::text as "observation_period_end_date",
    null::integer as "period_type_concept_id"
    -- from (SELECT * FROM {{ ref('emerge_consort_gira_int_cpt_observations') }} WHERE domain_id = 'Observation')
    