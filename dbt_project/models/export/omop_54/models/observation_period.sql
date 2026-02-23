{{ config(materialized='table') }}

    select
    observation_period_id::integer as "observation_period_id",
    person_id::integer as "person_id",
    observation_period_start_date::text as "observation_period_start_date",
    observation_period_end_date::text as "observation_period_end_date",
    period_type_concept_id::integer as "period_type_concept_id"
    from {{ ref('omop_54_observation_period') }}
    