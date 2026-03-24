{{ config(materialized='table', schema = 'omop') }}

    select
    null::integer as "episode_id",
    null::integer as "event_id",
    null::integer as "episode_event_field_concept_id"
