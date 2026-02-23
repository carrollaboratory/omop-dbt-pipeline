{{ config(materialized='table') }}

    select
    episode_id::integer as "episode_id",
    event_id::integer as "event_id",
    episode_event_field_concept_id::integer as "episode_event_field_concept_id"
    from {{ ref('omop_54_episode_event') }}
    