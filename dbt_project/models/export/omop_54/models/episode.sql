{{ config(materialized='table') }}

    select
    episode_id::integer as "episode_id",
    person_id::integer as "person_id",
    episode_concept_id::integer as "episode_concept_id",
    episode_start_date::text as "episode_start_date",
    episode_start_datetime::timestamp as "episode_start_datetime",
    episode_end_date::text as "episode_end_date",
    episode_end_datetime::timestamp as "episode_end_datetime",
    episode_parent_id::integer as "episode_parent_id",
    episode_number::integer as "episode_number",
    episode_object_concept_id::integer as "episode_object_concept_id",
    episode_type_concept_id::integer as "episode_type_concept_id",
    episode_source_value::text as "episode_source_value",
    episode_source_concept_id::integer as "episode_source_concept_id"
    from {{ ref('omop_54_episode') }}
    