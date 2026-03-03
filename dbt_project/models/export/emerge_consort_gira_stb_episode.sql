{{ config(materialized='table') }}

    select
    null::integer as "episode_id",
    null::integer as "person_id",
    null::integer as "episode_concept_id",
    null::text as "episode_start_date",
    null::timestamp as "episode_start_datetime",
    null::text as "episode_end_date",
    null::timestamp as "episode_end_datetime",
    null::integer as "episode_parent_id",
    null::integer as "episode_number",
    null::integer as "episode_object_concept_id",
    null::integer as "episode_type_concept_id",
    null::text as "episode_source_value",
    null::integer as "episode_source_concept_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    