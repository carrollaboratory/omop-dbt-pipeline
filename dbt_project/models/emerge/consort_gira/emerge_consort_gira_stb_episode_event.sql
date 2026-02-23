{{ config(materialized='table') }}

    select
    null::integer as "episode_id",
    null::integer as "event_id",
    null::integer as "episode_event_field_concept_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    