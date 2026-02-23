{{ config(materialized='table') }}

    select
    null::integer as "observation_id",
    null::integer as "person_id",
    null::integer as "observation_concept_id",
    null::text as "observation_date",
    null::timestamp as "observation_datetime",
    null::integer as "observation_type_concept_id",
    null::float as "value_as_number",
    null::text as "value_as_string",
    null::integer as "value_as_concept_id",
    null::integer as "qualifier_concept_id",
    null::integer as "unit_concept_id",
    null::integer as "provider_id",
    null::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "observation_source_value",
    null::integer as "observation_source_concept_id",
    null::text as "unit_source_value",
    null::text as "qualifier_source_value",
    null::text as "value_source_value",
    null::integer as "observation_event_id",
    null::integer as "obs_event_field_concept_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    