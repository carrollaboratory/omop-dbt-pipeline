{{ config(materialized='table') }}

    select
    observation_id::integer as "observation_id",
    person_id::integer as "person_id",
    observation_concept_id::integer as "observation_concept_id",
    observation_date::text as "observation_date",
    observation_datetime::timestamp as "observation_datetime",
    observation_type_concept_id::integer as "observation_type_concept_id",
    value_as_number::float as "value_as_number",
    value_as_string::text as "value_as_string",
    value_as_concept_id::integer as "value_as_concept_id",
    qualifier_concept_id::integer as "qualifier_concept_id",
    unit_concept_id::integer as "unit_concept_id",
    provider_id::integer as "provider_id",
    visit_occurrence_id::integer as "visit_occurrence_id",
    visit_detail_id::integer as "visit_detail_id",
    observation_source_value::text as "observation_source_value",
    observation_source_concept_id::integer as "observation_source_concept_id",
    unit_source_value::text as "unit_source_value",
    qualifier_source_value::text as "qualifier_source_value",
    value_source_value::text as "value_source_value",
    observation_event_id::integer as "observation_event_id",
    obs_event_field_concept_id::integer as "obs_event_field_concept_id"
    from {{ ref('omop_54_observation') }}
    