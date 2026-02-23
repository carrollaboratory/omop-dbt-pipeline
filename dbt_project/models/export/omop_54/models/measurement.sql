{{ config(materialized='table') }}

    select
    measurement_id::integer as "measurement_id",
    person_id::integer as "person_id",
    measurement_concept_id::integer as "measurement_concept_id",
    measurement_date::text as "measurement_date",
    measurement_datetime::timestamp as "measurement_datetime",
    measurement_time::text as "measurement_time",
    measurement_type_concept_id::integer as "measurement_type_concept_id",
    operator_concept_id::integer as "operator_concept_id",
    value_as_number::float as "value_as_number",
    value_as_concept_id::integer as "value_as_concept_id",
    unit_concept_id::integer as "unit_concept_id",
    range_low::float as "range_low",
    range_high::float as "range_high",
    provider_id::integer as "provider_id",
    visit_occurrence_id::integer as "visit_occurrence_id",
    visit_detail_id::integer as "visit_detail_id",
    measurement_source_value::text as "measurement_source_value",
    measurement_source_concept_id::integer as "measurement_source_concept_id",
    unit_source_value::text as "unit_source_value",
    unit_source_concept_id::integer as "unit_source_concept_id",
    value_source_value::text as "value_source_value",
    measurement_event_id::integer as "measurement_event_id",
    meas_event_field_concept_id::integer as "meas_event_field_concept_id"
    from {{ ref('omop_54_measurement') }}
    