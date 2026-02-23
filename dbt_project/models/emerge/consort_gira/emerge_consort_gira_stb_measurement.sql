{{ config(materialized='table') }}

    select
    null::integer as "measurement_id",
    null::integer as "person_id",
    null::integer as "measurement_concept_id",
    null::text as "measurement_date",
    null::timestamp as "measurement_datetime",
    null::text as "measurement_time",
    null::integer as "measurement_type_concept_id",
    null::integer as "operator_concept_id",
    null::float as "value_as_number",
    null::integer as "value_as_concept_id",
    null::integer as "unit_concept_id",
    null::float as "range_low",
    null::float as "range_high",
    null::integer as "provider_id",
    null::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "measurement_source_value",
    null::integer as "measurement_source_concept_id",
    null::text as "unit_source_value",
    null::integer as "unit_source_concept_id",
    null::text as "value_source_value",
    null::integer as "measurement_event_id",
    null::integer as "meas_event_field_concept_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    