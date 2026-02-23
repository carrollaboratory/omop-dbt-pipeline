{{ config(materialized='table') }}

    select
    device_exposure_id::integer as "device_exposure_id",
    person_id::integer as "person_id",
    device_concept_id::integer as "device_concept_id",
    device_exposure_start_date::text as "device_exposure_start_date",
    device_exposure_start_datetime::timestamp as "device_exposure_start_datetime",
    device_exposure_end_date::text as "device_exposure_end_date",
    device_exposure_end_datetime::timestamp as "device_exposure_end_datetime",
    device_type_concept_id::integer as "device_type_concept_id",
    unique_device_id::text as "unique_device_id",
    production_id::text as "production_id",
    quantity::integer as "quantity",
    provider_id::integer as "provider_id",
    visit_occurrence_id::integer as "visit_occurrence_id",
    visit_detail_id::integer as "visit_detail_id",
    device_source_value::text as "device_source_value",
    device_source_concept_id::integer as "device_source_concept_id",
    unit_concept_id::integer as "unit_concept_id",
    unit_source_value::text as "unit_source_value",
    unit_source_concept_id::integer as "unit_source_concept_id"
    from {{ ref('omop_54_device_exposure') }}
    