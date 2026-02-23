{{ config(materialized='table') }}

    select
    null::integer as "device_exposure_id",
    null::integer as "person_id",
    null::integer as "device_concept_id",
    null::text as "device_exposure_start_date",
    null::timestamp as "device_exposure_start_datetime",
    null::text as "device_exposure_end_date",
    null::timestamp as "device_exposure_end_datetime",
    null::integer as "device_type_concept_id",
    null::text as "unique_device_id",
    null::text as "production_id",
    null::integer as "quantity",
    null::integer as "provider_id",
    null::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "device_source_value",
    null::integer as "device_source_concept_id",
    null::integer as "unit_concept_id",
    null::text as "unit_source_value",
    null::integer as "unit_source_concept_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    