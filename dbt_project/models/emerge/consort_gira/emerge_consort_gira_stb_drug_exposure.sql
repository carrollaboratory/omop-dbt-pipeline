{{ config(materialized='table') }}

    select
    null::integer as "drug_exposure_id",
    null::integer as "person_id",
    null::integer as "drug_concept_id",
    null::text as "drug_exposure_start_date",
    null::timestamp as "drug_exposure_start_datetime",
    null::text as "drug_exposure_end_date",
    null::timestamp as "drug_exposure_end_datetime",
    null::text as "verbatim_end_date",
    null::integer as "drug_type_concept_id",
    null::text as "stop_reason",
    null::integer as "refills",
    null::float as "quantity",
    null::integer as "days_supply",
    null::text as "sig",
    null::integer as "route_concept_id",
    null::text as "lot_number",
    null::integer as "provider_id",
    null::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "drug_source_value",
    null::integer as "drug_source_concept_id",
    null::text as "route_source_value",
    null::text as "dose_unit_source_value"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    