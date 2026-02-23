{{ config(materialized='table') }}

    select
    drug_exposure_id::integer as "drug_exposure_id",
    person_id::integer as "person_id",
    drug_concept_id::integer as "drug_concept_id",
    drug_exposure_start_date::text as "drug_exposure_start_date",
    drug_exposure_start_datetime::timestamp as "drug_exposure_start_datetime",
    drug_exposure_end_date::text as "drug_exposure_end_date",
    drug_exposure_end_datetime::timestamp as "drug_exposure_end_datetime",
    verbatim_end_date::text as "verbatim_end_date",
    drug_type_concept_id::integer as "drug_type_concept_id",
    stop_reason::text as "stop_reason",
    refills::integer as "refills",
    quantity::float as "quantity",
    days_supply::integer as "days_supply",
    sig::text as "sig",
    route_concept_id::integer as "route_concept_id",
    lot_number::text as "lot_number",
    provider_id::integer as "provider_id",
    visit_occurrence_id::integer as "visit_occurrence_id",
    visit_detail_id::integer as "visit_detail_id",
    drug_source_value::text as "drug_source_value",
    drug_source_concept_id::integer as "drug_source_concept_id",
    route_source_value::text as "route_source_value",
    dose_unit_source_value::text as "dose_unit_source_value"
    from {{ ref('omop_54_drug_exposure') }}
    