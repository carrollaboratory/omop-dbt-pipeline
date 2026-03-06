{{ config(materialized='table') }}

    select
    null::integer as "drug_exposure_id",
    emerge_id::integer as "person_id",
    s_drug_concept_id::integer as "drug_concept_id",
    null::text as "drug_exposure_start_date",
    null::timestamp as "drug_exposure_start_datetime",
    null::text as "drug_exposure_end_date",
    null::timestamp as "drug_exposure_end_datetime",
    null::text as "verbatim_end_date",
    32817::integer as "drug_type_concept_id",
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
    cpt_id::integer as "drug_source_concept_id",
    null::text as "route_source_value",
    null::text as "dose_unit_source_value",
    cpt.row_id::integer as "x_row_id",
    encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror"
    from {{ ref('emerge_consort_gira_int_cpt_drugs') }} as cpt
    left join (
        from {{ ref('emerge_consort_gira_int_person_persons') }} ) as person
    using (emerge_id)
    