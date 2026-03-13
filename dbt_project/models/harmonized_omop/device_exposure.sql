{{ config(materialized='table', schema = 'omop') }}
select
    {{ generate_key('cpt', 'consort_gira', 'src_index') }}::integer as "device_exposure_id",
    emerge_id::integer as "person_id",
    s_device_concept_id::integer as "device_concept_id",
    date_add(vo.birth_date, INTERVAL (vo.age_at_event) YEAR)::date as "device_exposure_start_date",
    null::timestamp as "device_exposure_start_datetime",
    null::text as "device_exposure_end_date",
    null::timestamp as "device_exposure_end_datetime",
    32817::integer as "device_type_concept_id", --
    null::text as "unique_device_id",
    null::text as "production_id",
    null::integer as "quantity",
    null::integer as "provider_id",
    visit_occurrence_id::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "device_source_value",
    cpt_id::integer as "device_source_concept_id",
    null::integer as "unit_concept_id",
    null::text as "unit_source_value",
    null::integer as "unit_source_concept_id",
    cpt.row_id::integer as "x_row_id",
    encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror"
    from {{ ref('emerge_consort_gira_int_cpt_devices') }} as cpt
    left join {{ ref('emerge_consort_gira_int_visit_occurrences') }} as vo
    using (emerge_id, encounter_id)
    