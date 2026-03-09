{{ config(materialized='table') }}
    select
    device_id::integer as "device_exposure_id",
    person_id::integer as "person_id", -- TODO should macro.row_id be person_id or row_id for the Person primary key?
    s_device_concept_id::integer as "device_concept_id",
    date_add(birth_date, INTERVAL (age_at_event) YEAR)::date as "device_exposure_start_date",
    null::timestamp as "device_exposure_start_datetime",
    null::text as "device_exposure_end_date",
    null::timestamp as "device_exposure_end_datetime",
    32817::integer as "device_type_concept_id",
    null::text as "unique_device_id",
    null::text as "production_id",
    null::integer as "quantity",
    null::integer as "provider_id",
    null::integer as "visit_occurrence_id", -- To do: join visit occurrence by x_encounter_id
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
    left join ( select row_id, birth_date, emerge_id
        from {{ ref('emerge_consort_gira_int_person_persons') }} ) as p
    using (emerge_id)
    