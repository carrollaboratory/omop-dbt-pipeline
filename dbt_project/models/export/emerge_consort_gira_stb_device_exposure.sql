{{ config(materialized='table') }}

    select
    null::integer as "device_exposure_id",
    emerge_id::integer as "person_id",
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
    null::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "device_source_value",
    cpt_id::integer as "device_source_concept_id",
    null::integer as "unit_concept_id",
    null::text as "unit_source_value",
    null::integer as "unit_source_concept_id",
    row_id::integer as "x_row_id",
    encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror"
    from {{ ref('emerge_consort_gira_int_cpt_devices') }}
    left join (select
        distinct emerge_id,
        case when year_of_birth is not null
             then make_date(CAST(year_of_birth as integer), 6, 15)
             else make_date(1970, 6, 15)
        end as birth_date--TODO What to do when year_of_birth is null?
        from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }} ) as person
    using (emerge_id)
    