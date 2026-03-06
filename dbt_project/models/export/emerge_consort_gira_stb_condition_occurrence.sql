{{ config(materialized='table') }}

    select
    null::integer as "condition_occurrence_id",
    emerge_id::integer as "person_id",
    s_condition_concept_id::integer as "condition_concept_id",
    date_add(birth_date, INTERVAL (age_at_event) YEAR)::date as "condition_start_date",
    null::timestamp as "condition_start_datetime",
    null::text as "condition_end_date",
    null::timestamp as "condition_end_datetime",
    32817::integer as "condition_type_concept_id",
    null::integer as "condition_status_concept_id",
    null::text as "stop_reason",
    null::integer as "provider_id",
    null::integer as "visit_occurrence_id", -- To do: join in visit occurrence_ids
    null::integer as "visit_detail_id",
    icd_code::text as "condition_source_value", -- Either the code from the source or the code joined to the id given in the source
    icd_id::text as "condition_source_concept_id", -- Either the id from the source or the id that joined to the code
    null::text as "condition_status_source_value",
    meas.row_id::integer as "x_row_id",
    encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror"
    from {{ ref('emerge_consort_gira_int_icd_conditions') }} as meas
    left join (
        from {{ ref('emerge_consort_gira_int_person_persons') }} ) as person
    using (emerge_id)