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
    null::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    icd_code::text as "condition_source_value", -- Either the code from the source or the code joined to the id given in the source
    icd_id::text as "condition_source_concept_id", -- Either the id from the source or the id that joined to the code
    null::text as "condition_status_source_value",
    row_id::integer as "x_row_id",
    encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror"
    -- from {{ ref('emerge_consort_gira_int_icd_observations') }} --place holder icd conditions
    from {{ ref('emerge_consort_gira_int_icd_conditions') }} as meas
    left join (select
        distinct emerge_id,
        case when year_of_birth is not null
             then make_date(CAST(year_of_birth as integer), 6, 15)
             else make_date(1970, 6, 15)
        end as birth_date--TODO What to do when year_of_birth is null?
        from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }} ) as person
    using (emerge_id)