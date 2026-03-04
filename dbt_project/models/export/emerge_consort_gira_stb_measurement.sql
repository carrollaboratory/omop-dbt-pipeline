{{ config(materialized='table') }}

    select
    null::integer as "measurement_id",
    emerge_id::integer as "person_id",
    measurement_concept_id::integer as "measurement_concept_id",
    -- I want to take day, month, and year of birth as a date and add the number of years from age_at_event in measurement to get a date of measurement
    {{ dateadd(datepart="year", interval=1, from_date_or_timestamp="date 'birth_date'") }}::date as "measurement_date",
    -- date_add(date birth_date, INTERVAL age_at_event YEAR)::date as "measurement_date",
    null::timestamp as "measurement_datetime",
    null::text as "measurement_time",
    null::integer as "measurement_type_concept_id", -- required but unknown from the data
    null::integer as "operator_concept_id",
    value_as_number::float as "value_as_number",
    null::integer as "value_as_concept_id",
    unit_concept_id::integer as "unit_concept_id",
    range_low::float as "range_low",
    range_high::float as "range_high",
    null::integer as "provider_id",
    null::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "measurement_source_value",
    null::integer as "measurement_source_concept_id",
    unit_concept_as_text::text as "unit_source_value",
    null::integer as "unit_source_concept_id",
    null::text as "value_source_value",
    null::integer as "measurement_event_id",
    null::integer as "meas_event_field_concept_id",
    age_at_event::integer as "x_age_at_event",
    meas.row_id::integer as "x_row_id",
    meas.encounter_id::integer as "x_encounter_id",
    meas.gira_ror::integer as "x_gira_ror"
    from {{ ref('emerge_consort_gira_int_measurement') }} as meas
    left join ( SELECT
          *,
          -- CAST(CONCAT(year_of_birth + '-06-15'), DATE) as birth_date
          CONCAT(person.year_of_birth, '-06-15') as birth_date_A
          strptime(DATE birth_date_A, '%Y-%m-%d') as birth_date
          -- CAST(CONCAT(person.year_of_birth, '-06-15') as DATE) as birth_date
          FROM {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }} as person )
    using (emerge_id)
    
--     union all
    
--     select
--     null::integer as "measurement_id",
--     emerge_id::integer as "person_id",
--     measurement_concept_id::integer as "measurement_concept_id", -- We need to get BMI Z score into this field, too
--     date_add(date birth_date, INTERVAL age_at_event YEAR)::date as "measurement_date",
--     null::timestamp as "measurement_datetime",
--     null::text as "measurement_time",
--     null::integer as "measurement_type_concept_id", -- required but unknown from the data
--     null::integer as "operator_concept_id",
--     value_as_number::float as "value_as_number",
--     null::integer as "value_as_concept_id",
--     unit_concept_id::integer as "unit_concept_id",
--     null::float as "range_low",
--     null::float as "range_high",
--     null::integer as "provider_id",
--     null::integer as "visit_occurrence_id",
--     null::integer as "visit_detail_id",
--     measurement_concept_name::text as "measurement_source_value",
--     null::integer as "measurement_source_concept_id",
--     unit_concept_name::text as "unit_source_value",
--     null::integer as "unit_source_concept_id",
--     null::text as "value_source_value",
--     null::integer as "measurement_event_id",
--     null::integer as "meas_event_field_concept_id",
--     age_at_event::integer as "x_age_at_event",
--         row_id::integer as "x_row_id",
--     encounter_id::integer as "x_encounter_id",
--     gira_ror::integer as "x_gira_ror"
--     from {{ ref('emerge_consort_gira_int_bmi') }} as meas
--     left join ( SELECT
--           *,
--           CAST(CONCAT(year_of_birth + '-06-15'), DATE) as birth_date
--           FROM {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}  )
--     using (emerge_id)
    