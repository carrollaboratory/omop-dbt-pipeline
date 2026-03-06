{{ config(materialized='table') }}

    select
    null::integer as "measurement_id",
    emerge_id::integer as "person_id",
    measurement_concept_id::integer as "measurement_concept_id",
--     Take day, month, and year_of_birth as a date and add the number of years from measurement.age_at_event to get the measurement_date
    date_add(birth_date, INTERVAL (age_at_event) YEAR)::DATE AS "measurement_date",
    null::timestamp as "measurement_datetime",
    null::text as "measurement_time",
    null::integer as "measurement_type_concept_id", -- TODO required but unknown from the data
    null::integer as "operator_concept_id",
    value_as_number::float as "value_as_number",
    null::integer as "value_as_concept_id",
    s_concept_id::integer as "unit_concept_id",
    case
      when try_cast(range_low as float) is not null
      then try_cast(range_low as float)
      else null -- TODO What to do when not a float. Example: >=40 Not sure why characterization report didn't pick this up
    end as range_low,    
    case
      when try_cast(range_high as float) is not null
      then try_cast(range_high as float)
      else null -- TODO What to do when not a float. Example: >=40 Not sure why characterization report didn't pick this up
    end as range_high, 
    null::integer as "provider_id",
    null::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "measurement_source_value",
    null::integer as "measurement_source_concept_id",
    unit_concept_as_text::text as "unit_source_value",
    unit_concept_id::integer as "unit_source_concept_id",
    null::text as "value_source_value",
    null::integer as "measurement_event_id",
    null::integer as "meas_event_field_concept_id",
    age_at_event::integer as "x_age_at_event",
    meas.row_id::integer as "x_row_id",
    meas.encounter_id::integer as "x_encounter_id",
    meas.gira_ror::text as "x_gira_ror" --TODO Check distinct values - Non integer
    from {{ ref('emerge_consort_gira_int_measurement_measurements') }} as meas
    left join (select
        distinct emerge_id,
        CASE WHEN year_of_birth IS NOT NULL
             THEN MAKE_DATE(CAST(year_of_birth AS INTEGER), 6, 15)
             ELSE MAKE_DATE(1970, 6, 15)
        END AS birth_date--TODO What to do when year_of_birth is null?
        from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }} ) as person
    using (emerge_id)
    join to the lookup_standard table on unit_concept_id = src_concept_id
    
    union all
    
    select 
    null::integer as "measurement_id", --TODO What are we using for Primary keys? Same as ACR transforms?
    emerge_id::integer as "person_id",
    measurement_concept_id::integer as "measurement_concept_id", -- TODO We need to get BMI Z score into this field, too IF NOT NULL
    date_add(birth_date, INTERVAL (age_at_event) YEAR)::DATE AS "measurement_date",
    null::timestamp as "measurement_datetime",
    null::text as "measurement_time",
    null::integer as "measurement_type_concept_id", -- required but unknown from the data
    null::integer as "operator_concept_id",
    value_as_number::float as "value_as_number",
    null::integer as "value_as_concept_id",
    unit_concept_id::integer as "unit_concept_id",
    null::float as "range_low",
    null::float as "range_high",
    null::integer as "provider_id",
    null::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    measurement_concept_name::text as "measurement_source_value",
    null::integer as "measurement_source_concept_id",
    unit_concept_name::text as "unit_source_value",
    null::integer as "unit_source_concept_id",
    null::text as "value_source_value",
    null::integer as "measurement_event_id",
    null::integer as "meas_event_field_concept_id",
    age_at_event::integer as "x_age_at_event",
    row_id::integer as "x_row_id",
    encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror"
    from {{ ref('emerge_consort_gira_int_bmi_measurements') }}
    left join (select
        distinct emerge_id,
        CASE WHEN year_of_birth IS NOT NULL
             THEN MAKE_DATE(CAST(year_of_birth AS INTEGER), 6, 15)
             ELSE MAKE_DATE(1970, 6, 15)
        END AS birth_date--TODO What to do when year_of_birth is null?
        from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }} ) as person
    using (emerge_id)
    