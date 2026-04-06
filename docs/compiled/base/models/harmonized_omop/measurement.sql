

with base as (
    select
    emerge_id::integer as "person_id",
    s_measurement_concept_id::integer as "measurement_concept_id",
    date_add(vo.birth_date, INTERVAL (vo.age_at_event) YEAR)::date as "measurement_date",
    null::timestamp as "measurement_datetime",
    null::text as "measurement_time",
    32817::integer as "measurement_type_concept_id",
    null::integer as "operator_concept_id",
    value_as_number::float as "value_as_number",
    value_as_concept_id::integer as "value_as_concept_id",
    s_unit_concept_id::integer as "unit_concept_id",
    null::float as "range_low",
    null::float as "range_high",
    null::integer as "provider_id",
    visit_occurrence_id::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "measurement_source_value",
    null::integer as "measurement_source_concept_id",
    unit_concept_name::text as "unit_source_value",
    unit_concept_id::integer as "unit_source_concept_id",
    null::text as "value_source_value",
    null::integer as "measurement_event_id",
    null::integer as "meas_event_field_concept_id",
    vo.age_at_event::integer as "x_age_at_event",
    meas.row_id::integer as "x_row_id",
    meas.encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror" --TODO Check distinct values - Non integer
    from "dbt"."main"."emerge_consort_gira_int_bmi_measurements" as meas
    left join "dbt"."main"."emerge_consort_gira_int_visit_occurrences" as vo
    using (emerge_id, encounter_id)
        
    union all
    
    select
    emerge_id::integer as "person_id",
    s_measurement_concept_id::integer as "measurement_concept_id",
    date_add(vo.birth_date, INTERVAL (vo.age_at_event) YEAR)::date as "measurement_date",
    null::timestamp as "measurement_datetime",
    null::text as "measurement_time",
    32817::integer as "measurement_type_concept_id",
    null::integer as "operator_concept_id",
    null::float as "value_as_number",
    value_as_concept_id::integer as "value_as_concept_id",
    null::integer as "unit_concept_id",
    null::float as "range_low",
    null::float as "range_high",
    null::integer as "provider_id",
    visit_occurrence_id::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "measurement_source_value",
    cpt_id::integer as "measurement_source_concept_id",
    null::text as "unit_source_value",
    null::integer as "unit_source_concept_id",
    null::text as "value_source_value",
    null::integer as "measurement_event_id",
    null::integer as "meas_event_field_concept_id",
    vo.age_at_event::integer as "x_age_at_event",
    meas.row_id::integer as "x_row_id",
    meas.encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror" --TODO Check distinct values - Non integer
    from "dbt"."main"."emerge_consort_gira_int_cpt_measurements" as meas
    left join "dbt"."main"."emerge_consort_gira_int_visit_occurrences" as vo
    using (emerge_id, encounter_id)
    
    union all
    
    select
    emerge_id::integer as "person_id",
    s_measurement_concept_id::integer as "measurement_concept_id",
    date_add(vo.birth_date, INTERVAL (vo.age_at_event) YEAR)::date as "measurement_date",
    null::timestamp as "measurement_datetime",
    null::text as "measurement_time",
    32817::integer as "measurement_type_concept_id", 
    null::integer as "operator_concept_id",
    null::float as "value_as_number",
    value_as_concept_id::integer as "value_as_concept_id",
    null::integer as "unit_concept_id",
    null::float as "range_low",
    null::float as "range_high",
    null::integer as "provider_id",
    visit_occurrence_id::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    icd_code::text as "measurement_source_value",
    icd_id::integer as "measurement_source_concept_id",
    null::text as "unit_source_value",
    null::integer as "unit_source_concept_id",
    null::text as "value_source_value",
    null::integer as "measurement_event_id",
    null::integer as "meas_event_field_concept_id",
    vo.age_at_event::integer as "x_age_at_event",
    meas.row_id::integer as "x_row_id",
    meas.encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror" --TODO Check distinct values - Non integer
    from "dbt"."main"."emerge_consort_gira_int_icd_measurements" as meas
    left join "dbt"."main"."emerge_consort_gira_int_visit_occurrences" as vo
    using (emerge_id, encounter_id)
    
    union all
    
    select
    emerge_id::integer as "person_id",
    s_measurement_concept_id::integer as "measurement_concept_id",
    date_add(vo.birth_date, INTERVAL (vo.age_at_event) YEAR)::date as "measurement_date",
    null::timestamp as "measurement_datetime",
    null::text as "measurement_time",
    32817::integer as "measurement_type_concept_id", 
    null::integer as "operator_concept_id",
    value_as_number::float as "value_as_number",
    value_as_concept_id::integer as "value_as_concept_id",
    s_unit_concept_id::integer as "unit_concept_id",
    range_low::float as "range_low",
    range_high::float as "range_high",
    null::integer as "provider_id",
    visit_occurrence_id::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "measurement_source_value",
    measurement_concept_id::integer as "measurement_source_concept_id",
    unit_concept_as_text::text as "unit_source_value",
    unit_concept_id::integer as "unit_source_concept_id",
    null::text as "value_source_value",
    null::integer as "measurement_event_id",
    null::integer as "meas_event_field_concept_id",
    vo.age_at_event::integer as "x_age_at_event",
    meas.row_id::integer as "x_row_id",
    meas.encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror" --TODO Check distinct values - Non integer
    from "dbt"."main"."emerge_consort_gira_int_measurement_measurements" as meas
    left join "dbt"."main"."emerge_consort_gira_int_visit_occurrences" as vo
    using (emerge_id, encounter_id)
    
    )
    select
        CAST(2000000 AS INTEGER) + ROW_NUMBER() OVER ()::integer as "measurement_id",
        base.*
    from base