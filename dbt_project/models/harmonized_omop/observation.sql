{{ config(materialized='table', schema = 'omop') }}

    select distinct
    {{ generate_key('measurement', 'consort_gira', 'src_index') }}::integer as "observation_id",
    emerge_id::integer as "person_id",
    s_observation_concept_id::integer as "observation_concept_id", -- concept_id from joined table
    date_add(vo.birth_date, INTERVAL (vo.age_at_event) YEAR)::date as "observation_date", 
    null::timestamp as "observation_datetime", 
    32817::integer as "observation_type_concept_id",
    value_as_number::float as "value_as_number",
    value_as_text::text as "value_as_string",
    null::text as "value_as_concept_id",
    null::integer as "qualifier_concept_id",
    s_unit_concept_id::integer as "unit_concept_id",
    null::integer as "provider_id",
    visit_occurrence_id::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "observation_source_value",
    null::integer as "observation_source_concept_id",
    unit_concept_id::text as "unit_source_value",
    null::text as "qualifier_source_value",
    null::text as "value_source_value",
    null::integer as "observation_event_id",
    null::integer as "obs_event_field_concept_id",
    meas.row_id::integer as "x_row_id",
    meas.encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror"
    from {{ ref('emerge_consort_gira_int_measurement_observations') }} as meas
    left join {{ ref('emerge_consort_gira_int_visit_occurrences') }} as vo
    using (emerge_id, encounter_id)
    
    union all
    
    select  distinct
    {{ generate_key('cpt', 'consort_gira', 'src_index') }}::integer as "observation_id",
    emerge_id::integer as "person_id",
    s_observation_concept_id::integer as "observation_concept_id", -- concept_id from joined table
    date_add(vo.birth_date, INTERVAL (vo.age_at_event) YEAR)::date as "observation_date",
    null::timestamp as "observation_datetime", 
    32817::integer as "observation_type_concept_id", -- Code for EHR is consistent type
    null::float as "value_as_number",
    null::text as "value_as_string",
    null::text as "value_as_concept_id",
    null::integer as "qualifier_concept_id",
    null::integer as "unit_concept_id",
    null::integer as "provider_id",
    visit_occurrence_id::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    cpt_code::text as "observation_source_value",
    cpt_id::integer as "observation_source_concept_id",
    null::text as "unit_source_value",
    null::text as "qualifier_source_value",
    null::text as "value_source_value",
    null::integer as "observation_event_id",
    null::integer as "obs_event_field_concept_id",
    cpt.row_id::integer as "x_row_id",
    encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror"
    from {{ ref('emerge_consort_gira_int_cpt_observations') }} as cpt
    left join {{ ref('emerge_consort_gira_int_visit_occurrences') }} as vo
    using (emerge_id, encounter_id)
    
    union all
    
    select distinct
    {{ generate_key('icd', 'consort_gira', 'src_index') }}::integer as "observation_id",
    emerge_id::integer as "person_id",
    s_observation_concept_id::integer as "observation_concept_id", -- concept_id from joined table
    date_add(vo.birth_date, INTERVAL (vo.age_at_event) YEAR)::date as "observation_date", 
    null::timestamp as "observation_datetime", 
    null::integer as "observation_type_concept_id", -- required but unknown in the data
    null::float as "value_as_number",
    null::text as "value_as_string",
    null::text as "value_as_concept_id",
    null::integer as "qualifier_concept_id",
    null::integer as "unit_concept_id",
    null::integer as "provider_id",
    visit_occurrence_id::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    icd_code::text as "observation_source_value",
    icd_id::integer as "observation_source_concept_id",
    null::text as "unit_source_value",
    null::text as "qualifier_source_value",
    null::text as "value_source_value",
    null::integer as "observation_event_id",
    null::integer as "obs_event_field_concept_id",
    icd.row_id::text as "x_row_id",
    encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror"
    from {{ ref('emerge_consort_gira_int_icd_observations') }} as icd
    left join {{ ref('emerge_consort_gira_int_visit_occurrences') }} as vo
    using (emerge_id, encounter_id)