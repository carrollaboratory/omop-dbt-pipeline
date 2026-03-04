{{ config(materialized='table') }}

    select
    null::integer as "observation_id",
    emerge_id::integer as "person_id",
    null::integer as "observation_concept_id", -- concept_id from joined table
    null::text as "observation_date", -- date_add process needs to plug in here
    null::timestamp as "observation_datetime", 
    null::integer as "observation_type_concept_id", -- required but unknown in the data
    null::float as "value_as_number",
    null::text as "value_as_string",
    cpt_code::integer as "value_as_concept_id",
    null::integer as "qualifier_concept_id",
    null::integer as "unit_concept_id",
    null::integer as "provider_id",
    null::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "observation_source_value",
    null::integer as "observation_source_concept_id",
    null::text as "unit_source_value",
    null::text as "qualifier_source_value",
    null::text as "value_source_value",
    null::integer as "observation_event_id",
    null::integer as "obs_event_field_concept_id",
    row_id::integer as "x_row_id",
    encounter_id::integer as "x_encounter_id",
    gira_ror::integer as "x_gira_ror"
    from (SELECT * FROM {{ ref('emerge_consort_gira_int_cpt') }} WHERE domain_id = 'Observation')
    
    union all
    
    select
    null::integer as "observation_id",
    emerge_id::integer as "person_id",
    null::integer as "observation_concept_id", -- concept_id from joined table
    null::text as "observation_date", -- date_add process needs to plug in here
    null::timestamp as "observation_datetime", 
    null::integer as "observation_type_concept_id", -- required but unknown in the data
    null::float as "value_as_number",
    null::text as "value_as_string",
    icd_code::integer as "value_as_concept_id",
    null::integer as "qualifier_concept_id",
    null::integer as "unit_concept_id",
    null::integer as "provider_id",
    null::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "observation_source_value",
    null::integer as "observation_source_concept_id",
    null::text as "unit_source_value",
    null::text as "qualifier_source_value",
    null::text as "value_source_value",
    null::integer as "observation_event_id",
    null::integer as "obs_event_field_concept_id",
    row_id::integer as "x_row_id",
    encounter_id::integer as "x_encounter_id",
    gira_ror::integer as "x_gira_ror"
    from (SELECT * FROM {{ ref('emerge_consort_gira_int_icd') }} WHERE domain_id = 'Observation')
    