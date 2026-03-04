{{ config(materialized='table') }}

    select
    null::integer as "condition_occurrence_id",
    emerge_id::integer as "person_id",
    concept_id::integer as "condition_concept_id",
    null::text as "condition_start_date", -- required but unknown in data
    null::timestamp as "condition_start_datetime",
    null::text as "condition_end_date",
    null::timestamp as "condition_end_datetime",
    null::integer as "condition_type_concept_id",
    null::integer as "condition_status_concept_id",
    null::text as "stop_reason",
    null::integer as "provider_id",
    null::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    concept_code::text as "condition_source_value",
    null::integer as "condition_source_concept_id",
    null::text as "condition_status_source_value",
    row_id::integer as "x_row_id",
    encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror"
    from {{ ref('emerge_consort_gira_int_icd_observations') }} --place holder icd conditions
    
    union all
    
    select
    null::integer as "condition_occurrence_id",
    emerge_id::integer as "person_id",
    concept_id::integer as "condition_concept_id",
    null::text as "condition_start_date", -- required but unknown in data
    null::timestamp as "condition_start_datetime",
    null::text as "condition_end_date",
    null::timestamp as "condition_end_datetime",
    null::integer as "condition_type_concept_id",
    null::integer as "condition_status_concept_id",
    null::text as "stop_reason",
    null::integer as "provider_id",
    null::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    concept_code::text as "condition_source_value",
    cpt_code::integer as "condition_source_concept_id",
    null::text as "condition_status_source_value",
    row_id::integer as "x_row_id",
    encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror"
    from {{ ref('emerge_consort_gira_int_cpt_observations') }} --place holder cpt conditions
    