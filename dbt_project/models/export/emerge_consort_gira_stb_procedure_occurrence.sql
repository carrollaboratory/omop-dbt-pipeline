{{ config(materialized='table') }}

    select
    null::integer as "procedure_occurrence_id",
    emerge_id::integer as "person_id",
    concept_id::integer as "procedure_concept_id",
    null::text as "procedure_date",
    null::timestamp as "procedure_datetime",
    null::text as "procedure_end_date",
    null::timestamp as "procedure_end_datetime",
    null::integer as "procedure_type_concept_id", -- required but unknown in data
    null::integer as "modifier_concept_id",
    null::integer as "quantity",
    null::integer as "provider_id",
    null::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    concept_name::text as "procedure_source_value",
    icd_code::text as "procedure_source_concept_id",
    null::text as "modifier_source_value",
    row_id::text as "x_row_id",
    encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror"
    from {{ ref('emerge_consort_gira_int_icd_observations') }} -- placeholder for icd procedures
    