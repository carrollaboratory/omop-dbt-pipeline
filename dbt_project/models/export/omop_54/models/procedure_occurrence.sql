{{ config(materialized='table') }}

    select
    procedure_occurrence_id::integer as "procedure_occurrence_id",
    person_id::integer as "person_id",
    procedure_concept_id::integer as "procedure_concept_id",
    procedure_date::text as "procedure_date",
    procedure_datetime::timestamp as "procedure_datetime",
    procedure_end_date::text as "procedure_end_date",
    procedure_end_datetime::timestamp as "procedure_end_datetime",
    procedure_type_concept_id::integer as "procedure_type_concept_id",
    modifier_concept_id::integer as "modifier_concept_id",
    quantity::integer as "quantity",
    provider_id::integer as "provider_id",
    visit_occurrence_id::integer as "visit_occurrence_id",
    visit_detail_id::integer as "visit_detail_id",
    procedure_source_value::text as "procedure_source_value",
    procedure_source_concept_id::integer as "procedure_source_concept_id",
    modifier_source_value::text as "modifier_source_value"
    from {{ ref('omop_54_procedure_occurrence') }}
    