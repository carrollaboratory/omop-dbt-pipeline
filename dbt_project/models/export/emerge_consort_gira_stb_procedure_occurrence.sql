{{ config(materialized='table') }}

    select
    null::integer as "procedure_occurrence_id",
    null::integer as "person_id",
    null::integer as "procedure_concept_id",
    null::text as "procedure_date",
    null::timestamp as "procedure_datetime",
    null::text as "procedure_end_date",
    null::timestamp as "procedure_end_datetime",
    null::integer as "procedure_type_concept_id",
    null::integer as "modifier_concept_id",
    null::integer as "quantity",
    null::integer as "provider_id",
    null::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "procedure_source_value",
    null::integer as "procedure_source_concept_id",
    null::text as "modifier_source_value"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    