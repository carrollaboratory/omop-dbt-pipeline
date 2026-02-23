{{ config(materialized='table') }}

    select
    visit_occurrence_id::integer as "visit_occurrence_id",
    person_id::integer as "person_id",
    visit_concept_id::integer as "visit_concept_id",
    visit_start_date::text as "visit_start_date",
    visit_start_datetime::timestamp as "visit_start_datetime",
    visit_end_date::text as "visit_end_date",
    visit_end_datetime::timestamp as "visit_end_datetime",
    visit_type_concept_id::integer as "visit_type_concept_id",
    provider_id::integer as "provider_id",
    care_site_id::integer as "care_site_id",
    visit_source_value::text as "visit_source_value",
    visit_source_concept_id::integer as "visit_source_concept_id",
    admitted_from_concept_id::integer as "admitted_from_concept_id",
    admitted_from_source_value::text as "admitted_from_source_value",
    discharged_to_concept_id::integer as "discharged_to_concept_id",
    discharged_to_source_value::text as "discharged_to_source_value",
    preceding_visit_occurrence_id::integer as "preceding_visit_occurrence_id"
    from {{ ref('omop_54_visit_occurrence') }}
    