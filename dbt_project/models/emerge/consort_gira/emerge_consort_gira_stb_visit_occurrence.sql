{{ config(materialized='table') }}

    select
    null::integer as "visit_occurrence_id",
    null::integer as "person_id",
    null::integer as "visit_concept_id",
    null::text as "visit_start_date",
    null::timestamp as "visit_start_datetime",
    null::text as "visit_end_date",
    null::timestamp as "visit_end_datetime",
    null::integer as "visit_type_concept_id",
    null::integer as "provider_id",
    null::integer as "care_site_id",
    null::text as "visit_source_value",
    null::integer as "visit_source_concept_id",
    null::integer as "admitted_from_concept_id",
    null::text as "admitted_from_source_value",
    null::integer as "discharged_to_concept_id",
    null::text as "discharged_to_source_value",
    null::integer as "preceding_visit_occurrence_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    