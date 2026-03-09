{{ config(materialized='table') }}

    select
    null::integer as "visit_detail_id",
    null::integer as "person_id",
    null::integer as "visit_detail_concept_id",
    null::text as "visit_detail_start_date",
    null::timestamp as "visit_detail_start_datetime",
    null::text as "visit_detail_end_date",
    null::timestamp as "visit_detail_end_datetime",
    null::integer as "visit_detail_type_concept_id",
    null::integer as "provider_id",
    null::integer as "care_site_id",
    null::text as "visit_detail_source_value",
    null::integer as "visit_detail_source_concept_id",
    null::integer as "admitted_from_concept_id",
    null::text as "admitted_from_source_value",
    null::integer as "discharged_to_concept_id",
    null::text as "discharged_to_source_value",
    null::integer as "preceding_visit_detail_id",
    null::integer as "parent_visit_detail_id",
    null::integer as "visit_occurrence_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    