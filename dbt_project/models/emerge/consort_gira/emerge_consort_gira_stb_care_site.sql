{{ config(materialized='table') }}

    select
    null::integer as "care_site_id",
    null::text as "care_site_name",
    null::integer as "place_of_service_concept_id",
    null::integer as "location_id",
    null::text as "care_site_source_value",
    null::text as "place_of_service_source_value"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    