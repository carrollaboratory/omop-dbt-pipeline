{{ config(materialized='table') }}

    select
    null::integer as "specimen_id",
    null::integer as "person_id",
    null::integer as "specimen_concept_id",
    null::integer as "specimen_type_concept_id",
    null::text as "specimen_date",
    null::timestamp as "specimen_datetime",
    null::float as "quantity",
    null::integer as "unit_concept_id",
    null::integer as "anatomic_site_concept_id",
    null::integer as "disease_status_concept_id",
    null::text as "specimen_source_id",
    null::text as "specimen_source_value",
    null::text as "unit_source_value",
    null::text as "anatomic_site_source_value",
    null::text as "disease_status_source_value"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    