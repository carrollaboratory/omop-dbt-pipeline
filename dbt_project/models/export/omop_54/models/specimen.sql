{{ config(materialized='table') }}

    select
    specimen_id::integer as "specimen_id",
    person_id::integer as "person_id",
    specimen_concept_id::integer as "specimen_concept_id",
    specimen_type_concept_id::integer as "specimen_type_concept_id",
    specimen_date::text as "specimen_date",
    specimen_datetime::timestamp as "specimen_datetime",
    quantity::float as "quantity",
    unit_concept_id::integer as "unit_concept_id",
    anatomic_site_concept_id::integer as "anatomic_site_concept_id",
    disease_status_concept_id::integer as "disease_status_concept_id",
    specimen_source_id::text as "specimen_source_id",
    specimen_source_value::text as "specimen_source_value",
    unit_source_value::text as "unit_source_value",
    anatomic_site_source_value::text as "anatomic_site_source_value",
    disease_status_source_value::text as "disease_status_source_value"
    from {{ ref('omop_54_specimen') }}
    