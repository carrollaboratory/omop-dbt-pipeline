{{ config(materialized='table') }}

    select
    cdm_source_name::text as "cdm_source_name",
    cdm_source_abbreviation::text as "cdm_source_abbreviation",
    cdm_holder::text as "cdm_holder",
    source_description::text as "source_description",
    source_documentation_reference::text as "source_documentation_reference",
    cdm_etl_reference::text as "cdm_etl_reference",
    source_release_date::text as "source_release_date",
    cdm_release_date::text as "cdm_release_date",
    cdm_version::text as "cdm_version",
    vocabulary_version::text as "vocabulary_version"
    from {{ ref('omop_54_cdm_source') }}
    