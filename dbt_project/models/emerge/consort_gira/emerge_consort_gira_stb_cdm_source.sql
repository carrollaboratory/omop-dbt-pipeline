{{ config(materialized='table') }}

    select
    null::text as "cdm_source_name",
    null::text as "cdm_source_abbreviation",
    null::text as "cdm_holder",
    null::text as "source_description",
    null::text as "source_documentation_reference",
    null::text as "cdm_etl_reference",
    null::text as "source_release_date",
    null::text as "cdm_release_date",
    null::text as "cdm_version",
    null::text as "vocabulary_version"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    