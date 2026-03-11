{{ config(materialized='table') }}

    select
    null::integer as "metadata_concept_id",
    null::integer as "metadata_type_concept_id",
    null::text as "name",
    null::text as "value_as_string",
    null::integer as "value_as_concept_id",
    null::text as "metadata_date",
    null::timestamp as "metadata_datetime"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    