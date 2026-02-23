{{ config(materialized='table') }}

    select
    null::integer as "person_id",
    null::text as "death_date",
    null::timestamp as "death_datetime",
    null::integer as "death_type_concept_id",
    null::integer as "cause_concept_id",
    null::text as "cause_source_value",
    null::integer as "cause_source_concept_id"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    