{{ config(materialized='table') }}

    select
    null::integer as "cohort_definition_id",
    null::text as "cohort_definition_name",
    null::text as "cohort_definition_description",
    null::integer as "definition_type_concept_id",
    null::text as "cohort_definition_syntax",
    null::integer as "subject_concept_id",
    null::text as "cohort_initiation_date"
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    