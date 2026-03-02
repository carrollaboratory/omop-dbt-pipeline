{{ config(materialized='table') }}

    select
    null::integer as "visit_occurrence_id",
    emerge_id::integer as "person_id",
    null::integer as "visit_concept_id", -- required but unknown in the src data
    null::text as "visit_start_date", -- visits "will all be on the same date"
    null::timestamp as "visit_start_datetime",
    null::text as "visit_end_date", -- also the same date as start date?
    null::timestamp as "visit_end_datetime",
    null::integer as "visit_type_concept_id", -- required but unknown in the data, could be https://athena.ohdsi.org/search-terms/terms/32817
    null::integer as "provider_id",
    null::integer as "care_site_id",
    null::text as "visit_source_value",
    null::integer as "visit_source_concept_id",
    null::integer as "admitted_from_concept_id",
    null::text as "admitted_from_source_value",
    null::integer as "discharged_to_concept_id",
    null::text as "discharged_to_source_value",
    null::integer as "preceding_visit_occurrence_id", -- not required but we could add in logic here
    encounter_id::integer as "x_encounter_id"
    -- need to join cpt, measurement, icd, and bmi tables to get the total uniqe encounter_ids per participant
    select encounter_id, emerge_id from {{ ref('emerge_consort_gira_src_emerge_cpt_ex_release_20260129') }}
    union
    select encounter_id, emerge_id from {{ ref('emerge_consort_gira_src_emerge_icd_ex_release_20260129') }}
    union
    select encounter_id, emerge_id from {{ ref('emerge_consort_gira_src_emerge_bmi_ex_release_20260128') }}
    union
    select encounter_id, emerge_id from {{ ref('emerge_consort_gira_src_emerge_measurement_ex_release_20260127') }}
    group by emerge_id
    unique encounter_id
