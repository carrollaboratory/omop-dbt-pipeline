{{ config(materialized='table') }}

--     select emerge_id, encounter_id, (300000 + src_index) as visit_id
--     from {{ ref('emerge_consort_gira_src_emerge_measurement_ex_release_20260127') }}
--     union
--     select emerge_id, encounter_id, (300000 + src_index) as visit_id
--     from {{ ref('emerge_consort_gira_src_emerge_bmi_ex_release_20260128') }}
--     union
--     select emerge_id, encounter_id, (300000 + src_index) as visit_id
--     from {{ ref('emerge_consort_gira_src_emerge_cpt_ex_release_20260129') }}
--     union
--     select emerge_id, encounter_id, (300000 + src_index) as visit_id
--     from {{ ref('emerge_consort_gira_src_emerge_icd_ex_release_20260129') }}

    select emerge_id, encounter_id, src_index + 3000000 as visit_id
    from {{ ref('emerge_consort_gira_src_emerge_measurement_ex_release_20260127') }}
    union
    select emerge_id, encounter_id, src_index + 3000000 as visit_id
    from {{ ref('emerge_consort_gira_src_emerge_bmi_ex_release_20260128') }}
    union
    select emerge_id, encounter_id, src_index + 3000000 as visit_id
    from {{ ref('emerge_consort_gira_src_emerge_cpt_ex_release_20260129') }}
    union
    select emerge_id, encounter_id, src_index + 3000000 as visit_id
    from {{ ref('emerge_consort_gira_src_emerge_icd_ex_release_20260129') }}
