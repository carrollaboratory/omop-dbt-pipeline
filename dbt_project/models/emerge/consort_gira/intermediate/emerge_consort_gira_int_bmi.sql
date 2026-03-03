{{ config(materialized='table') }}
        
SELECT
    emerge_id,
    age_at_event,
    measurement_concept_id,
    measurement_concept_name,
    value_as_number,
    unit_concept_id,
    unit_concept_name,
    bmi_z_score,
    row_id,
    encounter_id,
    gira_ror,
    c.*
FROM {{ ref('emerge_consort_gira_src_emerge_bmi_ex_release_20260128') }} bmi_src
LEFT JOIN {{ ref('emerge_consort_gira_lookup_concept') }} AS concept_lookup
    ON bmi_src.measurement_concept_id = concept_lookup.concept_id
