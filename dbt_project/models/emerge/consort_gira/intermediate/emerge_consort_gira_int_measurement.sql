{{ config(materialized='table') }}
        
SELECT
    "emerge_id",
    "age_at_event",
    "measurement_concept_id",
    "value_as_number",
    "value_as_text",
    "range_low",
    "range_high",
    "range_flag",
    "unit_concept_id",
    "unit_concept_as_text",
    "row_id",
    "encounter_id",
    "gira_ror",
    c.*
FROM {{ ref('emerge_consort_gira_src_emerge_measurement_ex_release_20260127') }} meas_src
LEFT JOIN {{ ref('emerge_consort_gira_lookup_concept') }} AS c
    ON meas_src.measurement_concept_id = c.concept_id