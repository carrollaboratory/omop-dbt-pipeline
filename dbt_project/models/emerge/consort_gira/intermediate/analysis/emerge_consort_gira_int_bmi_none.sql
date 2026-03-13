{{ config(materialized='table') }}
-- Columns prefixed with 's_' represent the confirmed Standard value for the column. If the field is NULL no Standard could be found.
        
SELECT
    emerge_id,
    age_at_event,
    measurement_concept_id,
    measurement_concept_name,
    mci.s_concept_id as "s_measurement_concept_id", 
    mci.s_concept_code as "s_measurement_concept_code",
    value_as_number,
    unit_concept_id,
    unit_concept_name,
    uci.s_concept_id as "s_unit_concept_id",
    uci.s_concept_code as "s_unit_concept_code",
    bmi_z_score,
    row_id,
    encounter_id,
    gira_ror,
    src_index,
FROM {{ ref('emerge_consort_gira_src_emerge_bmi_ex_release_20260128') }} src
JOIN (SELECT -- JOIN used to drop rows that are not domain 'Measurement'
      s_concept_id, s_concept_code, src_concept_id
      FROM {{ ref('emerge_consort_gira_lookup_standards') }} 
      WHERE src_table = 'BMI'
      AND (domain_id is null
          or domain_id not in ('Measurement'))
      ) AS mci
    ON src.measurement_concept_id = mci.src_concept_id
LEFT JOIN (SELECT
      s_concept_id, s_concept_code, src_concept_id
      FROM {{ ref('emerge_consort_gira_lookup_standards') }} 
      WHERE src_table = 'BMI'
      AND domain_id != 'Measurement'
      ) AS uci
    ON src.unit_concept_id = uci.src_concept_id