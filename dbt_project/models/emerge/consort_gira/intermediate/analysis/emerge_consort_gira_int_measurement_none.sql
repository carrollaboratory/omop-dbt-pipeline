{{ config(materialized='table') }}
     
SELECT
    emerge_id,
    age_at_event,
    measurement_concept_id,
    mci.s_concept_id as "s_measurement_concept_id",
    mci.s_concept_code as "s_measurement_concept_code",
    value_as_number,
    value_as_text,
    range_low,
    range_high,
    range_flag,
    unit_concept_id,
    unit_concept_as_text,
    uci.s_concept_id as "s_unit_concept_id",
    uci.s_concept_code as "s_unit_concept_code",
    row_id,
    encounter_id,
    gira_ror,
    src_index,
FROM {{ ref('emerge_consort_gira_src_emerge_measurement_ex_release_20260127') }} src
JOIN (SELECT -- JOIN used to drop rows that are not domain 'Measurement'
      s_concept_id, s_concept_code, src_concept_code, src_concept_id
      FROM {{ ref('emerge_consort_gira_lookup_standards') }} 
      WHERE src_table = 'CPT'
      AND (domain_id is null
          or domain_id not in ('Measurement','Observation','Procedure','Drug','Device')
          or s_concept_id = '0')
      ) AS mci
    ON src.measurement_concept_id = mci.src_concept_id
LEFT JOIN (SELECT
      s_concept_id, s_concept_code, src_concept_id
      FROM {{ ref('emerge_consort_gira_lookup_standards') }} 
      WHERE src_table = 'M'
      ) AS uci
    ON src.unit_concept_id = uci.src_concept_id