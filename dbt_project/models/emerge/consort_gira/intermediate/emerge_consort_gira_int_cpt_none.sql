{{ config(materialized='table') }}
        
    SELECT
    emerge_id,
    age_at_event,
    cpt_code,
    mci.src_concept_id as "cpt_id", -- concept_id for the cpt_code. This could be non-standard. 
    mci.s_concept_id as "s_measurement_concept_id",
    mci.s_concept_code as "s_measurement_concept_code",
    row_id,
    encounter_id,
    gira_ror,
    FROM {{ ref('emerge_consort_gira_src_emerge_cpt_ex_release_20260129') }} src
    JOIN (SELECT -- JOIN used to drop rows that are not domain 'Measurement'
          s_concept_id, s_concept_code, src_concept_code, src_concept_id
          FROM {{ ref('emerge_consort_gira_lookup_standards') }} 
          WHERE src_table = 'CPT'
          AND (domain_id is null
              or domain_id is not in ('Measurement','Observation','Procedure','Drug','Device'))
          ) AS mci
        ON src.cpt_code = mci.src_concept_code