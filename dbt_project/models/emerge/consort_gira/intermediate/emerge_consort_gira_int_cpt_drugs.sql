{{ config(materialized='table') }}
        
    SELECT
    emerge_id,
    age_at_event,
    cpt_code,
    mci.src_concept_id as "cpt_id", -- concept_id for the cpt_code. This could be non-standard. 
    mci.s_concept_id as "s_drug_concept_id",
    mci.s_concept_code as "s_drug_concept_code",
    row_id,
    encounter_id,
    gira_ror,
    FROM {{ ref('emerge_consort_gira_src_emerge_cpt_ex_release_20260129') }} src
    JOIN (SELECT -- JOIN used to drop rows that are not domain 'Drug'
          s_concept_id, s_concept_code, src_concept_code, src_concept_id
          FROM {{ ref('emerge_consort_gira_lookup_standards') }} 
          WHERE src_table = 'CPT'
          AND domain_id = 'Drug'
          ) AS mci
        ON src.cpt_code = mci.src_concept_code