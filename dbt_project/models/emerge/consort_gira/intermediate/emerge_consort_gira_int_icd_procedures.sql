{{ config(materialized='table') }}
        
SELECT
    emerge_id,
    age_at_event,
    icd_code,
    mci.src_concept_id as icd_id,
    mci.s_concept_id as "s_procedure_concept_id",
    mci.s_concept_code as "s_procedure_concept_code",
    icd_flag,
    row_id,
    encounter_id,
    gira_ror,
    src_index,
FROM {{ ref('emerge_consort_gira_src_emerge_icd_ex_release_20260129') }} src
    JOIN (SELECT -- JOIN used to drop rows that are not domain 'Procedure'
          s_concept_id, s_concept_code, src_concept_id, src_concept_code, domain_id
          FROM {{ ref('emerge_consort_gira_lookup_standards') }} 
          WHERE src_table = 'ICD'
          AND domain_id = 'Procedure'
          ) AS mci
        ON src.icd_code = mci.src_concept_code
    where emerge_id not in (select emerge_id from {{ ref('emerge_consort_gira_lookup_exclusion') }})
    and domain_id is not null