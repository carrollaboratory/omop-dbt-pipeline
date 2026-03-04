{{ config(materialized='table') }}
        
SELECT

"emerge_id",
"age_at_event",
"icd_code",
"icd_flag",
"row_id",
"encounter_id",
"gira_ror",
c.*
FROM {{ ref('emerge_consort_gira_src_emerge_icd_ex_release_20260129') }} icd_src
LEFT JOIN {{ ref('emerge_consort_gira_lookup_concept') }} AS c
    ON icd_src.icd_code = c.concept_code
    WHERE vocabulary_id LIKE 'ICD%'