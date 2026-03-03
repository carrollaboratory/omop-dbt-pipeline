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
LEFT JOIN {{ ref('CONCEPT') }} AS concept_lookup
    ON icd_src.icd_code = concept_lookup.concept_id