{{ config(materialized='table') }}
        
SELECT

"emerge_id",
"age_at_event",
"cpt_code",
"row_id",
"encounter_id",
"gira_ror",
c.*
FROM {{ ref('emerge_consort_gira_src_emerge_cpt_ex_release_20260129') }} cpt_src
LEFT JOIN {{ ref('emerge_consort_gira_lookup_concept') }} AS c
    ON cpt_src.cpt_code = c.concept_code
    WHERE vocabulary_id = 'CPT4'