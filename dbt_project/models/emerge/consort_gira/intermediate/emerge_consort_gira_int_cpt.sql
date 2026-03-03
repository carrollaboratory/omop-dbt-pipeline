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
LEFT JOIN {{ ref('CONCEPT') }} AS concept_lookup
    ON cpt_src.cpt_code = concept_lookup.concept_id