{{ config(materialized='table') }}

-- Purpose: Filter down the concept table for joining to the source tables.
--          Also useful for analysis of missing concepts and non-standard concepts.

-- TODO Analysis -find where there is no join to the concept table.
-- TODO Analysis - find where concepts are not standard.
-- TODO Post analysis - remove unnecessary columns, or reference the CONCEPT table directly in the int models. 
        
WITH id_join_concepts as (
    SELECT DISTINCT measurement_concept_id AS concept_id,
    null as concept_code
--     CONCAT('M_', row_id) AS src_row_id
    FROM {{ ref('emerge_consort_gira_src_emerge_measurement_ex_release_20260127') }}
    WHERE measurement_concept_id IS NOT NULL

    UNION

    SELECT DISTINCT unit_concept_id AS concept_id,
    unit_concept_as_text as concept_code
--     CONCAT('M_', row_id) AS src_row_id
    FROM {{ ref('emerge_consort_gira_src_emerge_measurement_ex_release_20260127') }}
    WHERE unit_concept_id IS NOT NULL

    UNION

    SELECT DISTINCT gender_concept_id AS concept_id,
    null as concept_code
--     CONCAT('P_', row_id) AS src_row_id
    FROM {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    WHERE gender_concept_id IS NOT NULL

    UNION

    SELECT DISTINCT race_concept_id AS concept_id,
    null as concept_code
--     CONCAT('P_', row_id) AS src_row_id
    FROM {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    WHERE race_concept_id IS NOT NULL

    UNION

    SELECT DISTINCT ethnicity_concept_id AS concept_id,
    null as concept_code
--     CONCAT('P_', row_id) AS src_row_id
    FROM {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
    WHERE ethnicity_concept_id IS NOT NULL

    UNION

    SELECT DISTINCT measurement_concept_id AS concept_id,
    measurement_concept_name as concept_code
--     CONCAT('BMI_', row_id) AS src_row_id
    FROM {{ ref('emerge_consort_gira_src_emerge_bmi_ex_release_20260128') }}
    WHERE measurement_concept_id IS NOT NULL

    UNION

    SELECT DISTINCT unit_concept_id AS concept_id,
    unit_concept_name as concept_code
--     CONCAT('BMI_', row_id) AS src_row_id
    FROM {{ ref('emerge_consort_gira_src_emerge_bmi_ex_release_20260128') }}
    WHERE unit_concept_id IS NOT NULL
),

code_icd_concepts as (
    SELECT DISTINCT null as concept_id,
    icd_code AS concept_code
--     CONCAT('ICD_', row_id) AS src_row_id
    FROM {{ ref('emerge_consort_gira_src_emerge_icd_ex_release_20260129') }}
    WHERE icd_code IS NOT NULL

),

code_cpt_concepts as (
    SELECT DISTINCT null as concept_id,
    cpt_code AS concept_code
--     CONCAT('CPT_', row_id) AS src_row_id
    FROM {{ ref('emerge_consort_gira_src_emerge_cpt_ex_release_20260129') }}
    WHERE cpt_code IS NOT NULL
)

SELECT * FROM id_join_concepts LEFT JOIN {{ ref('CONCEPT') }} AS concept
    ON id_join_concepts.concept_id = concept.concept_id

UNION

SELECT * FROM code_icd_concepts LEFT JOIN {{ ref('CONCEPT') }} AS concept
    ON code_icd_concepts.concept_code = concept.concept_code 
    AND vocabulary_id LIKE 'ICD%'
    
UNION

SELECT * FROM code_cpt_concepts LEFT JOIN {{ ref('CONCEPT') }} AS concept
    ON code_cpt_concepts.concept_code = concept.concept_code 
    AND vocabulary_id = 'CPT4'
    



