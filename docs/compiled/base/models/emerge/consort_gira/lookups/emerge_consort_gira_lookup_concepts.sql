

-- Purpose: Filter down the concept table for joining to the source tables.
--          Also useful for analysis of missing concepts and non-standard concepts.

-- TODO Analysis -find where there is no join to the concept table.
-- TODO Analysis - find where concepts are not standard.
-- TODO Post analysis - remove unnecessary columns, or reference the CONCEPT table directly in the int models. 
        
WITH id_join_concepts as (
    SELECT DISTINCT measurement_concept_id AS concept_id,
    null as concept_code,
    'M' as src_table
    FROM "dbt"."main"."emerge_consort_gira_src_emerge_measurement_ex_release_20260127"
    WHERE measurement_concept_id IS NOT NULL

    UNION

    SELECT DISTINCT unit_concept_id AS concept_id,
    unit_concept_as_text as concept_code,
    'M' as src_table
    FROM "dbt"."main"."emerge_consort_gira_src_emerge_measurement_ex_release_20260127"
    WHERE unit_concept_id IS NOT NULL

    UNION

    SELECT DISTINCT gender_concept_id AS concept_id,
    null as concept_code,
    'P' as src_table
    FROM "dbt"."main"."emerge_consort_gira_src_emerge_person_ex_release_20260123"
    WHERE gender_concept_id IS NOT NULL

    UNION

    SELECT DISTINCT race_concept_id AS concept_id,
    null as concept_code,
    'P' as src_table
    FROM "dbt"."main"."emerge_consort_gira_src_emerge_person_ex_release_20260123"
    WHERE race_concept_id IS NOT NULL

    UNION

    SELECT DISTINCT ethnicity_concept_id AS concept_id,
    null as concept_code,
    'P' as src_table
    FROM "dbt"."main"."emerge_consort_gira_src_emerge_person_ex_release_20260123"
    WHERE ethnicity_concept_id IS NOT NULL

    UNION

    SELECT DISTINCT measurement_concept_id AS concept_id,
    measurement_concept_name as concept_code,
    'BMI' as src_table
    FROM "dbt"."main"."emerge_consort_gira_src_emerge_bmi_ex_release_20260128"
    WHERE measurement_concept_id IS NOT NULL

    UNION

    SELECT DISTINCT unit_concept_id AS concept_id,
    unit_concept_name as concept_code,
    'BMI' as src_table
    FROM "dbt"."main"."emerge_consort_gira_src_emerge_bmi_ex_release_20260128"
    WHERE unit_concept_id IS NOT NULL
),

code_icd_concepts as (
    SELECT DISTINCT null as concept_id,
    icd_code AS concept_code,
    'ICD' as src_table
    FROM "dbt"."main"."emerge_consort_gira_src_emerge_icd_ex_release_20260129"
    WHERE icd_code IS NOT NULL

),

code_cpt_concepts as (
    SELECT DISTINCT null as concept_id,
    cpt_code AS concept_code,
    'CPT' as src_table
    FROM "dbt"."main"."emerge_consort_gira_src_emerge_cpt_ex_release_20260129"
    WHERE cpt_code IS NOT NULL
),

code_bmi_units_concepts as (
    SELECT DISTINCT null as concept_id,
    unit_concept_name AS concept_code,
    'BMI' as src_table
    FROM "dbt"."main"."emerge_consort_gira_src_emerge_bmi_ex_release_20260128"
    WHERE unit_concept_id is null and unit_concept_name is not null
),

code_m_units_concepts as (
    SELECT DISTINCT null as concept_id,
    unit_concept_as_text AS concept_code,
    'M' as src_table
    FROM "dbt"."main"."emerge_consort_gira_src_emerge_measurement_ex_release_20260127"
    WHERE unit_concept_id is null and unit_concept_as_text is not null
),


unioned as (
SELECT * 
FROM id_join_concepts 
LEFT JOIN "dbt"."main_omop"."CONCEPT" AS concept
ON id_join_concepts.concept_id = concept.concept_id

UNION

SELECT *
FROM code_icd_concepts 
LEFT JOIN "dbt"."main_omop"."CONCEPT" AS concept
ON code_icd_concepts.concept_code = concept.concept_code 
AND vocabulary_id LIKE 'ICD%'
    
UNION

SELECT * 
FROM code_cpt_concepts 
LEFT JOIN "dbt"."main_omop"."CONCEPT" AS concept
ON code_cpt_concepts.concept_code = concept.concept_code 
AND vocabulary_id = 'CPT4'

UNION

SELECT *
FROM code_bmi_units_concepts 
LEFT JOIN "dbt"."main_omop"."CONCEPT" AS concept
ON code_bmi_units_concepts.concept_code = concept.concept_code 
AND domain_id = 'Unit'
    
UNION

SELECT * 
FROM code_m_units_concepts 
LEFT JOIN "dbt"."main_omop"."CONCEPT" AS concept
ON code_m_units_concepts.concept_code = concept.concept_code 
AND domain_id = 'Unit'
),

deduplicated AS ( -- Retain only one record per concept_code when the concept is found in multiple vocabularies.
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                -- Partition by both concept_code and concept_id to ensure unique results
                COALESCE(concept_code, CAST(concept_id AS STRING))
            ORDER BY src_table -- Secondary sorting logic to break ties
        ) AS row_number
    FROM unioned
)

SELECT *
FROM deduplicated
WHERE row_number = 1