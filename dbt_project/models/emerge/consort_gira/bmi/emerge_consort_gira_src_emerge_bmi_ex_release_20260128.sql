{{ config(materialized='table') }}
        
SELECT

    "EMERGE_ID"::TEXT AS "emerge_id",
    "AGE_AT_EVENT"::TEXT AS "age_at_event",
    "MEASUREMENT_CONCEPT_ID"::TEXT AS "measurement_concept_id",
    "MEASUREMENT_CONCEPT_NAME"::TEXT AS "measurement_concept_name",
    "VALUE_AS_NUMBER"::TEXT AS "value_as_number",
    "UNIT_CONCEPT_ID"::TEXT AS "unit_concept_id",
    "UNIT_CONCEPT_NAME"::TEXT AS "unit_concept_name",
    "BMI_Z_SCORE"::TEXT AS "bmi_z_score",
    "ROW_ID"::TEXT AS "row_id",
    "ENCOUNTER_ID"::TEXT AS "encounter_id",
    "GIRA_ROR"::TEXT AS "gira_ror"
FROM read_csv('../../_study_data/consort_gira/eMERGE_BMI_Ex_Release_20260128.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={
        'EMERGE_ID': 'VARCHAR',
        'AGE_AT_EVENT': 'VARCHAR',
        'MEASUREMENT_CONCEPT_ID': 'VARCHAR',
        'MEASUREMENT_CONCEPT_NAME': 'VARCHAR',
        'VALUE_AS_NUMBER': 'VARCHAR',
        'UNIT_CONCEPT_ID': 'VARCHAR',
        'UNIT_CONCEPT_NAME': 'VARCHAR',
        'BMI_Z_SCORE': 'VARCHAR',
        'ROW_ID': 'VARCHAR',
        'ENCOUNTER_ID': 'VARCHAR',
        'GIRA_ROR': 'VARCHAR'
    })