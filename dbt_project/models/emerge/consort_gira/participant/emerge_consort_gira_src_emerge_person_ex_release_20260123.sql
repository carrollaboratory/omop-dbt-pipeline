{{ config(materialized='table') }}
        
SELECT

    "EMERGE_ID"::TEXT AS "emerge_id",
    "WITHDRAWAL_STATUS"::TEXT AS "withdrawal_status",
    "YEAR_OF_BIRTH"::TEXT AS "year_of_birth",
    "GENDER_CONCEPT_ID"::TEXT AS "gender_concept_id",
    "RACE_CONCEPT_ID"::TEXT AS "race_concept_id",
    "ETHNICITY_CONCEPT_ID"::TEXT AS "ethnicity_concept_id"
FROM read_csv('../../_study_data/consort_gira/eMERGE_Person_Ex_Release_20260123.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={
        'EMERGE_ID': 'VARCHAR',
        'WITHDRAWAL_STATUS': 'VARCHAR',
        'YEAR_OF_BIRTH': 'VARCHAR',
        'GENDER_CONCEPT_ID': 'VARCHAR',
        'RACE_CONCEPT_ID': 'VARCHAR',
        'ETHNICITY_CONCEPT_ID': 'VARCHAR'
    })