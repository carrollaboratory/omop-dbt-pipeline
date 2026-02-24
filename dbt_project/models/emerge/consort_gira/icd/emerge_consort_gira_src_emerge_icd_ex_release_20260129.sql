{{ config(materialized='table') }}
        
SELECT

    "EMERGE_ID"::TEXT AS "emerge_id",
    "AGE_AT_EVENT"::TEXT AS "age_at_event",
    "ICD_CODE"::TEXT AS "icd_code",
    "ICD_FLAG"::TEXT AS "icd_flag",
    "ROW_ID"::TEXT AS "row_id",
    "ENCOUNTER_ID"::TEXT AS "encounter_id",
    "GIRA_ROR"::TEXT AS "gira_ror"
FROM read_csv('../../_study_data/consort_gira/eMERGE_ICD_Ex_Release_20260129.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={
        'EMERGE_ID': 'VARCHAR',
        'AGE_AT_EVENT': 'VARCHAR',
        'ICD_CODE': 'VARCHAR',
        'ICD_FLAG': 'VARCHAR',
        'ROW_ID': 'VARCHAR',
        'ENCOUNTER_ID': 'VARCHAR',
        'GIRA_ROR': 'VARCHAR'
    })