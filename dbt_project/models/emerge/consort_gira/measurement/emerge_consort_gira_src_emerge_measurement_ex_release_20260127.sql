{{ config(materialized='table') }}
        
SELECT
    "EMERGE_ID"::TEXT AS "emerge_id",
    "AGE_AT_EVENT"::TEXT AS "age_at_event",
    "MEASUREMENT_CONCEPT_ID"::TEXT AS "measurement_concept_id",
    "VALUE_AS_NUMBER"::TEXT AS "value_as_number",
    "VALUE_AS_TEXT"::TEXT AS "value_as_text",
    "RANGE_LOW"::TEXT AS "range_low",
    "RANGE_HIGH"::TEXT AS "range_high",
    "RANGE_FLAG"::TEXT AS "range_flag",
    "UNIT_CONCEPT_ID"::TEXT AS "unit_concept_id",
    "UNIT_CONCEPT_AS_TEXT"::TEXT AS "unit_concept_as_text",
    "ROW_ID"::TEXT AS "row_id",
    "ENCOUNTER_ID"::TEXT AS "encounter_id",
    "GIRA_ROR"::TEXT AS "gira_ror",
FROM read_csv('../../_study_data/consort_gira/eMERGE_Measurement_Ex_Release_20260127.csv', AUTO_DETECT=FALSE, HEADER=TRUE, columns={
        'EMERGE_ID': 'VARCHAR',
        'AGE_AT_EVENT': 'VARCHAR',
        'MEASUREMENT_CONCEPT_ID': 'VARCHAR',
        'VALUE_AS_NUMBER': 'VARCHAR',
        'VALUE_AS_TEXT': 'VARCHAR',
        'RANGE_LOW': 'VARCHAR',
        'RANGE_HIGH': 'VARCHAR',
        'RANGE_FLAG': 'VARCHAR',
        'UNIT_CONCEPT_ID': 'VARCHAR',
        'UNIT_CONCEPT_AS_TEXT': 'VARCHAR',
        'ROW_ID': 'VARCHAR',
        'ENCOUNTER_ID': 'VARCHAR',
        'GIRA_ROR': 'VARCHAR'
    })