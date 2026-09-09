{{ config(materialized='table') }}
        
SELECT
    ROW_NUMBER() OVER () AS "src_index",
    "EMERGE_ID"::TEXT AS "emerge_id",
    "CONSENT"::TEXT AS "consent",
    "SUBJECT_SOURCE"::TEXT AS "subject_source",

FROM read_csv('../../_study_data/consort_gira/eMERGE_6_Month_Data_External_Release/external_src_files/eMERGE_6_Month_Consent_DS_20260731.csv', AUTO_DETECT=FALSE, HEADER=TRUE, nullstr = ["null", "NA"],columns={
        'EMERGE_ID': 'VARCHAR',
        'CONSENT': 'VARCHAR',
        'SUBJECT_SOURCE': 'VARCHAR',
    })