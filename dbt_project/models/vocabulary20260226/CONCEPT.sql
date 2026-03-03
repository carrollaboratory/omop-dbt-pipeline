{{ config(materialized='table') }}
        
SELECT
*
FROM read_csv('../../_study_data/consort_gira/eMERGE_6_Month_Data_External_Release/seeds/CONCEPT.csv', AUTO_DETECT=TRUE, HEADER=TRUE)