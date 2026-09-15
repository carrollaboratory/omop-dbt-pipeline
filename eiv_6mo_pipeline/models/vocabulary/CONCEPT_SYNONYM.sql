{{ config(materialized='table') }}
        
SELECT
*
FROM read_csv('../../_study_data/vocabulary/20260318V2/CONCEPT_SYNONYM.csv', AUTO_DETECT=TRUE, HEADER=TRUE)