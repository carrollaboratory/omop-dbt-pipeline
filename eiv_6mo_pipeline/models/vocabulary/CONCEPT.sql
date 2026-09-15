{{ config(materialized='table') }}
        
SELECT
*
FROM read_csv('../../_study_data/vocabulary/20260318V2/CONCEPT.csv', AUTO_DETECT=TRUE, HEADER=TRUE)