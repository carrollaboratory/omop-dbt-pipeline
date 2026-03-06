{{ config(materialized='table') }}
        
SELECT
*
FROM read_csv('../../_study_data/vocabulary/CONCEPT.csv', AUTO_DETECT=TRUE, HEADER=TRUE)