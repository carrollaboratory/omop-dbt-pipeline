{{ config(materialized='table') }}
        
SELECT
*
FROM read_csv('../../_study_data/vocabulary/CONCEPT_RELATIONSHIP.csv', AUTO_DETECT=TRUE, HEADER=TRUE)