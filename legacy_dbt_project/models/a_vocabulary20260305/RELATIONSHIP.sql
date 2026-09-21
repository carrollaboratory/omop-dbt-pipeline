{{ config(materialized='table', schema = 'omop') }}
        
SELECT
*
FROM read_csv('../../_study_data/vocabulary/RELATIONSHIP.csv', AUTO_DETECT=TRUE, HEADER=TRUE)