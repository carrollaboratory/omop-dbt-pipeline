{{ config(materialized='table', schema = 'omop') }}
        
SELECT
*
FROM read_csv('../../_study_data/vocabulary/VOCABULARY.csv', AUTO_DETECT=TRUE, HEADER=TRUE)