{{ config(materialized='table', schema = 'omop') }}
        
SELECT
*
FROM read_csv('../../_study_data/vocabulary/CONCEPT_ANCESTOR.csv', AUTO_DETECT=TRUE, HEADER=TRUE, null_padding=true)