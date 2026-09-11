{{ config(materialized='table') }}
        
SELECT
*
FROM read_csv('../../_study_data/vocabulary/20260318V2/CONCEPT_ANCESTOR.csv', AUTO_DETECT=TRUE, HEADER=TRUE, null_padding=true)