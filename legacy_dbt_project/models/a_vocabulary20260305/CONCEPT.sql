{{ config(materialized='table', schema = 'omop') }}
        
SELECT
*
FROM read_csv('../../_study_data/vocabulary/CONCEPT.csv', AUTO_DETECT=TRUE, HEADER=TRUE, null_padding=true) -- todo Remove null_padding. Read errors at concept 1454005 which only has 7 of the 10 expected columns.

union all 
select
*
from {{ ref('eMERGE_GWAS_OMOP_concept_draft') }}

union all 
select
*
from {{ ref('eMERGEseq_OMOP_concept_draft') }}

union all 
select
*
from {{ ref('eMERGE_PGRNseq_OMOP_concept_draft') }}